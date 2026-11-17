package cwl

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
	"github.com/cenkalti/backoff/v7"
	"go.uber.org/zap"

	"github.com/pengubco/journald-to-cwl/batch"
)

const (
	// Base pause after a throttled call. Picked 10 seconds for no reason: it is half of the AWS SDK's default
	// max backoff. Up to half of it again is added at random, so many hosts throttled at once spread out.
	throttlePause = 10 * time.Second

	// Upper bound for one CloudWatch Logs call, including the SDK's own retries. Without it a request that
	// never returns would stall the writer forever, and the give-up rule below would never be evaluated.
	callTimeout = time.Minute

	// How long one batch may keep failing for reasons other than throttling before Write returns and
	// systemd restarts the process. Long enough to outlast a network outage of several minutes; short
	// enough that a misconfiguration, such as missing permissions or a wrong endpoint, still ends in a
	// visible restart. There is no measurement behind the exact value.
	failureBudget = 15 * time.Minute

	// Pause between attempts after a failure: 1 s, doubling to at most 30 s, each randomized by ±20%.
	// The cap also limits how often the "retrying" line lands in the journal, which this program reads.
	failurePauseMin    = time.Second
	failurePauseMax    = 30 * time.Second
	failurePauseJitter = 0.2
	failurePauseGrowth = 2.0
)

// "5000 transactions per second per account per Region You can request an increase to the per-second throttling quota
// by using the Service Quotas service." 5000 RPS sounds a lot, but is not when there are hundreds of EC2 instances where
// each instance runs a CWL writer.
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
var errThrottled = errors.New("too many requests")

type CloudwatchLogsAPI interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput,
		optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)

	CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput,
		optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
}

type SaveCursor func(cursor string) error

// Writer consume batches of log events from a channel and write them to CWL.
type Writer struct {
	batches    <-chan *batch.Batch
	cwlClient  CloudwatchLogsAPI
	logGroup   string
	logStream  string
	saveCursor SaveCursor

	// The fields below hold the constants above; tests shrink them.
	throttlePause time.Duration
	callTimeout   time.Duration
	failureBudget time.Duration
	failurePause  backoff.BackOff
}

func NewWriter(
	batches <-chan *batch.Batch,
	cwlClient CloudwatchLogsAPI,
	logGroup string,
	logStream string,
	saveCursor SaveCursor,
) *Writer {
	return &Writer{
		batches:       batches,
		cwlClient:     cwlClient,
		logGroup:      logGroup,
		logStream:     logStream,
		saveCursor:    saveCursor,
		throttlePause: throttlePause,
		callTimeout:   callTimeout,
		failureBudget: failureBudget,
		failurePause: &backoff.ExponentialBackOff{
			InitialInterval:     failurePauseMin,
			MaxInterval:         failurePauseMax,
			RandomizationFactor: failurePauseJitter,
			Multiplier:          failurePauseGrowth,
		},
	}
}

// outcome says what Write does after one attempt to send a batch.
type outcome int

const (
	sent      outcome = iota
	throttled         // wait and try again, however long it takes
	transient         // wait and try again, while the failure budget lasts
	permanent         // give up: the request itself is wrong
)

// classify maps the error of one attempt to an outcome. An error without an API error inside means no
// response arrived (connection refused, DNS, timeout), which a later attempt can survive. An API error
// counts as permanent only when the service blames the request; server faults are transient.
func classify(err error) outcome {
	if err == nil {
		return sent
	}
	if errors.Is(err, errThrottled) {
		return throttled
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorFault() == smithy.FaultClient {
		return permanent
	}
	return transient
}

// Write sends batches to CWL until ctx ends or a batch cannot be delivered. It returns without saving the
// cursor of the failed batch, so after systemd restarts the program it reads the same entries again.
//
// Throttling is retried without limit. Other failures that a retry can fix are retried until they have
// used failureBudget, not counting time spent waiting out throttling. A rejected request is not retried.
//
// Retrying here instead of exiting early loses nothing extra: the reader and batcher simply block on their
// channels while a batch is retried. Entries that journald rotates away during a long outage are gone
// whether the outage is spent in this loop or in a series of restarts.
func (w *Writer) Write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-w.batches:
			if !w.deliver(ctx, b) {
				return
			}
		}
	}
}

// deliver sends one batch and saves its cursor, retrying as described on Write. It reports whether Write
// should go on with the next batch.
func (w *Writer) deliver(ctx context.Context, b *batch.Batch) bool {
	w.failurePause.Reset()
	start := time.Now()
	var throttledFor time.Duration
	for {
		err := w.send(ctx, b.Events)
		var pause time.Duration
		switch classify(err) {
		case sent:
			if err := w.saveCursor(b.Cursor); err != nil {
				zap.S().Errorf("cannot save cursor, %v", err)
				return false
			}
			return true
		case throttled:
			pause = w.throttlePause + rand.N(w.throttlePause/2) //nolint:gosec // Spreads retries; need not be unpredictable.
			throttledFor += pause
			// Logged at most once per throttlePause, so this program does not flood the journal it ships.
			zap.S().Infof("PutLogEvents call throttled. retry in %v", pause)
		case transient:
			if time.Since(start)-throttledFor > w.failureBudget {
				zap.S().Errorf("giving up on batch after %v of failures: %v", w.failureBudget, err)
				return false
			}
			pause = w.failurePause.NextBackOff()
			zap.S().Errorf("PutLogEvents failed, retry in %v: %v", pause, err)
		case permanent:
			zap.S().Error(err)
			return false
		}
		if !waitFor(ctx, pause) {
			return false
		}
	}
}

// waitFor sleeps for d and reports false if ctx ended first.
func waitFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// send makes one attempt to put events, creating the log stream first if it does not exist yet.
func (w *Writer) send(ctx context.Context, events []types.InputLogEvent) error {
	err := w.putEvents(ctx, events)
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return err
	}
	if err := w.createStream(ctx); err != nil {
		return err
	}
	return w.putEvents(ctx, events)
}

func (w *Writer) putEvents(ctx context.Context, events []types.InputLogEvent) error {
	ctx, cancel := context.WithTimeout(ctx, w.callTimeout)
	defer cancel()
	_, err := w.cwlClient.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(w.logGroup),
		LogStreamName: aws.String(w.logStream),
	})
	return asThrottled(err)
}

func (w *Writer) createStream(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.callTimeout)
	defer cancel()
	_, err := w.cwlClient.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(w.logGroup),
		LogStreamName: aws.String(w.logStream),
	})
	// The stream can appear between our PutLogEvents and this call, for example when the previous run of
	// this program created it just before it was restarted.
	var exists *types.ResourceAlreadyExistsException
	if errors.As(err, &exists) {
		return nil
	}
	return asThrottled(err)
}

// asThrottled turns a ThrottlingException into errThrottled and leaves other errors as they are.
func asThrottled(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == ((*types.ThrottlingException)(nil)).ErrorCode() {
		return errThrottled
	}
	return err
}
