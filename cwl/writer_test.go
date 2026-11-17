package cwl

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
	"github.com/cenkalti/backoff/v7"
	"github.com/stretchr/testify/assert"

	"github.com/pengubco/journald-to-cwl/batch"
)

func TestWriteBatches(t *testing.T) {
	cases := []struct {
		name       string
		numBatches int
	}{
		{
			name:       "no batch",
			numBatches: 0,
		},
		{
			name:       "one batch",
			numBatches: 1,
		},
		{
			name:       "multiple batches",
			numBatches: 10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			batches := make(chan *batch.Batch)
			go func() {
				defer cancel()
				for i := 0; i < tc.numBatches; i++ {
					batches <- &batch.Batch{
						Events: make([]types.InputLogEvent, 100),
						Cursor: fmt.Sprintf("cursor-%d", (i+1)*100-1),
					}
				}
			}()
			var expectedCursors []string
			for i := 0; i < tc.numBatches; i++ {
				expectedCursors = append(expectedCursors, fmt.Sprintf("cursor-%d", (i+1)*100-1))
			}

			var cursors []string
			s := &fakeLogs{}
			w := NewWriter(batches, s, "instance-logs", "i-11111111111111111/journal-logs",
				func(cursor string) error {
					cursors = append(cursors, cursor)
					return nil
				})

			w.Write(ctx)

			assert.Equal(t, tc.numBatches*100, s.delivered)
			assert.Equal(t, expectedCursors, cursors)
		})
	}
}

var (
	errNoRoute   = errors.New("dial tcp 203.0.113.10:443: connect: no route to host")
	errRejected  = &smithy.GenericAPIError{Code: "InvalidParameterException", Fault: smithy.FaultClient}
	errDenied    = &smithy.GenericAPIError{Code: "AccessDeniedException", Fault: smithy.FaultClient}
	errOverload  = &smithy.GenericAPIError{Code: "ServiceUnavailableException", Fault: smithy.FaultServer}
	errSlowDown  = &smithy.GenericAPIError{Code: "ThrottlingException", Fault: smithy.FaultClient}
	errNoStream  = &types.ResourceNotFoundException{Message: aws.String("The specified log stream does not exist.")}
	errHasStream = &types.ResourceAlreadyExistsException{}
)

// fakeLogs answers the n-th PutLogEvents call with putErrs[n] and the n-th CreateLogStream call with
// createErrs[n]. Calls past the end of a list succeed, unless the matching always* error is set.
type fakeLogs struct {
	putErrs, createErrs     []error
	alwaysPut, alwaysCreate error

	puts, creates int
	delivered     int
	// timeouts records how long each call had before its context expired.
	timeouts []time.Duration
}

func (f *fakeLogs) noteTimeout(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok {
		f.timeouts = append(f.timeouts, time.Until(deadline))
	}
}

func (f *fakeLogs) PutLogEvents(ctx context.Context, in *cloudwatchlogs.PutLogEventsInput,
	_ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	f.noteTimeout(ctx)
	n := f.puts
	f.puts++
	if f.alwaysPut != nil {
		return nil, f.alwaysPut
	}
	if n < len(f.putErrs) && f.putErrs[n] != nil {
		return nil, f.putErrs[n]
	}
	f.delivered += len(in.LogEvents)
	return &cloudwatchlogs.PutLogEventsOutput{}, nil
}

func (f *fakeLogs) CreateLogStream(ctx context.Context, _ *cloudwatchlogs.CreateLogStreamInput,
	_ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	f.noteTimeout(ctx)
	n := f.creates
	f.creates++
	if f.alwaysCreate != nil {
		return nil, f.alwaysCreate
	}
	if n < len(f.createErrs) && f.createErrs[n] != nil {
		return nil, f.createErrs[n]
	}
	return &cloudwatchlogs.CreateLogStreamOutput{}, nil
}

// newTestWriter returns a writer over the given batches with every wait shortened to milliseconds. When
// stopAfter is positive, the context is cancelled once that many cursors are saved, so Write returns
// instead of waiting for more batches. The returned slice pointer collects the saved cursors.
func newTestWriter(f *fakeLogs, stopAfter int, cursors ...string) (*Writer, context.Context, *[]string) {
	ch := make(chan *batch.Batch, len(cursors))
	for _, c := range cursors {
		ch <- &batch.Batch{Events: []types.InputLogEvent{{}}, Cursor: c}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	saved := &[]string{}
	w := NewWriter(ch, f, "group", "stream", func(c string) error {
		*saved = append(*saved, c)
		if len(*saved) == stopAfter {
			cancel()
		}
		return nil
	})
	w.throttlePause = 2 * time.Millisecond
	w.failureBudget = 200 * time.Millisecond
	w.failurePause = &backoff.ExponentialBackOff{
		InitialInterval: time.Millisecond, MaxInterval: 4 * time.Millisecond, Multiplier: 2,
	}
	return w, ctx, saved
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err  error
		want outcome
	}{
		{nil, sent},
		{errThrottled, throttled},
		{errNoRoute, transient},
		{context.DeadlineExceeded, transient},
		{errOverload, transient},
		{&smithy.GenericAPIError{Code: "Unknown"}, transient},
		{errRejected, permanent},
		{fmt.Errorf("operation PutLogEvents: %w", errDenied), permanent},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.err), func(t *testing.T) {
			assert.Equal(t, tc.want, classify(tc.err))
		})
	}
}

func TestWriteRetriesUntilBatchIsSent(t *testing.T) {
	for _, err := range []error{errSlowDown, errNoRoute, errOverload} {
		t.Run(err.Error(), func(t *testing.T) {
			f := &fakeLogs{putErrs: []error{err, err, err}}
			w, ctx, saved := newTestWriter(f, 1, "c1")

			w.Write(ctx)

			assert.Equal(t, 4, f.puts)
			assert.Equal(t, 1, f.delivered)
			assert.Equal(t, []string{"c1"}, *saved)
		})
	}
}

func TestWriteStopsOnRejectedRequest(t *testing.T) {
	cases := []struct {
		name       string
		f          *fakeLogs
		saveCursor SaveCursor
	}{
		{"PutLogEvents rejected", &fakeLogs{alwaysPut: errRejected}, nil},
		{"CreateLogStream denied", &fakeLogs{putErrs: []error{errNoStream}, alwaysCreate: errDenied}, nil},
		{"cursor not saved", &fakeLogs{}, func(string) error { return errors.New("disk full") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, ctx, _ := newTestWriter(tc.f, 0, "c1")
			if tc.saveCursor != nil {
				w.saveCursor = tc.saveCursor
			}

			start := time.Now()
			w.Write(ctx)

			// Write returned by itself after one attempt, well before the failure budget ran out.
			assert.NoError(t, ctx.Err())
			assert.Equal(t, 1, tc.f.puts)
			assert.Less(t, time.Since(start), w.failureBudget)
		})
	}
}

func TestWriteGivesUpWhenFailureBudgetIsSpent(t *testing.T) {
	f := &fakeLogs{alwaysPut: errNoRoute}
	w, ctx, saved := newTestWriter(f, 0, "c1")

	start := time.Now()
	w.Write(ctx)

	assert.NoError(t, ctx.Err())
	assert.GreaterOrEqual(t, time.Since(start), w.failureBudget)
	assert.Greater(t, f.puts, 1)
	assert.Empty(t, *saved)
}

func TestThrottlingDoesNotCountAgainstFailureBudget(t *testing.T) {
	// Three throttle pauses of at least 50 ms each, then a network error. The budget is 30 ms, so the
	// network error is only retried if the pauses were left out of the budget.
	f := &fakeLogs{putErrs: []error{errSlowDown, errSlowDown, errSlowDown, errNoRoute}}
	w, ctx, saved := newTestWriter(f, 1, "c1")
	w.throttlePause = 50 * time.Millisecond
	w.failureBudget = 30 * time.Millisecond

	w.Write(ctx)

	assert.Equal(t, 5, f.puts)
	assert.Equal(t, []string{"c1"}, *saved)
}

func TestThrottlePauseEndsOnCancel(t *testing.T) {
	f := &fakeLogs{alwaysPut: errSlowDown}
	w, _, _ := newTestWriter(f, 0, "c1")
	w.throttlePause = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	w.Write(ctx)

	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, 1, f.puts)
}

func TestFailurePauseStartsOverForEachBatch(t *testing.T) {
	// Each batch fails once. Starting over, the pause before each retry is 10 ms; carried over from the
	// first batch, the second pause would be 1 s.
	f := &fakeLogs{putErrs: []error{errNoRoute, nil, errNoRoute}}
	w, ctx, saved := newTestWriter(f, 2, "c1", "c2")
	w.failurePause = &backoff.ExponentialBackOff{
		InitialInterval: 10 * time.Millisecond, MaxInterval: time.Second, Multiplier: 100,
	}

	start := time.Now()
	w.Write(ctx)

	assert.Equal(t, []string{"c1", "c2"}, *saved)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestWriteCreatesMissingStream(t *testing.T) {
	for _, createErr := range []error{nil, errHasStream} {
		t.Run(fmt.Sprint(createErr), func(t *testing.T) {
			f := &fakeLogs{putErrs: []error{errNoStream}, createErrs: []error{createErr}}
			w, ctx, saved := newTestWriter(f, 1, "c1")

			w.Write(ctx)

			assert.Equal(t, 1, f.creates)
			assert.Equal(t, 2, f.puts)
			assert.Equal(t, 1, f.delivered)
			assert.Equal(t, []string{"c1"}, *saved)
		})
	}
}

func TestThrottledCreateLogStreamIsRetried(t *testing.T) {
	f := &fakeLogs{putErrs: []error{errNoStream, errNoStream}, createErrs: []error{errSlowDown}}
	w, ctx, saved := newTestWriter(f, 1, "c1")

	w.Write(ctx)

	assert.Equal(t, 2, f.creates)
	assert.Equal(t, []string{"c1"}, *saved)
}

func TestEveryCallHasItsOwnTimeout(t *testing.T) {
	f := &fakeLogs{putErrs: []error{errNoStream}}
	w, ctx, _ := newTestWriter(f, 1, "c1")
	// A parent without a deadline, so any deadline the fake sees comes from callTimeout.
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-ctx.Done()
		cancel()
	}()

	w.Write(parent)

	// PutLogEvents, CreateLogStream, then PutLogEvents again.
	assert.Len(t, f.timeouts, 3)
	for _, left := range f.timeouts {
		assert.InDelta(t, w.callTimeout.Seconds(), left.Seconds(), 5)
	}
}
