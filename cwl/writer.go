package cwl

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"

	"github.com/pengubco/journald-to-cwl/batch"
)

// Picked 10 seconds for no reason. 10 seconds is half of the default max backoff in AWS SDK.
const timeToWaitOnThrottle time.Duration = 10 * time.Second

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
}

func NewWriter(
	batches <-chan *batch.Batch,
	cwlClient CloudwatchLogsAPI,
	logGroup string,
	logStream string,
	saveCursor SaveCursor,
) *Writer {
	return &Writer{
		batches:    batches,
		cwlClient:  cwlClient,
		logGroup:   logGroup,
		logStream:  logStream,
		saveCursor: saveCursor,
	}
}

// Write log events to CWL. It panics if it cannot send events to CWL.
func (w *Writer) Write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-w.batches:
			err := w.writeBatch(ctx, batch.Events)
			if err == nil {
				if err := w.saveCursor(batch.Cursor); err != nil {
					zap.S().Panicf("cannot save cursor, %w", err)
				}
				continue
			}
			if err == errThrottled {
				time.Sleep(timeToWaitOnThrottle)
			}
			if err := w.writeBatch(ctx, batch.Events); err != nil {
				zap.S().Panicf("cannot write events to CWL", err)
				if err := w.saveCursor(batch.Cursor); err != nil {
					zap.S().Panicf("cannot save cursor, %w", err)
				}
			}
		}
	}
}

func (w *Writer) writeBatch(ctx context.Context, events []types.InputLogEvent) error {
	putEvents := func(events []types.InputLogEvent) error {
		request := &cloudwatchlogs.PutLogEventsInput{
			LogEvents:     events,
			LogGroupName:  aws.String(w.logGroup),
			LogStreamName: aws.String(w.logStream),
		}
		_, err := w.cwlClient.PutLogEvents(ctx, request)
		return err
	}

	createStream := func() error {
		request := &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(w.logGroup),
			LogStreamName: aws.String(w.logStream),
		}
		_, err := w.cwlClient.CreateLogStream(ctx, request)
		return err
	}

	err := putEvents(events)
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	switch apiErr.ErrorCode() {
	case ((*types.ThrottlingException)(nil)).ErrorCode():
		return errThrottled
	case ((*types.ResourceNotFoundException)(nil)).ErrorCode():
		if err := createStream(); err != nil {
			return err
		}
		return putEvents(events)
	}

	return err
}
