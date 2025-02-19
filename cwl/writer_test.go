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
			s := &cwlStub{}
			w := NewWriter(batches, s, "instance-logs", "i-11111111111111111/journal-logs",
				func(cursor string) error {
					cursors = append(cursors, cursor)
					return nil
				})

			w.Write(ctx)

			assert.Equal(t, tc.numBatches*100, s.eventsCnt)
			assert.Equal(t, expectedCursors, cursors)
		})
	}
}

func TestReturnOnNonRetriableError(t *testing.T) {
	cases := []struct {
		name                 string
		errOnPutLogEvents    error
		errOnCreateLogStream error
		saveCursor           SaveCursor
	}{
		{
			name:                 "put log events faield, non retriable",
			errOnPutLogEvents:    errors.New("cannot put log events"),
			errOnCreateLogStream: nil,
			saveCursor:           func(string) error { return nil },
		},
		{
			name: "create log stream failed",
			errOnPutLogEvents: &types.ResourceNotFoundException{
				Message: aws.String("stream does not exist"),
			},
			errOnCreateLogStream: errors.New("cannot create log stream"),
			saveCursor:           func(string) error { return nil },
		},
		{
			name:       "save cursor error",
			saveCursor: func(cursor string) error { return errors.New("cannot save cursor") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches := make(chan *batch.Batch, 1)
			batches <- &batch.Batch{
				Events: []types.InputLogEvent{{}},
				Cursor: "cursor-0",
			}
			w := NewWriter(batches, &cwlStub{
				errOnPutLogEvents:    tc.errOnPutLogEvents,
				errOnCreateLogStream: tc.errOnCreateLogStream,
			}, "instance-logs", "i-11111111111111111/journal-logs", tc.saveCursor)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			w.Write(ctx)
			// The Write returns but the context was not cancelled. This means, the Write returns on non-retriable errors.
			assert.NoError(t, ctx.Err())
		})
	}
}

func TestRetryOnThrottle(t *testing.T) {
	cases := []struct {
		name              string
		errOnPutLogEvents error
	}{
		{
			name: "put log events throttled",
			errOnPutLogEvents: &smithy.GenericAPIError{
				Code: ((*types.ThrottlingException)(nil)).ErrorCode(),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches := make(chan *batch.Batch, 1)
			batches <- &batch.Batch{
				Events: []types.InputLogEvent{{}},
				Cursor: "cursor-0",
			}
			w := NewWriter(batches, &cwlStub{
				errOnPutLogEvents: tc.errOnPutLogEvents,
			}, "instance-logs", "i-11111111111111111/journal-logs", func(string) error { return nil })
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			w.Write(ctx)
			// The Write returns and the context has been cancelled. This indicates that the Write retried on throttles.
			assert.Error(t, ctx.Err())
		})
	}
}

// cwlStub counts number of events it received.
type cwlStub struct {
	eventsCnt            int
	errOnPutLogEvents    error
	errOnCreateLogStream error
}

func (s *cwlStub) PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput,
	optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	if s.errOnPutLogEvents != nil {
		return nil, s.errOnPutLogEvents
	}
	s.eventsCnt += len(params.LogEvents)
	return nil, nil //nolint:nilnil
}

func (s *cwlStub) CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput,
	optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	return nil, s.errOnCreateLogStream
}
