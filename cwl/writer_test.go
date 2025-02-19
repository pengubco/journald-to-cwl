package cwl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
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
			w := NewWriter(batches, s, "journal-logs", "i-11111111111111111",
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

func TestPanicOnError(t *testing.T) {
	cases := []struct {
		name                 string
		errOnPutLogEvents    error
		errOnCreateLogStream error
		saveCursor           SaveCursor
	}{
		{
			name:                 "put log events faield",
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
			saveCursor: func(string) error { return errors.New("cannot save cursor") },
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
			}, "journal-logs", "i-11111111111111111", tc.saveCursor)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)

			go assert.Panics(t, func() {
				defer wg.Done()
				w.Write(ctx)
			})
			wg.Wait()
		})
	}
}

// cwlStub counts number of events it received.
type cwlStub struct {
	eventsCnt            int
	errOnPutLogEvents    error
	errOnCreateLogStream error
}

func (s *cwlStub) PutLogEvents(_ context.Context, params *cloudwatchlogs.PutLogEventsInput,
	_ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	if s.errOnPutLogEvents != nil {
		return nil, s.errOnPutLogEvents
	}
	s.eventsCnt += len(params.LogEvents)
	return nil, nil //nolint:nilnil
}

func (s *cwlStub) CreateLogStream(context.Context, *cloudwatchlogs.CreateLogStreamInput,
	...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	return nil, s.errOnCreateLogStream
}
