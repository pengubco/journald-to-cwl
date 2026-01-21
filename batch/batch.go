package batch

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/coreos/go-systemd/v22/sdjournal"
	"go.uber.org/zap"
)

const (
	// The maximum CWL batch sizeis 1,048,576 bytes. "This size is calculated as the sum of all event messages in UTF-8,
	// plus 26 bytes for each log event. This quota can't be changed."
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
	maxCWLBatchSize = 1024 * 1024 * 9 / 10

	// For a CWL LogEvent whose messge is biggeer than maxCWLBatchSize, keep a few leading bytes.
	// bytesToKeepForLogEvent must be less than maxCWLBatchSize
	bytesToKeepForLogEvent = 500

	maxBatchEvents = 1000

	maxBatchWait = 2 * time.Second
)

// isAuditLog checks if a journal entry is an audit log.
func isAuditLog(e *sdjournal.JournalEntry) bool {
	return e.Fields[sdjournal.SD_JOURNAL_FIELD_TRANSPORT] == "audit"
}

// Batch is a unit collection of CWL log events and the journal entry cursor of the last log event.
type Batch struct {
	Events []types.InputLogEvent

	// Cursor is the cursor of the last journal entry. "In journald, a cursor is an opaque text string that uniquely
	// describes the position of an entry in the journal and is portable across machines, platforms and journal files."
	Cursor string
}

// Batcher transforms journal entries into log events and batches log events into, you guessed it, batches.
type Batcher struct {
	entries   <-chan *sdjournal.JournalEntry
	converter EntryToEventConverter

	batches chan *Batch

	// Maximum total bytes of all messages in one batch.
	maxPayload int

	// Maximum number of log events in one batch.
	maxEvents int

	// Maximum time to wait for a batch.
	MaxWait time.Duration

	// skipAuditLog determines whether to skip audit logs.
	skipAuditLog bool
}

func NewBatcher(
	entries <-chan *sdjournal.JournalEntry,
	converter EntryToEventConverter,
	opts ...Option,
) *Batcher {
	b := Batcher{
		entries:      entries,
		converter:    converter,
		batches:      make(chan *Batch),
		maxPayload:   maxCWLBatchSize,
		maxEvents:    maxBatchEvents,
		MaxWait:      maxBatchWait,
		skipAuditLog: false,
	}
	for _, opt := range opts {
		opt(&b)
	}
	return &b
}

// Batches returns a channel of Batch. The returned channel is never closed.
func (b *Batcher) Batches() <-chan *Batch {
	return b.batches
}

// Batch batches entries until the ctx is canceled. Batch should be called only once per batcher.
func (b *Batcher) Batch(ctx context.Context) {
	bytesCount := 0
	ticker := time.NewTicker(b.MaxWait)
	defer ticker.Stop()
	var batch *Batch

	saveOldBatch := func() {
		if len(batch.Events) == 0 {
			return
		}
		b.batches <- batch
	}

	startNewBatch := func() {
		batch = &Batch{
			Events: make([]types.InputLogEvent, 0, b.maxEvents),
		}
		bytesCount = 0
		ticker.Reset(b.MaxWait)
	}

	startNewBatch()

	for {
		select {
		case <-ctx.Done():
			saveOldBatch()
			return
		case <-ticker.C:
			saveOldBatch()
			startNewBatch()
		case entry := <-b.entries:
			// Skip audit logs if configured to do so.
			if b.skipAuditLog && isAuditLog(entry) {
				continue
			}
			event := b.converter(entry)
			if event.Message == nil {
				// this should never happen.
				zap.S().Error("input log event message should never be nil")
				continue
			}
			msgSize := len(*event.Message)
			// Rare case. For single entry that's too big, keep only the first 500 bytes.
			if msgSize > maxCWLBatchSize {
				event.Message = aws.String((*event.Message)[:bytesToKeepForLogEvent])
				msgSize = len(*event.Message)
			}
			if msgSize+bytesCount > b.maxPayload || len(batch.Events) == b.maxEvents {
				saveOldBatch()
				startNewBatch()
			}
			batch.Events = append(batch.Events, event)
			batch.Cursor = entry.Cursor
			bytesCount += msgSize
		}
	}
}

type Option func(*Batcher)

func WithMaxPayload(maxPayload int) Option {
	return func(b *Batcher) {
		b.maxPayload = maxPayload
	}
}

func WithMaxEvents(maxEvents int) Option {
	return func(b *Batcher) {
		b.maxEvents = maxEvents
	}
}

func WithMaxWait(maxWait time.Duration) Option {
	return func(b *Batcher) {
		b.MaxWait = maxWait
	}
}

func WithSkipAuditLog(skip bool) Option {
	return func(b *Batcher) {
		b.skipAuditLog = skip
	}
}
