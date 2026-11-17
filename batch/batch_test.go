package batch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/coreos/go-systemd/v22/sdjournal"
	"github.com/stretchr/testify/assert"
)

var dummyInstanceID = "i-11111111111111111"

func TestEntryToEventConverter(t *testing.T) {
	timeUnixMilli := time.UnixMilli(int64(1722650790111))
	now := func() time.Time {
		return timeUnixMilli
	}
	cursor := "cursor-0"
	converter := NewEntryToEventConverter(dummyInstanceID, now)
	entry, expectedEvent := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, cursor)
	event := converter(entry)
	assert.JSONEq(t, *expectedEvent.Message, *event.Message)
	assert.Equal(t, *(expectedEvent.Timestamp), *(event.Timestamp))
}

// TestBatchOnMaxEvents tests batching entries into batch every maxEvents.
func TestBatchOnMaxEvents(t *testing.T) {
	timeUnixMilli := time.UnixMilli(int64(1722650790111))
	now := func() time.Time {
		return timeUnixMilli
	}
	converter := NewEntryToEventConverter(dummyInstanceID, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entriesChan := make(chan *sdjournal.JournalEntry)
	go func() {
		for i := 0; i < 10; i++ {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
	}()

	// Set MaxWait long enough to not interfer with MaxEvents.
	batcher := NewBatcher(entriesChan, converter, WithMaxEvents(2), WithMaxWait(time.Minute))
	go batcher.Batch(ctx)

	// Verify we get two batches.
	batch := <-batcher.Batches()
	assert.Equal(t, "cursor-1", batch.Cursor)
	batch = <-batcher.Batches()
	assert.Equal(t, "cursor-3", batch.Cursor)
}

func TestPrecedesLast(t *testing.T) {
	at := func(ms int64) types.InputLogEvent { return types.InputLogEvent{Timestamp: aws.Int64(ms)} }
	cases := []struct {
		name     string
		batch    []types.InputLogEvent
		next     types.InputLogEvent
		expected bool
	}{
		{name: "empty batch", batch: nil, next: at(5), expected: false},
		{name: "later timestamp", batch: []types.InputLogEvent{at(5)}, next: at(6), expected: false},
		{name: "same timestamp", batch: []types.InputLogEvent{at(5)}, next: at(5), expected: false},
		{name: "earlier timestamp", batch: []types.InputLogEvent{at(5)}, next: at(4), expected: true},
		{name: "compares with newest only", batch: []types.InputLogEvent{at(9), at(5)}, next: at(7), expected: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, precedesLast(&Batch{Events: tc.batch}, tc.next))
		})
	}
}

// TestBatchStartsNewBatchWhenClockGoesBack feeds four entries whose read-time clock jumps back by one
// minute after the second, and expects two batches of two, each in order and with unchanged timestamps.
func TestBatchStartsNewBatchWhenClockGoesBack(t *testing.T) {
	base := time.UnixMilli(int64(1722650790111))
	readTimes := []time.Time{base, base.Add(time.Millisecond), base.Add(-time.Minute), base.Add(-time.Minute + time.Millisecond)}
	next := 0
	converter := NewEntryToEventConverter(dummyInstanceID, func() time.Time {
		next++
		return readTimes[next-1]
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entriesChan := make(chan *sdjournal.JournalEntry)
	// Neither the size limit nor the timer can cut a batch here.
	batcher := NewBatcher(entriesChan, converter, WithMaxEvents(100), WithMaxWait(time.Hour))
	go batcher.Batch(ctx)
	go func() {
		for i, readTime := range readTimes {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, readTime, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
		// The last batch is only flushed on shutdown, and every entry has been consumed by now.
		cancel()
	}()

	var got [][]int64
	var cursors []string
	for range 2 {
		var b *Batch
		select {
		case b = <-batcher.Batches():
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d batches, want 2: %v", len(got), got)
		}
		var stamps []int64
		for _, e := range b.Events {
			stamps = append(stamps, aws.ToInt64(e.Timestamp))
		}
		got = append(got, stamps)
		cursors = append(cursors, b.Cursor)
	}
	assert.Equal(t, [][]int64{
		{readTimes[0].UnixMilli(), readTimes[1].UnixMilli()},
		{readTimes[2].UnixMilli(), readTimes[3].UnixMilli()},
	}, got)
	assert.Equal(t, []string{"cursor-1", "cursor-3"}, cursors)
}

func TestBatchOnMaxWait(t *testing.T) {
	timeUnixMilli := time.UnixMilli(int64(1722650790111))
	now := func() time.Time {
		return timeUnixMilli
	}
	converter := NewEntryToEventConverter(dummyInstanceID, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entriesChan := make(chan *sdjournal.JournalEntry)
	// Set MaxEvents big enough to not interfer with MaxWait.
	batcher := NewBatcher(entriesChan, converter, WithMaxEvents(1000), WithMaxWait(2*time.Second))
	go batcher.Batch(ctx)

	for i := 0; i < 10; i++ {
		entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
		entriesChan <- entry
	}
	batch := <-batcher.Batches()
	assert.Equal(t, "cursor-9", batch.Cursor)

	for i := 10; i < 30; i++ {
		entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
		entriesChan <- entry
	}
	batch = <-batcher.Batches()
	assert.Equal(t, "cursor-29", batch.Cursor)
}

// TestIsAuditLog tests the isAuditLog function.
func TestIsAuditLog(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		expected  bool
	}{
		{
			name:      "audit log",
			transport: "audit",
			expected:  true,
		},
		{
			name:      "syslog",
			transport: "syslog",
			expected:  false,
		},
		{
			name:      "kernel",
			transport: "kernel",
			expected:  false,
		},
		{
			name:      "empty transport",
			transport: "",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &sdjournal.JournalEntry{
				Fields: map[string]string{
					sdjournal.SD_JOURNAL_FIELD_TRANSPORT: tt.transport,
				},
			}
			result := isAuditLog(entry)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBatchSkipAuditLog tests that audit logs are skipped when skipAuditLog is enabled.
func TestBatchSkipAuditLog(t *testing.T) {
	timeUnixMilli := time.UnixMilli(int64(1722650790111))
	now := func() time.Time {
		return timeUnixMilli
	}
	converter := NewEntryToEventConverter(dummyInstanceID, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entriesChan := make(chan *sdjournal.JournalEntry)

	// Create batcher with skipAuditLog enabled.
	batcher := NewBatcher(entriesChan, converter, WithMaxEvents(10), WithMaxWait(time.Minute), WithSkipAuditLog(true))
	go batcher.Batch(ctx)

	// Send a mix of audit and non-audit logs.
	go func() {
		// Send 2 regular logs
		for i := 0; i < 2; i++ {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
		// Send 3 audit logs (should be skipped)
		for i := 2; i < 5; i++ {
			entry := getAuditLogEntry(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
		// Send 8 more regular logs to reach maxEvents (10 non-audit logs total), plus one more: a full batch
		// is flushed when the next entry arrives, not when the last slot is filled.
		for i := 5; i < 14; i++ {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
	}()

	// Verify we get a batch with only non-audit logs.
	batch := <-batcher.Batches()
	// Should have 10 events (skipped 3 audit logs out of the first 13)
	assert.Equal(t, 10, len(batch.Events))
	// Cursor should be from the last non-audit log processed (cursor-12, which is the 10th non-audit log)
	assert.Equal(t, "cursor-12", batch.Cursor)
}

// TestBatchDoNotSkipAuditLog tests that audit logs are included when skipAuditLog is disabled.
func TestBatchDoNotSkipAuditLog(t *testing.T) {
	timeUnixMilli := time.UnixMilli(int64(1722650790111))
	now := func() time.Time {
		return timeUnixMilli
	}
	converter := NewEntryToEventConverter(dummyInstanceID, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entriesChan := make(chan *sdjournal.JournalEntry)

	// Create batcher with skipAuditLog disabled (default).
	batcher := NewBatcher(entriesChan, converter, WithMaxEvents(10), WithMaxWait(time.Minute))
	go batcher.Batch(ctx)

	// Send a mix of audit and non-audit logs.
	go func() {
		// Send 2 regular logs
		for i := 0; i < 2; i++ {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
		// Send 3 audit logs (should NOT be skipped)
		for i := 2; i < 5; i++ {
			entry := getAuditLogEntry(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
		// Send 5 more regular logs to reach maxEvents, plus one more: a full batch is flushed when the next
		// entry arrives, not when the last slot is filled.
		for i := 5; i < 11; i++ {
			entry, _ := getExampleEntryAndEvent(dummyInstanceID, timeUnixMilli, fmt.Sprintf("cursor-%d", i))
			entriesChan <- entry
		}
	}()

	// Verify we get a batch with all logs including audit logs.
	batch := <-batcher.Batches()
	// Should have 10 events (all of the first 10 logs included)
	assert.Equal(t, 10, len(batch.Events))
	assert.Equal(t, "cursor-9", batch.Cursor)
}

// getAuditLogEntry creates a journal entry that is an audit log.
func getAuditLogEntry(instanceID string, timestamp time.Time, cursor string) *sdjournal.JournalEntry {
	entry := &sdjournal.JournalEntry{
		Fields: map[string]string{
			sdjournal.SD_JOURNAL_FIELD_PID:          "1",
			sdjournal.SD_JOURNAL_FIELD_UID:          "2",
			sdjournal.SD_JOURNAL_FIELD_GID:          "3",
			sdjournal.SD_JOURNAL_FIELD_COMM:         "auditd",
			sdjournal.SD_JOURNAL_FIELD_SYSTEMD_UNIT: "auditd",
			sdjournal.SD_JOURNAL_FIELD_PRIORITY:     "6",
			sdjournal.SD_JOURNAL_FIELD_BOOT_ID:      "f595e6391111111111111111372bf520",
			sdjournal.SD_JOURNAL_FIELD_MACHINE_ID:   "ec22e31111111111111111111111115b",
			sdjournal.SD_JOURNAL_FIELD_HOSTNAME:     "hello-server1.us-west-2.amazon.com",
			sdjournal.SD_JOURNAL_FIELD_TRANSPORT:    "audit",
			sdjournal.SD_JOURNAL_FIELD_MESSAGE:      "audit log message",
		},
		Cursor:             cursor,
		RealtimeTimestamp:  uint64(timestamp.UnixMicro()),
		MonotonicTimestamp: 897993707018,
	}
	return entry
}

func getExampleEntryAndEvent(instanceID string, timestamp time.Time, cursor string) (*sdjournal.JournalEntry, types.InputLogEvent) {
	entry := sdjournal.JournalEntry{
		Fields: map[string]string{
			sdjournal.SD_JOURNAL_FIELD_PID:               "1",
			sdjournal.SD_JOURNAL_FIELD_UID:               "2",
			sdjournal.SD_JOURNAL_FIELD_GID:               "3",
			"_ERRNO":                                     "1",
			sdjournal.SD_JOURNAL_FIELD_COMM:              "cowsay",
			sdjournal.SD_JOURNAL_FIELD_SYSTEMD_UNIT:      "sshd",
			sdjournal.SD_JOURNAL_FIELD_PRIORITY:          "6",
			sdjournal.SD_JOURNAL_FIELD_SYSLOG_FACILITY:   "4",
			sdjournal.SD_JOURNAL_FIELD_SYSLOG_IDENTIFIER: "sshd",
			sdjournal.SD_JOURNAL_FIELD_SYSLOG_PID:        "1",
			sdjournal.SD_JOURNAL_FIELD_BOOT_ID:           "f595e6391111111111111111372bf520",
			sdjournal.SD_JOURNAL_FIELD_MACHINE_ID:        "ec22e31111111111111111111111115b",
			sdjournal.SD_JOURNAL_FIELD_HOSTNAME:          "hello-server1.us-west-2.amazon.com",
			sdjournal.SD_JOURNAL_FIELD_TRANSPORT:         "syslog",
			sdjournal.SD_JOURNAL_FIELD_MESSAGE:           "connection lost",
			"OTHER_KEY":                                  "OTHTER_VALUE",
		},
		Cursor:             cursor,
		RealtimeTimestamp:  1722650790111473,
		MonotonicTimestamp: 897993707018,
	}

	message := fmt.Sprintf(`
{
    "instanceId": "%s",
    "realTimestamp": 1722650790111473,
    "pid": 1,
    "uid": 2,
    "gid": 3,
    "cmdName": "cowsay",
    "systemdUnit": "sshd",
    "bootId": "f595e6391111111111111111372bf520",
    "machineId": "ec22e31111111111111111111111115b",
    "hostname": "hello-server1.us-west-2.amazon.com",
    "transport": "syslog",
    "priority": "info",
    "message": "connection lost",
    "syslog": {
        "facility": 4,
        "ident": "sshd",
        "pid": 1
    }
}
`, instanceID)

	event := types.InputLogEvent{
		Message:   aws.String(message),
		Timestamp: aws.Int64(timestamp.UnixMilli()),
	}
	return &entry, event
}
