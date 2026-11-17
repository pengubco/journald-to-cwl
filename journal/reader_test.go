package journal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
	"github.com/stretchr/testify/assert"
)

func TestReadEntries(t *testing.T) {
	cases := []struct {
		name       string
		numEntries int
	}{
		{"all entries are ready to read", 10},
		{"no entries are ready to read", 0},
		{"some entries are ready to read", 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var entries []*sdjournal.JournalEntry
			for i := 0; i < tc.numEntries; i++ {
				entries = append(entries, &sdjournal.JournalEntry{
					Cursor: fmt.Sprintf("cursor-%d", i),
				})
			}
			rand.Shuffle(tc.numEntries, func(i, j int) { //nolint:gosec // Test input order only.
				entries[i], entries[j] = entries[j], entries[i]
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			j := newJournalStub(entries)
			r := NewReader(j, WithWaitForDataTimeout(time.Millisecond))
			go r.Read(ctx)

			var entriesReceived []*sdjournal.JournalEntry
			for i := 0; i < tc.numEntries; i++ {
				entriesReceived = append(entriesReceived, <-r.Entries())
			}
			assert.Equal(t, entries, entriesReceived)
		})
	}
}

func TestPanicOnError(t *testing.T) {
	cases := []struct {
		name                string
		shouldNextError     bool
		shouldGetEntryError bool
	}{
		{"Next returns error", true, false},
		{"GetEntry returns error", false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			entries := make([]*sdjournal.JournalEntry, 1000)
			j := newJournalStub(entries)
			j.setShouldGetEntryError(tc.shouldGetEntryError)
			j.setShouldNextError(tc.shouldNextError)
			r := NewReader(j, WithWaitForDataTimeout(time.Millisecond))
			assert.Panics(t, func() {
				r.Read(ctx)
			})
		})
	}
}

// journalStub pretends to be journal reader and returns the given entries one by one.
type journalStub struct {
	entries []*sdjournal.JournalEntry
	index   int

	mu sync.Mutex

	shouldNextError     bool
	shouldGetEntryError bool
}

func newJournalStub(
	entries []*sdjournal.JournalEntry,
) *journalStub {
	return &journalStub{
		entries: entries,
		index:   -1,
	}
}

func (j *journalStub) Next() (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.shouldNextError {
		return 0, errors.New("cannot advance cursor")
	}
	j.index++
	if j.index >= len(j.entries) {
		return 0, nil
	}
	return 1, nil
}

func (j *journalStub) GetEntry() (*sdjournal.JournalEntry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.shouldGetEntryError {
		return nil, errors.New("cannot read entry")
	}
	return j.entries[j.index], nil
}

func (j *journalStub) Wait(d time.Duration) int {
	time.Sleep(d)
	return 0
}

func (j *journalStub) setShouldNextError(b bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.shouldNextError = b
}

func (j *journalStub) setShouldGetEntryError(b bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.shouldGetEntryError = b
}
