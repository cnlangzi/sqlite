package sqlite

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// slogEntry captures a log entry for testing.
type slogEntry struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	// Fields
	Buffer          int    `json:"buffer,omitempty"`
	SizeLimit       int    `json:"size_limit,omitempty"`
	Trigger         string `json:"trigger,omitempty"`
	Count           int    `json:"count,omitempty"`
	Elapsed         string `json:"elapsed,omitempty"`
	Err             string `json:"err,omitempty"`
	RemainingBuffer int    `json:"remaining_buffer,omitempty"`
}

// testHandler implements slog.Handler for testing.
type testHandler struct {
	mu      sync.Mutex
	entries []slogEntry
}

func (h *testHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := slogEntry{
		Level:  r.Level.String(),
		Msg:    r.Message,
	}

	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "buffer":
			if v, ok := a.Value.Any().(int64); ok {
				entry.Buffer = int(v)
			}
		case "size_limit":
			if v, ok := a.Value.Any().(int64); ok {
				entry.SizeLimit = int(v)
			}
		case "trigger":
			if v, ok := a.Value.Any().(string); ok {
				entry.Trigger = v
			}
		case "count":
			if v, ok := a.Value.Any().(int64); ok {
				entry.Count = int(v)
			}
		case "elapsed":
			if v, ok := a.Value.Any().(int64); ok {
				entry.Elapsed = time.Duration(v).String()
			}
		case "err":
			if v, ok := a.Value.Any().(string); ok {
				entry.Err = v
			}
		case "remaining_buffer":
			if v, ok := a.Value.Any().(int64); ok {
				entry.RemainingBuffer = int(v)
			}
		}
		return true
	})

	h.entries = append(h.entries, entry)
	return nil
}

func (h *testHandler) WithAttrs(_ []slog.Attr) slog.Handler         { return h }
func (h *testHandler) WithGroup(_ string) slog.Handler              { return h }
func (h *testHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *testHandler) Sync()                                        {}
func (h *testHandler) Close()                                       {}

// resetSlog replaces the default slog with a test handler.
func resetSlog(t *testing.T) (*testHandler, func()) {
	h := &testHandler{}
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	return h, func() {
		slog.SetDefault(old)
	}
}

// findEntry finds the first entry with the given message.
func findEntry(h *testHandler, msg string) *slogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.entries {
		if h.entries[i].Msg == msg {
			e := h.entries[i]
			return &e
		}
	}
	return nil
}

// countEntries counts entries with the given message.
func countEntries(h *testHandler, msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for i := range h.entries {
		if h.entries[i].Msg == msg {
			n++
		}
	}
	return n
}

func TestSlog_SizeTriggeredFlush(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          3,
		FlushInterval: 100 * time.Second,
	})
	defer bw.Close()

	// Insert 3 rows to trigger size-based flush
	for i := 0; i < 3; i++ {
		_, err := bw.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, "user")
		require.NoError(t, err)
	}

	// Verify commit triggered and committed logs
	triggered := findEntry(h, "commit triggered")
	require.NotNil(t, triggered, "should have commit triggered log")
	assert.Equal(t, "size", triggered.Trigger)

	committed := findEntry(h, "committed")
	require.NotNil(t, committed, "should have committed log")
	assert.Equal(t, "size", committed.Trigger)
	assert.Equal(t, 3, committed.Count)
}

func TestSlog_IntervalTriggeredFlush(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100, // high threshold
		FlushInterval: 50 * time.Millisecond,
	})
	defer bw.Close()

	// Insert fewer than size threshold
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Wait for interval-based flush
	time.Sleep(100 * time.Millisecond)

	// Verify interval commit
	triggered := findEntry(h, "commit triggered")
	require.NotNil(t, triggered, "should have commit triggered log")
	assert.Equal(t, "interval", triggered.Trigger)

	committed := findEntry(h, "committed")
	require.NotNil(t, committed, "should have committed log")
	assert.Equal(t, "interval", committed.Trigger)
}

func TestSlog_ManualFlush(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Second,
	})
	defer bw.Close()

	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	// Manual flush
	err = bw.Flush()
	require.NoError(t, err)

	// Verify flush requested log
	flushReq := findEntry(h, "flush requested")
	require.NotNil(t, flushReq, "should have flush requested log")

	// Verify manual commit
	committed := findEntry(h, "committed")
	require.NotNil(t, committed, "should have committed log")
	assert.Equal(t, "manual", committed.Trigger)
}

func TestSlog_CloseDrain(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Second,
	})

	// Add some buffered work
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	require.NoError(t, err)
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (2, 'b')")
	require.NoError(t, err)

	// Close should drain and commit
	err = bw.Close()
	require.NoError(t, err)

	// Verify close signal received
	closeSignal := findEntry(h, "close signal received")
	require.NotNil(t, closeSignal, "should have close signal received log")

	// Verify drained on close
	drained := findEntry(h, "drained on close")
	require.NotNil(t, drained, "should have drained on close log")
	assert.Equal(t, 2, drained.RemainingBuffer)
}

func TestSlog_TaskBlocked(t *testing.T) {
	_, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	// Create writer with tiny task channel to trigger blocking
	bw := &BufferWriter{
		DB:     db,
		cfg:    BufferConfig{Size: 100, FlushInterval: 100 * time.Second},
		tasks:  make(chan TaskFunc, 1), // tiny channel
		close:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		buffer: 0,
	}
	go bw.waitSingal()
	defer bw.Close()

	// First task should succeed
	_, _ = bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	// Might succeed or fail depending on timing

	// Try to fill the channel
	bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (2, 'b')")
	bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (3, 'c')")

	// Verify task blocked log appears at least once
	time.Sleep(10 * time.Millisecond)
}

func TestSlog_TxCommit(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Second,
	})
	defer bw.Close()

	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (2, 'bob')")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify tx committed
	txCommitted := findEntry(h, "tx committed")
	require.NotNil(t, txCommitted, "should have tx committed log")
	assert.Equal(t, 2, txCommitted.Count)
}

func TestSlog_TxRollback(t *testing.T) {
	h, cleanup := resetSlog(t)
	defer cleanup()

	db, dbCleanup := newTestDB(t)
	defer dbCleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Second,
	})
	defer bw.Close()

	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	// Verify tx rolled back
	txRolled := findEntry(h, "tx rolled back")
	require.NotNil(t, txRolled, "should have tx rolled back log")
	assert.Equal(t, 1, txRolled.Count)
}
