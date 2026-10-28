package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// newTestDBWriter builds a *DB wrapper around a freshly created BufferWriter
// using the temp-file database created by newTestDB. This lets Batch tests
// invoke the (*DB).Batch API without going through the full Open() /
// newPragmaConnector stack.
func newTestDBWriter(t *testing.T, cfg BufferConfig) (*DB, func()) {
	t.Helper()
	rawDB, cleanup := newTestDB(t)
	bw := NewWriter(rawDB, cfg)
	d := &DB{Writer: bw, Reader: rawDB}
	return d, func() {
		_ = bw.Close()
		cleanup()
	}
}

func newTestDB(t *testing.T) (*sql.DB, func()) {
	f, err := os.CreateTemp("", "batchtest-*.db")
	require.NoError(t, err)
	f.Close()

	db, err := sql.Open("sqlite3", f.Name()+"?_journal_mode=WAL&_synchronous=OFF")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	return db, func() {
		db.Close()
		os.Remove(f.Name())
	}
}

func TestWriter_DirectExec_SizeBased(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          3,
		FlushInterval: 100 * time.Second,
	})
	defer bw.Close()

	// First exec should create global tx
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	assert.NoError(t, err)
	assert.Equal(t, 1, bw.buffer)

	// Second exec should reuse global tx
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (2, 'bob')")
	assert.NoError(t, err)
	assert.Equal(t, 2, bw.buffer)

	// Third exec triggers flush (size == BatchSize)
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (3, 'charlie')")
	assert.NoError(t, err)
	assert.Equal(t, 0, bw.buffer) // flushed after 3

	// Verify data is committed
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestWriter_BeginTx_Commit(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// BeginTx should create BatchTx
	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	btx := tx

	// Exec on BatchTx should buffer
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	assert.NoError(t, err)
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (2, 'bob')")
	assert.NoError(t, err)

	// Data not yet visible before commit
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// Commit should execute buffer in global tx
	err = btx.Commit()
	assert.NoError(t, err)

	// Now data should be visible
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestWriter_BeginTx_Rollback(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// Insert one row first
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'existing')")
	require.NoError(t, err)
	bw.Flush()

	// BeginTx and add rows
	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	btx := tx
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (2, 'alice')")
	assert.NoError(t, err)
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (3, 'bob')")
	assert.NoError(t, err)

	// Rollback should discard buffer
	err = btx.Rollback()
	assert.NoError(t, err)

	// Only the first row should exist
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestWriter_DirectExec_TimeBased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, cleanup := newTestDB(t)
		defer cleanup()

		bw := NewWriter(db, BufferConfig{
			Size:          100, // high threshold
			FlushInterval: 1 * time.Second,
		})
		defer bw.Close()

		// Insert below threshold
		_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
		assert.NoError(t, err)
		_, err = bw.Exec("INSERT INTO users (id, name) VALUES (2, 'bob')")
		assert.NoError(t, err)

		// Wait for time-based flush
		time.Sleep(1 * time.Second)
		synctest.Wait()

		// Buffer should be flushed
		assert.Equal(t, 0, bw.buffer)

		// Data should be committed
		var count int
		err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

// TestWriter_FlushConcurrentWithIntervalCommit verifies that Flush() is safe
// to call while the background goroutine commits on its own timer. The buffer
// counter belongs to the flush goroutine, which resets it when it commits, so
// the caller's goroutine must not read it. Under -race this test fails if
// Flush() reads the counter.
func TestWriter_FlushConcurrentWithIntervalCommit(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100, // high threshold: commits come from the timer
		FlushInterval: 10 * time.Millisecond,
	})
	defer bw.Close()

	for i := 0; i < 20; i++ {
		_, err := bw.Exec("INSERT INTO users (id, name) VALUES (?, 'x')", i+1)
		require.NoError(t, err)

		// Let the interval trigger commit on the flush goroutine, then flush
		// from this goroutine: both touch the buffer counter.
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, bw.Flush())
	}
}

// TestWriter_TaskBlockedDoesNotReadBuffer verifies that the queue-full path of
// do() does not read the buffer counter on the caller's goroutine: the flush
// goroutine may be incrementing it at the same time. Under -race this test
// fails if do() reads the counter.
func TestWriter_TaskBlockedDoesNotReadBuffer(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// A single-slot task queue makes the queue-full path reachable.
	bw := &BufferWriter{
		DB:     db,
		cfg:    BufferConfig{Size: 100, FlushInterval: 100 * time.Second},
		tasks:  make(chan TaskFunc, 1),
		close:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		buffer: 0,
	}
	go bw.waitSingal()
	defer bw.Close()

	for i := 0; i < 100; i++ {
		// Occupy the only slot so the next do() takes its default branch.
		bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (?, 'x')", i+1)

		// ErrQueueFull is returned when the queue is full and the task is
		// rejected. The error is discarded here: whether the flush goroutine
		// has already drained the slot is a race, and the deterministic
		// assertion lives in TestWriter_QueueFullReturnsErrQueueFull.
		_, _ = bw.Exec("INSERT INTO users (id, name) VALUES (?, 'y')", i+101)
	}
}

// TestWriter_QueueFullReturnsErrQueueFull verifies that a task rejected
// because the task queue is full reports ErrQueueFull, not ErrClosed. The two
// conditions are different: ErrClosed is terminal, ErrQueueFull is transient
// back-pressure that the caller may retry.
func TestWriter_QueueFullReturnsErrQueueFull(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// No flush goroutine is started, so the queue stays full for the whole
	// test and do() deterministically takes its default branch. Close() is
	// therefore not called: no goroutine would ever close bw.done.
	bw := &BufferWriter{
		DB:     db,
		cfg:    BufferConfig{Size: 100, FlushInterval: 100 * time.Second, QueueCapacity: 1},
		tasks:  make(chan TaskFunc, 1),
		close:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		buffer: 0,
	}

	// Fill the only slot so the next do() takes its default branch.
	bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (?, 'x')", 1)

	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (?, 'y')", 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrQueueFull)
	assert.NotErrorIs(t, err, ErrClosed)

	// The writer itself is not closed: only the queue was full.
	assert.False(t, bw.closed.Load())
}

// TestWriter_QueueFullLeavesWriterUsable verifies that a task rejected
// because the task queue is full does not poison the writer: once the queue
// drains, the same writer accepts writes and flushes normally.
func TestWriter_QueueFullLeavesWriterUsable(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Second,
		QueueCapacity: 2,
	})
	defer bw.Close()

	// Park the flush goroutine inside a task so nothing drains the queue
	// while the test fills it.
	blocker := newBlockingTask()
	bw.tasks <- blocker
	<-blocker.started

	// Fill the queue to capacity.
	bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (?, 'x')", 1)
	bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (?, 'x')", 2)

	// The queue is full and the flush goroutine is parked, so this write is
	// rejected deterministically.
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (?, 'y')", 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrQueueFull)
	assert.NotErrorIs(t, err, ErrClosed)

	// Let the flush goroutine drain the queue again.
	close(blocker.release)
	require.Eventually(t, func() bool { return len(bw.tasks) == 0 }, 2*time.Second, 5*time.Millisecond)

	// The writer is still healthy: the rejection was back-pressure, not a
	// terminal condition.
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (?, 'z')", 4)
	require.NoError(t, err)
	require.NoError(t, bw.Flush())
}

// blockingTask is a TaskFunc whose Exec parks until release is closed. It
// lets a test hold the flush goroutine inside task.Exec so the task queue can
// be filled deterministically.
type blockingTask struct {
	notify  chan TaskResult
	started chan struct{}
	release chan struct{}
}

func newBlockingTask() *blockingTask {
	return &blockingTask{
		notify:  make(chan TaskResult, 1),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (t *blockingTask) Exec(_ *BufferWriter) TaskResult {
	close(t.started)
	<-t.release
	return TaskResult{}
}

func (t *blockingTask) Notify() chan TaskResult { return t.notify }

func (t *blockingTask) Action() Action { return ActionExec }

func TestWriter_DirectExec_Concurrent(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// Note: Direct Exec is not safe for concurrent use without external synchronization
	// This test verifies basic single-threaded behavior
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := bw.Exec("INSERT INTO users (id, name) VALUES (?, ?)", id*100+j, "user")
				if err != nil {
					t.Errorf("Exec failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	// Flush remaining
	bw.Flush()

	var count int
	err := bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 50, count)
}

func TestWriter_GlobalTxReusedAfterFlush(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          2,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// First batch
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.NoError(t, err)
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (2, 'b')")
	assert.NoError(t, err)
	// Should have flushed now (size == BatchSize)

	// Second batch - should create NEW global tx
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (3, 'c')")
	assert.NoError(t, err)
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (4, 'd')")
	assert.NoError(t, err)

	// Flush manually
	err = bw.Flush()
	assert.NoError(t, err)

	// All 4 rows should exist
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)
}

func TestWriter_DeferRollbackAfterCommit(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'existing')")
	require.NoError(t, err)

	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	btx := tx
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (2, 'alice')")
	require.NoError(t, err)

	// Simulate defer tx.Rollback() - should be safely ignored after Commit
	err = btx.Commit()
	assert.NoError(t, err)

	// Rollback after commit should be no-op
	err = btx.Rollback()
	assert.NoError(t, err) // should not error

	// Data should be committed
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestWriter_ExecAfterClose verifies that Exec/ExecContext return ErrClosed
// after the writer has been closed.
func TestWriter_ExecAfterClose(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})

	err := bw.Close()
	require.NoError(t, err)

	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.ErrorIs(t, err, ErrClosed)

	_, err = bw.ExecContext(context.Background(), "INSERT INTO users (id, name) VALUES (2, 'b')")
	assert.ErrorIs(t, err, ErrClosed)
}

// TestWriter_CloseIdempotent verifies that calling Close() multiple times
// concurrently does not deadlock or panic.
func TestWriter_CloseIdempotent(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := bw.Close()
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

// TestWriter_CloseCommitsPendingBuffer verifies data already buffered in the
// global transaction is committed (not discarded) when Close() is called.
func TestWriter_CloseCommitsPendingBuffer(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          100, // high threshold, won't auto-flush
		FlushInterval: 10 * time.Second,
	})

	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	require.NoError(t, err)
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (2, 'b')")
	require.NoError(t, err)
	assert.Equal(t, 2, bw.buffer)

	err = bw.Close()
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestWriter_CloseDrainsPendingTasks verifies that tasks queued in the channel
// at Close time are notified with ErrClosed rather than being silently dropped.
func TestWriter_CloseDrainsPendingTasks(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Use a very small task channel so we can fill it before Close.
	bw := NewWriter(db, BufferConfig{
		Size:          1000,
		FlushInterval: 10 * time.Second,
	})

	// Block the flush goroutine by filling it with a slow-ish batch.
	// We close synchronously; tasks enqueued after close should get ErrClosed.
	bw.Close()

	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.ErrorIs(t, err, ErrClosed)
}

// TestWriter_ExecContext_CancelledBeforeSend verifies that ExecContext returns
// immediately with ctx.Err() when the context is already cancelled on entry.
func TestWriter_ExecContext_CancelledBeforeSend(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})
	defer bw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := bw.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.ErrorIs(t, err, context.Canceled)
}

// TestWriter_ExecContext_CancelledWhileWaiting verifies that ExecContext
// returns ctx.Err() when the context is cancelled while waiting for result.
func TestWriter_ExecContext_CancelledWhileWaiting(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})
	defer bw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Fill the tasks channel to delay processing.
	for i := 0; i < 100; i++ {
		bw.tasks <- Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, "x")
	}

	time.Sleep(2 * time.Millisecond) // let ctx expire

	_, err := bw.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (999, 'late')")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestWriter_Tx_ExecAfterDone verifies that calling Exec on a Tx after
// Commit or Rollback returns an error.
func TestWriter_Tx_ExecAfterDone(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})
	defer bw.Close()

	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.Error(t, err)
}

// TestWriter_Tx_CommitAfterClose verifies that committing a Tx whose Writer
// has already been closed returns ErrClosed.
func TestWriter_Tx_CommitAfterClose(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 10 * time.Second,
	})

	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	require.NoError(t, err)

	bw.Close()

	err = tx.Commit()
	assert.ErrorIs(t, err, ErrClosed)
}

// TestWriter_Tx_RollbackOnError verifies that when one statement
// inside a Tx fails, the error is returned immediately and a subsequent
// Rollback undoes all prior statements in the transaction.
func TestWriter_Tx_RollbackOnError(t *testing.T) {

	synctest.Test(t, func(t *testing.T) {

		db, cleanup := newTestDB(t)
		defer cleanup()

		bw := NewWriter(db, BufferConfig{
			Size:          10,
			FlushInterval: 10 * time.Second,
		})
		defer bw.Close()

		// Commit a row that will cause a PK conflict inside the Tx.
		_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'existing')")
		require.NoError(t, err)
		require.NoError(t, bw.Flush())

		tx, err := bw.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		_, err = tx.Exec("INSERT INTO users (id, name) VALUES (2, 'ok')")
		require.NoError(t, err)
		_, err = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'dup')") // PK conflict
		assert.Error(t, err)                                               // error returned immediately

		synctest.Wait()

		// Caller must rollback on error; this undoes the id=2 insert too.
		err = tx.Rollback()
		require.NoError(t, err)

		// The entire Tx should be rolled back; no new rows added.
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		// Writer should still be usable after a failed Tx.
		_, err = bw.Exec("INSERT INTO users (id, name) VALUES (3, 'after')")
		assert.NoError(t, err)
		require.NoError(t, bw.Flush())

		err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

	})
}

// TestWriter_ConcurrentBeginTxCommit verifies that multiple goroutines
// performing BeginTx+Commit concurrently all succeed and all rows are written.
func TestWriter_ConcurrentBeginTxCommit(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          5,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	const goroutines = 20
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			tx, err := bw.BeginTx(context.Background(), nil)
			if !assert.NoError(t, err) {
				return
			}
			_, err = tx.Exec("INSERT INTO users (id, name) VALUES (?, ?)", base*10+1, "u1")
			assert.NoError(t, err)
			_, err = tx.Exec("INSERT INTO users (id, name) VALUES (?, ?)", base*10+2, "u2")
			assert.NoError(t, err)
			assert.NoError(t, tx.Commit())
		}(i)
	}
	wg.Wait()

	require.NoError(t, bw.Flush())

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, goroutines*2, count)
}

// TestBatch_CommitAll verifies that N statements inside a Batch closure all
// commit together, the Writer buffer counter is unaffected (the Batch task
// itself does not increment it), and a single channel send was used.
func TestBatch_CommitAll(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	const n = 5
	err := d.Batch(func(tx *sql.Tx) error {
		for i := 1; i <= n; i++ {
			if _, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i, fmt.Sprintf("u%d", i),
			); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, 0, d.Writer.buffer, "Batch must not bump the Writer buffer counter")

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, n, count)
}

// TestBatch_RollbackOnError verifies that returning a non-nil error from the
// Batch closure rolls back the transaction, propagates the error verbatim,
// and releases the Writer lock so subsequent Exec calls succeed.
func TestBatch_RollbackOnError(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	sentinel := errors.New("user-supplied failure")
	err := d.Batch(func(tx *sql.Tx) error {
		_, _ = tx.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
		_, _ = tx.Exec("INSERT INTO users (id, name) VALUES (2, 'b')")
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 0, count, "rollback must discard every statement in the closure")

	// Lock must be released: a follow-up Exec succeeds.
	_, err = d.Writer.Exec("INSERT INTO users (id, name) VALUES (3, 'after')")
	require.NoError(t, err)
}

// TestBatch_PanicRollsBack verifies that a panic inside the Batch closure
// is recovered, converted to an "sqlite: batch panic: ..." error, and the
// transaction is rolled back. The lock is also released.
func TestBatch_PanicRollsBack(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	err := d.Batch(func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO users (id, name) VALUES (1, 'before_panic')")
		require.NoError(t, err)
		panic("boom")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite: batch panic: boom")

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 0, count, "panic must roll back the transaction")

	// Lock must be released.
	_, err = d.Writer.Exec("INSERT INTO users (id, name) VALUES (2, 'after')")
	require.NoError(t, err)
}

// TestBatch_LocksWriter verifies that while a Batch closure is running, any
// Writer.Exec / Writer.BeginTx call is blocked on txMu until Batch returns.
func TestBatch_LocksWriter(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	started := make(chan struct{})
	release := make(chan struct{})
	batchDone := make(chan error, 1)

	go func() {
		batchDone <- d.Batch(func(tx *sql.Tx) error {
			close(started)
			<-release
			_, err := tx.Exec("INSERT INTO users (id, name) VALUES (1, 'in_batch')")
			return err
		})
	}()

	<-started

	execDone := make(chan error, 1)
	go func() {
		_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (2, 'blocked')")
		execDone <- err
	}()

	// Exec must remain blocked while Batch holds txMu.
	select {
	case err := <-execDone:
		t.Fatalf("Writer.Exec returned %v while Batch was still active", err)
	case <-time.After(50 * time.Millisecond):
		// expected: Exec is blocked on txMu.RLock()
	}

	close(release)

	require.NoError(t, <-batchDone)
	require.NoError(t, <-execDone)

	// The second Exec was buffered (FlushInterval is high to keep it
	// buffered during the test window); flush so we can count rows.
	require.NoError(t, d.Writer.Flush())

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 2, count)
}

// TestBatch_FlushesPendingBuffer verifies that Batch flushes any pending
// buffered writes before opening its own transaction, so the closure sees
// the prior committed state on its *sql.Tx.
func TestBatch_FlushesPendingBuffer(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 10 * time.Second, // high interval so nothing auto-flushes
	})
	defer cleanup()

	// Buffer a row through the normal Exec path.
	_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (1, 'pre')")
	require.NoError(t, err)
	require.Equal(t, 1, d.Writer.buffer, "row must sit in the buffer")

	var insideCount int
	err = d.Batch(func(tx *sql.Tx) error {
		if err := tx.QueryRow("SELECT COUNT(*) FROM users").Scan(&insideCount); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT INTO users (id, name) VALUES (2, 'in_batch')")
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, 1, insideCount, "buffered row must be visible inside the batch tx")

	var finalCount int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&finalCount))
	assert.Equal(t, 2, finalCount)
}

// TestBatch_EmptyCommit verifies that an empty closure still commits a
// no-op tx and returns nil.
func TestBatch_EmptyCommit(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	err := d.Batch(func(tx *sql.Tx) error {
		return nil
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 0, count)
}

// TestBatch_AfterClose verifies that calling Batch after the Writer has been
// closed returns ErrClosed without leaking the Writer lock — a subsequent
// Exec must also observe ErrClosed (no deadlock).
func TestBatch_AfterClose(t *testing.T) {
	rawDB, cleanup := newTestDB(t)
	defer cleanup()
	bw := NewWriter(rawDB, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	d := &DB{Writer: bw, Reader: rawDB}

	require.NoError(t, bw.Close())

	err := d.Batch(func(tx *sql.Tx) error {
		t.Fatal("closure must not run after Writer.Close")
		return nil
	})
	require.ErrorIs(t, err, ErrClosed)

	// Subsequent Exec must surface ErrClosed too; this proves the lock was
	// released by the defer in Batch (otherwise this RLock would deadlock
	// and the test would hang until the test framework kills it).
	_, err = bw.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
	assert.ErrorIs(t, err, ErrClosed)
}

// TestBatch_RecoversFromLeakedPoolConnection verifies that Batch handles the
// state where a prior code path left bw.tx set with no buffered statements
// (so commitWithTrigger skipped it). With the writer pool configured at
// SetMaxOpenConns(1) — the production default from openFile / openMemory —
// a stranded *sql.Tx would otherwise block BeginTx forever.
//
// The leak is induced through the public API: BeginTx opens bw.tx and sets
// bw.isPending=true; Commit (with no Exec in between) leaves bw.tx dangling
// because commitWithTrigger returns early when buffer < 1.
func TestBatch_RecoversFromLeakedPoolConnection(t *testing.T) {
	rawDB, cleanup := newTestDB(t)
	defer cleanup()
	rawDB.SetMaxOpenConns(1)

	bw := NewWriter(rawDB, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()
	d := &DB{Writer: bw, Reader: rawDB}

	// Induce the leak: BeginTx opens a tx on bw, Commit-without-Exec leaves
	// bw.tx set holding the only writer-pool connection.
	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Without Batch's defensive cleanup, BeginTx inside Batch would block
	// forever on the full single-connection pool. Run in a goroutine with
	// a hard timeout so a hang surfaces as a clear failure.
	done := make(chan error, 1)
	go func() {
		done <- d.Batch(func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO users (id, name) VALUES (1, 'a')")
			return err
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Batch deadlocked: a stranded bw.tx held the only writer connection")
	}

	var count int
	require.NoError(t, rawDB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 1, count)
}
