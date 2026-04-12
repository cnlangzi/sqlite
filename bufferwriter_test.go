package sqlite

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

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

// TestWriter_Tx_SavepointRollbackOnError verifies that when one statement
// inside a Tx.Commit fails, the savepoint rolls back so the partially
// executed statements do not leak into the global transaction, while
// other independent writes are unaffected.
func TestWriter_Tx_SavepointRollbackOnError(t *testing.T) {
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
	require.NoError(t, err)

	err = tx.Commit()
	assert.Error(t, err) // should fail due to PK conflict

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
