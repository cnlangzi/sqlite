package sqlite

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	bw := NewWriter(db, Buffer{
		BufferSize:    3,
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

	bw := NewWriter(db, Buffer{
		BufferSize:    10,
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

	bw := NewWriter(db, Buffer{
		BufferSize:    10,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// Insert one row first
	_, err := bw.Exec("INSERT INTO users (id, name) VALUES (1, 'existing')")
	require.NoError(t, err)
	bw.Commit()

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

		bw := NewWriter(db, Buffer{
			BufferSize:    100, // high threshold
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

func TestWriter_TimeBasedBlockedByActiveBatchTx(t *testing.T) {
	t.SkipNow()
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, Buffer{
		BufferSize:    2,
		FlushInterval: 100 * time.Millisecond,
	})
	defer bw.Close()

	// BeginTx - holds lock
	tx, err := bw.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	btx := tx

	// Insert rows into BatchTx
	_, err = btx.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')")
	assert.NoError(t, err)

	// Time-based should not flush (blocked by active BatchTx)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, bw.buffer) // pending but not flushed

	// Commit should release lock and trigger flush
	err = btx.Commit()
	assert.NoError(t, err)

	// Buffer should be empty now
	assert.Equal(t, 0, bw.buffer)

	// Data should be committed
	var count int
	err = bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestWriter_DirectExec_Concurrent(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, Buffer{
		BufferSize:    100,
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
	bw.Commit()

	var count int
	err := bw.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 50, count)
}

func TestWriter_GlobalTxReusedAfterFlush(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, Buffer{
		BufferSize:    2,
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
	err = bw.Commit()
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

	bw := NewWriter(db, Buffer{
		BufferSize:    10,
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
