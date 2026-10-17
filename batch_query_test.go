package sqlite

// Write / Query / Batch coordination tests. The goal is to verify that
// the three surfaces — Writer writes (Exec / ExecContext / BeginTx /
// Batch) and Reader reads (Query / QueryContext / QueryRow /
// QueryRowContext) — cooperate correctly:
//
//   - Reader never observes uncommitted or in-flight Batch writes.
//   - After Batch returns, Reader sees exactly the Batch's effect
//     atomically (no partial visibility across parallel Batches).
//   - Inside a Batch closure, tx queries see prior writes (both prior
//     Exec rows that Batch flushed and prior closure inserts).
//   - A slow or cancelled Reader query never blocks or breaks Batch.
//   - Under heavy mixed read+write load, no goroutine deadlocks and
//     every row eventually becomes visible to the Reader.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch_Query_ReaderIsolationFromUncommittedBatch verifies that
// while a Batch is mid-execution — its closure has issued INSERTs but
// the Batch has not yet committed — the Reader pool cannot observe
// those writes. This is the ACID isolation guarantee: a Batch is one
// atomic unit, so its in-flight writes must be invisible to other
// connections.
func TestBatch_Query_ReaderIsolationFromUncommittedBatch(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	// Seed one row so the table is non-empty.
	_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (1, 'pre')")
	require.NoError(t, err)
	require.NoError(t, d.Writer.Flush())

	inserted := make(chan struct{})
	release := make(chan struct{})
	batchDone := make(chan error, 1)
	go func() {
		batchDone <- d.Batch(func(tx *sql.Tx) error {
			// Insert rows into the Batch's tx, but do NOT return
			// until release — so the Batch has not committed yet.
			for i := 2; i <= 5; i++ {
				if _, err := tx.Exec(
					"INSERT INTO users (id, name) VALUES (?, ?)",
					i, fmt.Sprintf("in_batch_%d", i),
				); err != nil {
					return err
				}
			}
			close(inserted)
			<-release
			return nil
		})
	}()

	<-inserted

	// Reader must NOT see the Batch's in-flight rows. Only id=1 is
	// visible. Try every Query / QueryRow / QueryContext /
	// QueryRowContext surface — none should see rows 2..5.
	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 1, n, "Reader must not see in-flight Batch writes (QueryRow)")

	require.NoError(t, d.Reader.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 1, n, "Reader must not see in-flight Batch writes (QueryRowContext)")

	rows, err := d.Reader.Query("SELECT id FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int{1}, ids,
		"Reader must not see in-flight Batch writes (Query)")

	close(release)
	require.NoError(t, <-batchDone)

	// After Batch commits, every row becomes visible atomically.
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 5, n, "all Batch rows become visible after commit")
}

// TestBatch_Query_ClosureReadYourWrite verifies that queries issued
// from inside the Batch closure observe (a) rows inserted earlier by
// the buffered Exec path that Batch flushed on entry, and (b) rows
// inserted earlier in the same closure. This is read-your-own-write
// inside the Batch's *sql.Tx.
func TestBatch_Query_ClosureReadYourWrite(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 10 * time.Second, // large: nothing auto-flushes
	})
	defer cleanup()

	// Buffer a row through Exec. It sits in bw.tx; Batch will flush
	// it on entry, so the closure sees it as committed.
	_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (1, 'exec_buf')")
	require.NoError(t, err)
	require.Equal(t, 1, d.Writer.buffer, "row must sit in the buffer")

	err = d.Batch(func(tx *sql.Tx) error {
		// (a) Buffered Exec row is visible — Batch flushed it.
		var seen string
		if err := tx.QueryRow(
			"SELECT name FROM users WHERE id = 1",
		).Scan(&seen); err != nil {
			return fmt.Errorf("closure read buffered row: %w", err)
		}
		if seen != "exec_buf" {
			return fmt.Errorf("expected 'exec_buf', got %q", seen)
		}

		// Insert two more rows in the closure.
		for i := 2; i <= 3; i++ {
			if _, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i, fmt.Sprintf("closure_%d", i),
			); err != nil {
				return err
			}
		}

		// (b) Same-closure inserts are visible (read-your-own-write).
		rows, err := tx.Query("SELECT id FROM users ORDER BY id")
		if err != nil {
			return err
		}
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		assert.Equal(t, []int{1, 2, 3}, ids,
			"closure must see its own inserts via tx.Query")

		// Also try QueryContext + QueryRowContext inside the closure.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var n int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM users").Scan(&n); err != nil {
			return err
		}
		assert.Equal(t, 3, n, "QueryRowContext sees all closure-side rows")

		return nil
	})
	require.NoError(t, err)

	// After commit, Reader sees the same set.
	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 3, n)
}

// TestBatch_Query_NeverSeesPartialBatch verifies that under parallel
// Batch writers, a concurrent Reader never observes a partially-
// committed Batch. Each Batch adds a fixed number of rows; the
// Reader's count must therefore always be (seed + k*perBatch) for
// some integer k.
func TestBatch_Query_NeverSeesPartialBatch(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	// Seed rows so the Reader sees a non-zero baseline.
	err := d.Batch(func(tx *sql.Tx) error {
		for i := 1; i <= 10; i++ {
			if _, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i, fmt.Sprintf("seed-%d", i),
			); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	const (
		numBatches = 8
		perBatch   = 5
		numReaders = 4
		readsEach  = 25
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for g := 0; g < numBatches; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := 1000 + g*perBatch
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < perBatch; i++ {
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							base+i+1, fmt.Sprintf("b%d-%d", g, i),
						); err != nil {
							return err
						}
					}
					return nil
				})
				assert.NoError(t, err, "batch %d", g)
			}(g)
		}

		for r := 0; r < numReaders; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				for i := 0; i < readsEach; i++ {
					var n int
					if err := d.Reader.QueryRow(
						"SELECT COUNT(*) FROM users",
					).Scan(&n); err != nil {
						t.Errorf("reader %d iter %d: %v", r, i, err)
						return
					}
					if n < 10 || n > 10+numBatches*perBatch {
						t.Errorf("reader %d iter %d: count %d out of range",
							r, i, n)
						return
					}
					if (n-10)%perBatch != 0 {
						t.Errorf(
							"reader %d iter %d: saw partial Batch "+
								"(count=%d, perBatch=%d)",
							r, i, n, perBatch)
						return
					}
				}
			}(r)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: parallel Batches + concurrent Readers did not complete")
	}

	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 10+numBatches*perBatch, n,
		"every parallel Batch's rows must be visible after completion")
}

// TestBatch_Query_LongReaderDoesNotBlockBatch verifies that while a
// Reader is holding a connection mid-iteration, a Batch on the Writer
// side still completes promptly. This proves the two paths are not
// gated by the same lock — the Writer's txMu governs writers only,
// and the Reader's pool is independent.
//
// The "slow Reader" is implemented by opening Rows, consuming one row
// (which holds the Reader connection), then waiting for a release
// signal before iterating further. The Batch runs while the Reader
// is parked between rows.
func TestBatch_Query_LongReaderDoesNotBlockBatch(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	// Seed enough rows that the Reader's iteration is non-trivial.
	err := d.Batch(func(tx *sql.Tx) error {
		for i := 1; i <= 50; i++ {
			if _, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i, fmt.Sprintf("seed-%d", i),
			); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Hold a Reader connection open. We drain one row (to ensure the
	// connection is actually checked out), then park on a release
	// channel while the Batch runs.
	readerHolding := make(chan struct{})
	readerRelease := make(chan struct{})
	readerDone := make(chan struct{}, 1)
	go func() {
		rows, err := d.Reader.Query("SELECT id FROM users ORDER BY id")
		if err != nil {
			t.Errorf("reader query: %v", err)
			readerDone <- struct{}{}
			return
		}
		defer rows.Close()

		// Consume one row so the Reader connection is checked out.
		if rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Errorf("reader scan: %v", err)
				readerDone <- struct{}{}
				return
			}
		}
		close(readerHolding)
		<-readerRelease
		readerDone <- struct{}{}
	}()

	<-readerHolding

	// Reader is parked on a connection. Run a Batch on the Writer.
	// It must complete promptly — they use independent connections.
	start := time.Now()
	err = d.Batch(func(tx *sql.Tx) error {
		for i := 1000; i < 1010; i++ {
			if _, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i, fmt.Sprintf("batch-%d", i),
			); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 2*time.Second,
		"Batch must not wait on a Reader holding a connection")

	// Release the Reader and verify Batch's rows landed.
	close(readerRelease)
	<-readerDone

	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 60, n, "Batch rows are present even while a Reader was parked")
}

// TestBatch_Query_CancelDoesNotAffectBatch verifies that cancelling a
// Reader QueryContext returns ctx.Err() to the caller and does not
// disturb a concurrently running Batch.
func TestBatch_Query_CancelDoesNotAffectBatch(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	// Seed rows.
	_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (1, 'pre')")
	require.NoError(t, err)
	require.NoError(t, d.Writer.Flush())

	// Start a slow Batch.
	release := make(chan struct{})
	batchDone := make(chan error, 1)
	go func() {
		batchDone <- d.Batch(func(tx *sql.Tx) error {
			<-release
			_, err := tx.Exec(
				"INSERT INTO users (id, name) VALUES (2, 'in_batch')")
			return err
		})
	}()

	// Reader context cancels immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var name string
	row := d.Reader.QueryRowContext(ctx, "SELECT name FROM users WHERE id = 1")
	require.ErrorIs(t, row.Scan(&name), context.Canceled,
		"cancelled QueryContext must surface ctx.Err() on Scan")

	// Batch is unaffected: release it and confirm it commits.
	close(release)
	require.NoError(t, <-batchDone)

	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, 2, n, "Batch commit independent of cancelled reader query")
}

// TestBatch_Query_MixedReadWriteStress runs a high-fanout mix of
// Reader queries (Query / QueryContext / QueryRow / QueryRowContext),
// Writer.Exec inserts, and parallel Batch closures. Every row written
// must eventually become visible to the Reader pool; no goroutine may
// deadlock.
func TestBatch_Query_MixedReadWriteStress(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          50,
		FlushInterval: 5 * time.Millisecond,
	})
	defer cleanup()

	const (
		batchG  = 6
		execG   = 6
		readerG = 4
		batchN  = 4
		execN   = 5
		readN   = 30
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup

		// Batch goroutines: ID range 1..4.
		for g := 0; g < batchG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := g * batchN
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < batchN; i++ {
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							base+i+1, fmt.Sprintf("b%02d-%d", g, i),
						); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Errorf("batch %d: %v", g, err)
				}
			}(g)
		}

		// Exec goroutines: ID range 100000+.
		for g := 0; g < execG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := 100000 + g*execN
				for i := 0; i < execN; i++ {
					_, err := d.Writer.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						base+i+1, fmt.Sprintf("e%02d-%d", g, i),
					)
					if err != nil {
						t.Errorf("exec %d-%d: %v", g, i, err)
					}
				}
			}(g)
		}

		// Reader goroutines: cycle through every Query* API.
		for r := 0; r < readerG; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				ctx := context.Background()
				var prev int // for monotonicity sanity check
				for i := 0; i < readN; i++ {
					var n int
					var err error
					switch (r + i) % 4 {
					case 0:
						row := d.Reader.QueryRow(
							"SELECT COUNT(*) FROM users")
						err = row.Scan(&n)
					case 1:
						row := d.Reader.QueryRowContext(ctx,
							"SELECT COUNT(*) FROM users")
						err = row.Scan(&n)
					case 2:
						rows, qerr := d.Reader.Query(
							"SELECT COUNT(*) FROM users")
						if qerr != nil {
							err = qerr
							break
						}
						if rows.Next() {
							err = rows.Scan(&n)
						}
						_ = rows.Close()
					case 3:
						rows, qerr := d.Reader.QueryContext(ctx,
							"SELECT COUNT(*) FROM users")
						if qerr != nil {
							err = qerr
							break
						}
						if rows.Next() {
							err = rows.Scan(&n)
						}
						_ = rows.Close()
					}
					if err != nil {
						t.Errorf("reader %d iter %d: %v", r, i, err)
						return
					}
					// Sanity bound — across this goroutine's reads the
					// committed row count may only stay flat or grow.
					if n < prev {
						t.Errorf(
							"reader %d iter %d: count went from %d to %d (non-monotonic)",
							r, i, prev, n)
						return
					}
					prev = n
				}
			}(r)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: mixed Write + Query + Batch did not complete")
	}

	require.NoError(t, d.Writer.Flush())

	want := batchG*batchN + execG*execN
	var n int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n))
	assert.Equal(t, want, n,
		"every Batch + Exec row must be visible to the Reader after the stress run")
}