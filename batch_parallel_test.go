package sqlite

// Parallel insert tests for DB.Batch coordinating with the rest of the
// writer API (Writer.Exec, Writer.ExecContext, Writer.Flush,
// Writer.BeginTx, and Tx.*). The tests focus on whether the txMu RWMutex
// and the single flush goroutine correctly serialize the different write
// paths so no rows are lost, no goroutine deadlocks, and atomicity holds.
//
// Each test guards against hangs with a hard time.After so a deadlock
// surfaces as a clear failure rather than a stuck test run.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch_ParallelInsert_MultipleBatchesSerialize spawns N goroutines
// that each run a Batch closure inserting a unique ID range. Because Batch
// holds txMu.Lock for its entire lifetime, the Batches run one after the
// other. The test verifies every Batch committed (all N*M rows present) and
// that no goroutine hung waiting for the lock.
func TestBatch_ParallelInsert_MultipleBatchesSerialize(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	const goroutines = 20
	const perBatch = 10

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < perBatch; i++ {
						id := g*perBatch + i + 1
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							id, fmt.Sprintf("g%02d-u%d", g, i),
						); err != nil {
							return err
						}
					}
					return nil
				})
				assert.NoError(t, err, "goroutine %d", g)
			}(g)
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("deadlock: %d parallel Batches did not complete", goroutines)
	}

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, goroutines*perBatch, count,
		"every Batch closure's rows must commit (no row lost to serialization)")
}

// TestBatch_ParallelInsert_QueuedExecsAllProceed verifies that Execs
// dispatched while a Batch is holding the lock are correctly serialized
// (not dropped) and all their rows commit after Batch releases.
//
// The Batch closure closes `started` before waiting on `release`, so once
// the test observes `<-started` it is guaranteed that Batch already owns
// txMu.Lock and any later Exec call will block until Batch returns.
func TestBatch_ParallelInsert_QueuedExecsAllProceed(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 10 * time.Second, // large so buffered Execs stay buffered
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

	// Dispatch many Execs while Batch holds txMu.Lock. They must block,
	// not race past the Batch.
	const numExecs = 25
	var execStarted int32
	execDone := make(chan error, numExecs)
	for i := 0; i < numExecs; i++ {
		go func(i int) {
			atomic.AddInt32(&execStarted, 1)
			_, err := d.Writer.Exec(
				"INSERT INTO users (id, name) VALUES (?, ?)",
				i+2, fmt.Sprintf("exec-%d", i),
			)
			execDone <- err
		}(i)
	}

	// Wait until every goroutine has at least entered Exec (so all are
	// contending for txMu.RLock).
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&execStarted) < int32(numExecs) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, int32(numExecs), atomic.LoadInt32(&execStarted),
		"all Exec goroutines must have started before releasing the Batch")

	// While Batch holds txMu.Lock, no Exec should have completed.
	select {
	case err := <-execDone:
		t.Fatalf("Exec returned %v while Batch still held the lock", err)
	default:
	}

	close(release)
	require.NoError(t, <-batchDone)

	// Every queued Exec must now finish successfully.
	for i := 0; i < numExecs; i++ {
		require.NoError(t, <-execDone, "queued Exec %d", i)
	}

	require.NoError(t, d.Writer.Flush(),
		"writer must remain usable after Batch released")

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, numExecs+1, count,
		"Batch row + every queued Exec row must be committed")
}

// TestBatch_ParallelInsert_BatchBlocksConcurrentBeginTx verifies that
// BeginTx (which holds txMu.Lock for the lifetime of the returned Tx)
// waits behind an active Batch. Once Batch releases, the BeginTx proceeds
// and its own writes commit too.
func TestBatch_ParallelInsert_BatchBlocksConcurrentBeginTx(t *testing.T) {
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

	beginDone := make(chan error, 1)
	go func() {
		tx, err := d.Writer.BeginTx(context.Background(), nil)
		if err != nil {
			beginDone <- err
			return
		}
		_, _ = tx.Exec("INSERT INTO users (id, name) VALUES (2, 'in_begin')")
		beginDone <- tx.Commit()
	}()

	// BeginTx must remain blocked while Batch holds txMu.Lock.
	select {
	case err := <-beginDone:
		t.Fatalf("BeginTx returned %v while Batch was active", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-batchDone)
	require.NoError(t, <-beginDone)

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, 2, count, "both Batch row and BeginTx row must commit")
}

// TestBatch_ParallelInsert_MixedWrites runs a mix of Batch, Exec, and
// BeginTx goroutines concurrently, each writing a disjoint ID range, then
// verifies every row landed. This is the core "do they all play nicely
// together?" test.
func TestBatch_ParallelInsert_MixedWrites(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          50,
		FlushInterval: 10 * time.Millisecond,
	})
	defer cleanup()

	const (
		batchGoroutines = 5
		execGoroutines  = 10
		beginGoroutines = 3
		rowsPerBatch    = 5
		rowsPerExec     = 4
		rowsPerBegin    = 3
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup

		// Batch goroutines: ID range [1, batchGoroutines*rowsPerBatch].
		for g := 0; g < batchGoroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := g * rowsPerBatch
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < rowsPerBatch; i++ {
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							base+i+1, fmt.Sprintf("b%02d-%d", g, i),
						); err != nil {
							return err
						}
					}
					return nil
				})
				assert.NoError(t, err, "batch %d", g)
			}(g)
		}

		// Exec goroutines: ID range 100000+i (well above Batch).
		for g := 0; g < execGoroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := 100000 + g*rowsPerExec
				for i := 0; i < rowsPerExec; i++ {
					_, err := d.Writer.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						base+i+1, fmt.Sprintf("e%02d-%d", g, i),
					)
					assert.NoError(t, err, "exec %d-%d", g, i)
				}
			}(g)
		}

		// BeginTx goroutines: ID range 500000+ (well above both
		// Batch and Exec so the three sets never collide).
		for g := 0; g < beginGoroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				tx, err := d.Writer.BeginTx(context.Background(), nil)
				if !assert.NoError(t, err, "begin %d", g) {
					return
				}
				base := 500000 + g*rowsPerBegin
				for i := 0; i < rowsPerBegin; i++ {
					_, _ = tx.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						base+i+1, fmt.Sprintf("x%02d-%d", g, i),
					)
				}
				assert.NoError(t, tx.Commit(), "begin commit %d", g)
			}(g)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: combined Batch + Exec + BeginTx writers did not complete")
	}

	require.NoError(t, d.Writer.Flush())

	want := batchGoroutines*rowsPerBatch +
		execGoroutines*rowsPerExec +
		beginGoroutines*rowsPerBegin
	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, want, count, "no row may be dropped across the three write APIs")
}

// TestBatch_ParallelInsert_RollbackIsolated runs one failing Batch alongside
// several successful Batches. The failing Batch's rows must roll back; the
// others must commit unaffected. This proves Batch atomicity is per-call
// and doesn't leak into concurrent callers.
func TestBatch_ParallelInsert_RollbackIsolated(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	const (
		goodGoroutines = 5
		rowsPerBatch   = 4
		failingIDs     = 4
	)
	sentinel := errors.New("intentional batch failure")

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup

		// Failing Batch.
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := d.Batch(func(tx *sql.Tx) error {
				for i := 0; i < failingIDs; i++ {
					if _, err := tx.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						9000+i+1, "WILL_ROLL_BACK",
					); err != nil {
						return err
					}
				}
				return sentinel
			})
			if !errors.Is(err, sentinel) {
				t.Errorf("failing batch returned %v, want %v", err, sentinel)
			}
		}()

		// Successful Batches.
		for g := 0; g < goodGoroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				base := (g + 1) * 100
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < rowsPerBatch; i++ {
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							base+i+1, fmt.Sprintf("ok%02d-%d", g, i),
						); err != nil {
							return err
						}
					}
					return nil
				})
				assert.NoError(t, err, "good batch %d", g)
			}(g)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("deadlock: rollback-isolation test did not complete")
	}

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, goodGoroutines*rowsPerBatch, count,
		"rolled-back rows must not be visible; successful batches must commit")

	var rolledBack int
	require.NoError(t, d.Reader.QueryRow(
		"SELECT COUNT(*) FROM users WHERE name = 'WILL_ROLL_BACK'",
	).Scan(&rolledBack))
	assert.Equal(t, 0, rolledBack,
		"every row from the failing Batch must be rolled back")
}

// TestBatch_ParallelInsert_BatchSeesPriorBatchCommit confirms that Batch
// calls running concurrently still observe each other's commits in
// order — i.e. the second Batch's closure sees the first Batch's data
// because the txMu.Lock forces them to run sequentially.
func TestBatch_ParallelInsert_BatchSeesPriorBatchCommit(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	var wg sync.WaitGroup
	const N = 10

	// First wave: insert distinct rows. They all run, but the lock
	// guarantees a total order; a subsequent Batch can rely on every
	// earlier Batch's writes being visible.
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			err := d.Batch(func(tx *sql.Tx) error {
				_, err := tx.Exec(
					"INSERT INTO users (id, name) VALUES (?, ?)",
					i+1, fmt.Sprintf("seed-%d", i),
				)
				return err
			})
			assert.NoError(t, err, "seed batch %d", i)
		}(i)
	}
	wg.Wait()

	// Second wave: each Batch reads every prior seed inside its closure.
	// If the lock were missing, a second-wave Batch might run before a
	// first-wave Batch committed and miss rows.
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			err := d.Batch(func(tx *sql.Tx) error {
				var seen int
				if err := tx.QueryRow(
					"SELECT COUNT(*) FROM users WHERE name LIKE 'seed-%'",
				).Scan(&seen); err != nil {
					return err
				}
				if seen != N {
					return fmt.Errorf("batch %d saw %d prior commits, want %d", i, seen, N)
				}
				return nil
			})
			assert.NoError(t, err, "verify batch %d", i)
		}(i)
	}
	wg.Wait()

	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, N, count)
}

// TestBatch_ParallelInsert_ReaderConcurrentWithBatch verifies that the
// Reader pool is not gated by txMu — reads continue to make progress
// while a long-running Batch holds the write lock.
func TestBatch_ParallelInsert_ReaderConcurrentWithBatch(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          100,
		FlushInterval: 100 * time.Millisecond,
	})
	defer cleanup()

	// Seed a row so the Reader has something to find.
	_, err := d.Writer.Exec("INSERT INTO users (id, name) VALUES (1, 'seed')")
	require.NoError(t, err)
	require.NoError(t, d.Writer.Flush())

	started := make(chan struct{})
	release := make(chan struct{})
	batchDone := make(chan error, 1)
	go func() {
		batchDone <- d.Batch(func(tx *sql.Tx) error {
			close(started)
			<-release
			_, err := tx.Exec("INSERT INTO users (id, name) VALUES (2, 'after_batch')")
			return err
		})
	}()

	<-started

	// While Batch holds txMu, run several Reader queries. They must
	// all complete — if Reader were gated on txMu the test would hang.
	const reads = 20
	for i := 0; i < reads; i++ {
		var name string
		require.NoError(t,
			d.Reader.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name),
			"reader blocked while Batch held the write lock (read %d)", i)
		assert.Equal(t, "seed", name)
	}

	close(release)
	require.NoError(t, <-batchDone)

	// After the Batch commits, the new row is visible to readers too.
	var name string
	require.NoError(t, d.Reader.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name))
	assert.Equal(t, "after_batch", name)
}

// TestBatch_ParallelInsert_NoDeadlockUnderMixedLoad is the kitchen-sink
// stress test: many goroutines of every flavor, all running at once. The
// only assertion that matters is "everything finishes and every row
// lands". A deadlock or a lost row will fail this test.
func TestBatch_ParallelInsert_NoDeadlockUnderMixedLoad(t *testing.T) {
	d, cleanup := newTestDBWriter(t, BufferConfig{
		Size:          50,
		FlushInterval: 5 * time.Millisecond,
	})
	defer cleanup()

	const (
		batchG  = 8
		execG   = 8
		beginG  = 4
		readG   = 4
		batchN  = 4
		execN   = 5
		beginN  = 3
		readN   = 10
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for g := 0; g < batchG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				err := d.Batch(func(tx *sql.Tx) error {
					for i := 0; i < batchN; i++ {
						id := g*100 + i + 1
						if _, err := tx.Exec(
							"INSERT INTO users (id, name) VALUES (?, ?)",
							id, fmt.Sprintf("b%02d-%d", g, i),
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

		for g := 0; g < execG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < execN; i++ {
					id := (g+1)*1000 + i + 1
					_, err := d.Writer.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						id, fmt.Sprintf("e%02d-%d", g, i),
					)
					if err != nil {
						t.Errorf("exec %d-%d: %v", g, i, err)
					}
				}
			}(g)
		}

		for g := 0; g < beginG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				tx, err := d.Writer.BeginTx(context.Background(), nil)
				if err != nil {
					t.Errorf("begin %d: %v", g, err)
					return
				}
				for i := 0; i < beginN; i++ {
					id := (g+1)*10000 + i + 1
					_, _ = tx.Exec(
						"INSERT INTO users (id, name) VALUES (?, ?)",
						id, fmt.Sprintf("x%02d-%d", g, i),
					)
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("begin commit %d: %v", g, err)
				}
			}(g)
		}

		for g := 0; g < readG; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < readN; i++ {
					var n int
					if err := d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&n); err != nil {
						t.Errorf("read %d-%d: %v", g, i, err)
					}
				}
			}(g)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: mixed parallel load did not complete in 60s")
	}

	require.NoError(t, d.Writer.Flush())

	want := batchG*batchN + execG*execN + beginG*beginN
	var count int
	require.NoError(t, d.Reader.QueryRow("SELECT COUNT(*) FROM users").Scan(&count))
	assert.Equal(t, want, count,
		"every Batch + Exec + BeginTx row must be present after the stress run")
}