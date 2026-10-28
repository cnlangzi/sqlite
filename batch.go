package sqlite

import (
	"database/sql"
	"fmt"
)

// Batch executes fn within a single atomic SQLite transaction. The
// transaction is opened on the Writer's underlying *sql.DB, so all
// statements inside fn are executed on a dedicated transaction connection
// and either commit together or roll back together.
//
// Batch is mutually exclusive with DB.Exec / DB.ExecContext / DB.BeginTx
// for its entire lifetime: it acquires the Writer's txMu.Lock() on entry
// and releases it on return. Any concurrent Exec, ExecContext, or BeginTx
// call from another goroutine will block until Batch returns.
//
// fn MUST NOT call DB.Exec, DB.ExecContext, DB.BeginTx, DB.Batch, or
// DB.Flush from inside the closure. Batch already holds txMu.Lock() on the
// calling goroutine's stack, so a reentrant attempt would deadlock the
// writer (Go mutexes are not reentrant, and the flush goroutine that is
// running fn is also the only goroutine that drains the task channel).
// Execute statements via the *sql.Tx passed to fn instead.
//
// Semantics:
//
//   - fn returns nil     → transaction is committed; Batch returns nil.
//   - fn returns non-nil → transaction is rolled back; Batch returns the
//     error from fn.
//   - fn panics          → transaction is rolled back; the panic is
//     recovered and converted to an error of the form
//     "sqlite: batch panic: <value>"; Batch does not propagate the panic.
//
// The user must not call Commit or Rollback on the *sql.Tx passed to fn;
// Batch owns the transaction lifecycle. Calling Commit or Rollback from
// inside fn will cause Batch's own commit/rollback step to return
// sql.ErrTxDone.
//
// No per-statement sql.Result is returned. If per-statement results are
// required, use DB.BeginTx instead.
//
// Batch uses exactly one channel send into the Writer's task queue
// regardless of how many statements fn executes, making it significantly
// cheaper than the equivalent BeginTx + N*Exec loop for large bulk writes.
func (db *DB) Batch(fn func(*sql.Tx) error) error {
	return db.Writer.Batch(fn)
}

// Batch is the underlying implementation invoked by (*DB).Batch. See
// (*DB).Batch for the full contract; calling it directly on the Writer is
// equivalent to going through (*DB).Batch.
func (bw *BufferWriter) Batch(fn func(*sql.Tx) error) error {
	bw.txMu.Lock()
	defer bw.txMu.Unlock()

	task := &Task{
		action: ActionBatch,
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			if err := bw.commitWithTrigger(triggerManual); err != nil {
				return TaskResult{Error: fmt.Errorf("sqlite: flush before batch: %w", err)}
			}

			// Defensive cleanup: commitWithTrigger above returns early
			// when bw.buffer < 1, leaving bw.tx set if it was already
			// non-nil. In that state bw.tx holds an *sql.Tx whose
			// underlying connection is still checked out of bw.DB. The
			// writer pool is configured with SetMaxOpenConns(1) by
			// openFile / openMemory, so the BeginTx below would block
			// forever waiting for a free connection. Roll back any
			// stranded tx before opening the batch tx.
			if bw.tx != nil {
				_ = bw.tx.Rollback()
				bw.tx = nil
			}

			tx, err := bw.DB.Begin()
			if err != nil {
				return TaskResult{Error: err}
			}

			fnErr := safeBatchCall(fn, tx)
			if fnErr != nil {
				_ = tx.Rollback()
				return TaskResult{Error: fnErr}
			}
			if err := tx.Commit(); err != nil {
				return TaskResult{Error: err}
			}
			return TaskResult{}
		},
	}

	ch, err := bw.do(task)
	if err != nil {
		return err
	}
	return bw.await(ch).Error
}

// safeBatchCall invokes fn with the given transaction, recovering from any
// panic and converting it to a non-nil error so Batch never propagates a
// panic out of its own call site.
func safeBatchCall(fn func(*sql.Tx) error, tx *sql.Tx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sqlite: batch panic: %v", r)
		}
	}()
	return fn(tx)
}