package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// Batch executes fn within a single atomic SQLite transaction. The transaction
// is opened on the Writer's underlying *sql.DB, so all statements inside fn
// are executed on a dedicated transaction connection and either commit
// together or roll back together.
//
// Batch is mutually exclusive with DB.Exec / DB.ExecContext / DB.BeginTx for
// its entire lifetime: it acquires the Writer's txMu.Lock() on entry and
// releases it on return. Any concurrent Exec, ExecContext, or BeginTx call
// will block until Batch returns.
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
	bw := db.Writer
	bw.txMu.Lock()
	defer bw.txMu.Unlock()

	task := &Task{
		action: ActionBatch,
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			if err := bw.commitWithTrigger(triggerManual); err != nil {
				return TaskResult{Error: fmt.Errorf("sqlite: flush before batch: %w", err)}
			}

			tx, err := bw.DB.BeginTx(context.Background(), nil)
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
	return (<-ch).Error
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