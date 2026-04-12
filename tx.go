package sqlite

import (
	"database/sql"
	"errors"
	"log/slog"
)

// Tx is a buffered transaction that batches multiple write statements
// and executes them atomically on Commit. Unlike a traditional database
// transaction, Tx does not hold a lock on the database; statements are
// buffered locally and submitted to the Writer's shared transaction on Commit.
type Tx struct {
	w *BufferWriter

	buf  []TaskArgs // buffered statements to execute on commit
	done bool       // true after Commit or Rollback has been called
}

// Exec buffers a write statement for later execution. The statement is NOT
// executed immediately; it is held in memory until Commit is called, at which
// point all buffered statements are executed atomically within a single
// SQLite transaction. Returns (nil, nil) because execution is deferred.
func (btx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	btx.buf = append(btx.buf, TaskArgs{query: query, args: args})
	return nil, nil
}

// Commit executes all buffered statements atomically in the Writer's global
// transaction. All statements are committed together or rolled back together
// on error. After Commit is called, the Tx can no longer be used.
func (btx *Tx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}

	btx.done = true
	task, err := btx.w.do(Commit(btx.buf...))

	if err != nil {
		return err
	}

	result := <-task
	if result.Error == nil {
		slog.Debug("tx committed", slog.Int("count", len(btx.buf)))
	} else {
		slog.Debug("tx rolled back", slog.Int("count", len(btx.buf)))
	}

	return result.Error

}

// Rollback discards all buffered statements without executing them.
// After Rollback is called, the Tx can no longer be used.
// If the Tx has already been committed, Rollback is a no-op and returns nil.
func (btx *Tx) Rollback() error {

	if btx.done {
		return nil
	}
	btx.done = true
	count := len(btx.buf)
	btx.buf = nil

	slog.Debug("tx rolled back", slog.Int("count", count))

	return nil
}
