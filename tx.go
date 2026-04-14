package sqlite

import (
	"database/sql"
	"errors"
	"log/slog"
)

// Tx is an exclusive transaction that holds the Writer's write lock for its
// entire lifetime. All Exec calls are executed immediately through the
// Writer's background goroutine within the shared transaction. No other
// writes via Writer.Exec or Writer.ExecContext can proceed while a Tx is
// active. The lock is released when Commit or Rollback is called.
type Tx struct {
	w     *BufferWriter
	done  bool // true after Commit or Rollback has been called
	count int  // number of successfully executed Exec calls
}

// Exec immediately sends a write statement to the Writer's background
// goroutine for execution within the active exclusive transaction. Returns
// the real sql.Result (RowsAffected, LastInsertId) unlike the previous
// buffered implementation.
func (btx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	ch, err := btx.w.do(Exec(query, args...))
	if err != nil {
		return nil, err
	}
	result := <-ch
	if result.Error != nil {
		return nil, result.Error
	}
	btx.count++
	return result.Result.(sql.Result), nil
}

// Commit flushes the exclusive transaction and releases the write lock.
// After Commit returns, the Tx cannot be reused.
func (btx *Tx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}
	btx.done = true

	ch, err := btx.w.do(Commit())
	if err != nil {
		return err
	}
	result := <-ch
	if result.Error != nil {
		slog.Debug("tx commit failed", slog.String("err", result.Error.Error()))
		return result.Error
	}

	slog.Debug("tx committed", slog.Int("count", btx.count))
	btx.done = true

	btx.w.txMu.Unlock()

	return nil
}

// Rollback rolls back the exclusive transaction and releases the write lock.
// If the Tx has already been committed or rolled back, Rollback is a no-op.
func (btx *Tx) Rollback() error {
	if btx.done {
		return nil
	}

	ch, err := btx.w.do(Rollback())
	btx.done = true
	btx.w.txMu.Unlock()
	if err != nil {
		return err
	}
	result := <-ch

	slog.Debug("tx rolled back", slog.Int("count", btx.count))
	return result.Error
}
