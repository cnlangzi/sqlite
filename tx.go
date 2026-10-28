package sqlite

import (
	"database/sql"
	"errors"
	"log/slog"
	"sync"
)

// Tx is an exclusive transaction that holds the Writer's write lock for its
// entire lifetime. All Exec calls are executed immediately through the
// Writer's background goroutine within the shared transaction. No other
// writes via Writer.Exec or Writer.ExecContext can proceed while a Tx is
// active. The lock is released when Commit or Rollback is called, on every
// path — including a failed commit.
type Tx struct {
	w     *BufferWriter
	done  bool // true after Commit or Rollback has been called
	count int  // number of successfully executed Exec calls

	// releaseOnce releases the writer's exclusive lock exactly once.
	// The transaction can be finished by Commit or by Rollback, and only
	// one of them may release the lock. The Once makes that an invariant of
	// the release itself rather than of the done flag, which is not
	// synchronized: a concurrent Commit and Rollback would otherwise both
	// reach the unlock and panic.
	releaseOnce sync.Once
}

// release releases the writer's exclusive lock. It is safe to call more than
// once: only the first call unlocks.
func (btx *Tx) release() {
	btx.releaseOnce.Do(func() { btx.w.txMu.Unlock() })
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
	result := btx.w.await(ch)
	if result.Error != nil {
		return nil, result.Error
	}
	btx.count++
	return result.Result.(sql.Result), nil
}

// Commit flushes the exclusive transaction and releases the write lock.
// After Commit returns, the Tx cannot be reused.
//
// The write lock is released on every path: when the commit succeeds, when
// the commit task cannot be submitted, and when the commit itself fails. A
// failed commit returns the underlying error unchanged and leaves the writer
// usable — the next statement runs in a fresh transaction.
//
// When the commit task cannot be submitted (ErrClosed, or ErrQueueFull
// because the writer is saturated), the commit never ran: the transaction
// stays open in the writer and its statements are committed by the next
// Flush, BeginTx or Close. The size- and interval-triggered auto-commits
// stay disabled until then, because the writer still holds that
// transaction.
func (btx *Tx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}
	btx.done = true
	// Release the lock on every exit path, so a failed commit cannot leave
	// the caller — or every other writer — blocked on it forever.
	defer btx.release()

	ch, err := btx.w.do(Commit())
	if err != nil {
		return err
	}
	result := btx.w.await(ch)
	if result.Error != nil {
		slog.Debug("tx commit failed", slog.String("err", result.Error.Error()))
		return result.Error
	}

	slog.Debug("tx committed", slog.Int("count", btx.count))

	return nil
}

// Rollback rolls back the exclusive transaction and releases the write lock.
// If the Tx has already been committed or rolled back, Rollback is a no-op.
//
// The lock is released exactly once, no matter how the transaction was
// finished: a Rollback after a failed Commit must not release it again.
func (btx *Tx) Rollback() error {
	if btx.done {
		return nil
	}

	ch, err := btx.w.do(Rollback())
	btx.done = true
	btx.release()
	if err != nil {
		return err
	}
	result := btx.w.await(ch)

	slog.Debug("tx rolled back", slog.Int("count", btx.count))
	return result.Error
}
