package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("sqlite: writer is closed")

// Writer provides transparent batch writing with channel-based coordination.
// All writes are serialized through a single goroutine via cmdCh.
type Writer struct {
	*sql.DB
	cfg BufferConfig

	tasks  chan TaskFunc
	close  chan struct{}
	done   chan struct{} // closed when flush goroutine exits; broadcast to all do() callers
	once   sync.Once
	closed atomic.Bool // set to true before done is closed; fast-path guard in do()

	tx *sql.Tx

	buffer     int
	lastCommit time.Time
}

// NewWriter creates a Writer wrapping the provided *sql.DB.
// The returned Writer starts a background flush goroutine immediately.
// Close must be called to shut it down cleanly.
func NewWriter(db *sql.DB, cfg BufferConfig) *Writer {
	cfg.Validate()
	w := &Writer{
		DB:     db,
		cfg:    cfg,
		tasks:  make(chan TaskFunc, 100),
		close:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		buffer: 0,
	}
	go w.flush()

	return w
}

// flush is the writer's background goroutine. It receives tasks from the
// tasks channel, accumulates them in a transaction, and commits based on
// buffer size or elapsed time. It exits when close is signaled, committing
// any remaining buffered work before terminating.
func (w *Writer) flush() {
	timer := time.NewTicker(w.cfg.FlushInterval)
	defer func() {
		if w.buffer > 0 && w.tx != nil {
			err := w.Commit()
			if err != nil {
				slog.Error("sqlite: commit", slog.String("err", err.Error()))
			}
		}

		timer.Stop()

		w.closed.Store(true)
		close(w.done)
	}()

	for {
		select {
		case <-w.close:
			// Drain buffered tasks so their callers are not leaked.
			for {
				select {
				case t := <-w.tasks:
					t.Notify() <- TaskResult{Error: ErrClosed}
				default:
					goto drained
				}
			}
		drained:
			// Commit whatever was already buffered before the close signal.
			if err := w.Commit(); err != nil {
				slog.Error("sqlite: commit on close", slog.String("err", err.Error()))
			}
			return // defer handles close(w.done)
		case task := <-w.tasks:

			if w.tx == nil {
				var err error
				w.tx, err = w.Begin()
				if err != nil {
					task.Notify() <- TaskResult{Error: err}
					continue
				}
			}
			result := task.Exec(w.tx)
			w.buffer++
			// Check size threshold

			if task.Flush() || w.buffer >= w.cfg.Size {
				if err := w.Commit(); err != nil {
					slog.Error("sqlite: commit", slog.String("err", err.Error()))
				}
			}

			task.Notify() <- result

		case <-timer.C:
			if w.buffer > 0 && time.Since(w.lastCommit) >= w.cfg.FlushInterval {
				if err := w.Commit(); err != nil {
					slog.Error("sqlite: commit", slog.String("err", err.Error()))
				}
			}

		}
	}
}

// Commit flushes any pending buffered statements by committing the current
// transaction. If there is no pending work, it is a no-op. After a successful
// commit, a new lazy transaction is started for subsequent writes.
func (w *Writer) Commit() error {
	if w.tx == nil || w.buffer < 1 {
		return nil
	}

	err := w.tx.Commit()
	if err == nil {
		w.buffer = 0
		w.tx = nil // next tx is created lazily on the next incoming task
		w.lastCommit = time.Now()
	}

	return err
}

// do submits a task to the flush goroutine and returns a channel that
// receives the result. If the writer is already closed, it returns ErrClosed.
func (w *Writer) do(task TaskFunc) (chan TaskResult, error) {
	if w.closed.Load() {
		return nil, ErrClosed
	}
	select {
	case w.tasks <- task:
		return task.Notify(), nil
	case <-w.done:
		return nil, ErrClosed
	}
}

// ExecContext executes a write statement (INSERT, UPDATE, DELETE, etc.)
// asynchronously. The statement is buffered and flushed according to the
// BufferConfig settings. If the context is cancelled before the statement
// is processed, ctx.Err() is returned.
func (w *Writer) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ch, err := w.do(Exec(query, args...))
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Result.(sql.Result), nil
	}
}

// Exec executes a write statement (INSERT, UPDATE, DELETE, etc.)
// asynchronously. The statement is buffered and flushed according to the
// BufferConfig settings. This is the non-context variant; use ExecContext
// if you need cancellation or deadline control.
func (w *Writer) Exec(query string, args ...any) (sql.Result, error) {
	ch, err := w.do(Exec(query, args...))
	if err != nil {
		return nil, err
	}
	result := <-ch
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Result.(sql.Result), nil
}

// BeginTx starts a new buffered transaction. The returned Tx does not
// acquire a database lock immediately; statements are buffered locally and
// only committed when Tx.Commit is called. The opts parameter is accepted
// for compatibility with database/sql but is not used (savepoints handle
// the transactional semantics internally).
func (w *Writer) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return &Tx{w: w}, nil
}

// Close signals the flush goroutine to stop, waits for it to drain and
// commit any remaining buffered work, then marks the writer as closed.
// It is safe to call Close multiple times concurrently; subsequent calls
// return nil immediately.
func (w *Writer) Close() error {
	w.once.Do(func() {
		w.close <- struct{}{}
		<-w.done // wait for flush to commit and exit
	})
	return nil
}
