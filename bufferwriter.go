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

// commitTrigger indicates what caused a commit.
type commitTrigger string

const (
	triggerSize     commitTrigger = "size"
	triggerInterval commitTrigger = "interval"
	triggerManual   commitTrigger = "manual"
	triggerClose    commitTrigger = "close"
)

// BufferWriter provides transparent batch writing with channel-based coordination.
// All writes are serialized through a single goroutine via cmdCh.
type BufferWriter struct {
	*sql.DB
	cfg BufferConfig

	tasks  chan TaskFunc
	close  chan struct{}
	done   chan struct{} // closed when flush goroutine exits; broadcast to all do() callers
	once   sync.Once
	closed atomic.Bool // set to true before done is closed; fast-path guard in do()

	txMu      sync.RWMutex
	isPending bool

	tx *sql.Tx

	buffer     int
	lastCommit time.Time
}

// NewWriter creates a Writer wrapping the provided *sql.DB.
// The returned Writer starts a background flush goroutine immediately.
// Close must be called to shut it down cleanly.
func NewWriter(db *sql.DB, cfg BufferConfig) *BufferWriter {
	cfg.Validate()
	w := &BufferWriter{
		DB:     db,
		cfg:    cfg,
		tasks:  make(chan TaskFunc, 100),
		close:  make(chan struct{}, 1),
		done:   make(chan struct{}),
		buffer: 0,
	}
	go w.waitSingal()

	return w
}

// waitSingal is the writer's background goroutine. It receives tasks from the
// tasks channel, accumulates them in a transaction, and commits based on
// buffer size or elapsed time. It exits when close is signaled, committing
// any remaining buffered work before terminating.
func (bw *BufferWriter) waitSingal() {
	timer := time.NewTicker(bw.cfg.FlushInterval)
	defer func() {
		if bw.buffer > 0 && bw.tx != nil {
			err := bw.commitWithTrigger(triggerClose)
			if err != nil {
				slog.Error("commit failed",
					slog.String("trigger", string(triggerClose)),
					slog.Int("buffer", bw.buffer),
					slog.String("err", err.Error()))
			}
		}

		timer.Stop()

		bw.closed.Store(true)
		close(bw.done)
	}()

	for {
		select {
		case <-bw.close:
			slog.Info("close signal received")
			// Drain buffered tasks so their callers are not leaked.
			for {
				select {
				case t := <-bw.tasks:
					t.Notify() <- TaskResult{Error: ErrClosed}
				default:
					goto drained
				}
			}
		drained:
			// Commit whatever was already buffered before the close signal.
			remaining := bw.buffer
			if err := bw.commitWithTrigger(triggerClose); err != nil {
				slog.Error("commit failed",
					slog.String("trigger", string(triggerClose)),
					slog.Int("buffer", remaining),
					slog.String("err", err.Error()))
			} else {
				slog.Info("drained on close", slog.Int("remaining_buffer", remaining))
			}
			return // defer handles close(w.done)
		case task := <-bw.tasks:
			slog.Debug("task received", slog.Int("buffer", bw.buffer))

			// Only lazily open a transaction for statements that need one.
			// ActionBegin opens its own tx internally; opening one here would
			// create a leaked tx (and its awaitDone goroutine) that is never closed.
			if bw.tx == nil && (task.Action() == ActionExec || task.Action() == ActionQuery) {
				var err error
				bw.tx, err = bw.DB.Begin()
				if err != nil {
					task.Notify() <- TaskResult{Error: err}
					continue
				}
				slog.Debug("tx started", slog.Int("buffer", bw.buffer))
			}

			result := task.Exec(bw)
			if result.Buffered {
				bw.buffer++
			}

			slog.Debug("buffered", slog.Int("buffer", bw.buffer), slog.Int("size_limit", bw.cfg.Size))

			if !bw.isPending && bw.buffer >= bw.cfg.Size {
				trigger := triggerSize

				slog.Debug("commit triggered", slog.String("trigger", string(trigger)), slog.Int("buffer", bw.buffer))
				if err := bw.commitWithTrigger(trigger); err != nil {
					slog.Error("commit failed",
						slog.String("trigger", string(trigger)),
						slog.Int("buffer", bw.buffer),
						slog.String("err", err.Error()))
				}

			}

			task.Notify() <- result

		case <-timer.C:
			if bw.buffer > 0 && !bw.isPending && time.Since(bw.lastCommit) >= bw.cfg.FlushInterval {
				slog.Debug("commit triggered", slog.String("trigger", string(triggerInterval)), slog.Int("buffer", bw.buffer))
				if err := bw.commitWithTrigger(triggerInterval); err != nil {
					slog.Error("commit failed",
						slog.String("trigger", string(triggerInterval)),
						slog.Int("buffer", bw.buffer),
						slog.String("err", err.Error()))
				}
			}

		}
	}
}

func (bw *BufferWriter) Flush() error {
	bw.txMu.RLock()
	defer bw.txMu.RUnlock()
	slog.Debug("flush requested", slog.Int("buffer", bw.buffer))

	task, err := bw.do(Commit())

	if err != nil {
		return err
	}

	result := <-task

	return result.Error
}

// commitWithTrigger commits the current transaction and logs with the given trigger.
func (bw *BufferWriter) commitWithTrigger(trigger commitTrigger) error {
	if bw.tx == nil || bw.buffer < 1 {
		return nil
	}

	start := time.Now()
	err := bw.tx.Commit()
	elapsed := time.Since(start)
	count := bw.buffer

	if err != nil {
		slog.Error("commit failed",
			slog.String("trigger", string(trigger)),
			slog.Int("buffer", bw.buffer),
			slog.String("err", err.Error()))

		return err
	}

	bw.buffer = 0
	bw.tx = nil // next tx is created lazily on the next incoming task
	bw.lastCommit = time.Now()
	slog.Info("committed",
		slog.String("trigger", string(trigger)),
		slog.Int("count", count),
		slog.Duration("elapsed", elapsed))

	return nil
}

// do submits a task to the flush goroutine and returns a channel that
// receives the result. If the writer is already closed, it returns ErrClosed.
func (bw *BufferWriter) do(task TaskFunc) (chan TaskResult, error) {
	if bw.closed.Load() {
		return nil, ErrClosed
	}
	select {
	case bw.tasks <- task:
		return task.Notify(), nil
	case <-bw.done:
		return nil, ErrClosed
	default:
		slog.Warn("task blocked", slog.Int("buffer", bw.buffer))
		return nil, ErrClosed
	}
}

// ExecContext executes a write statement (INSERT, UPDATE, DELETE, etc.)
// asynchronously. The statement is buffered and flushed according to the
// BufferConfig settings. If the context is cancelled before the statement
// is processed, ctx.Err() is returned.
func (bw *BufferWriter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	bw.txMu.RLock()
	defer bw.txMu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ch, err := bw.do(Exec(query, args...))
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
func (bw *BufferWriter) Exec(query string, args ...any) (sql.Result, error) {
	bw.txMu.RLock()
	defer bw.txMu.RUnlock()

	ch, err := bw.do(Exec(query, args...))
	if err != nil {
		return nil, err
	}
	result := <-ch
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Result.(sql.Result), nil
}

func (bw *BufferWriter) Begin() (*Tx, error) {
	return bw.BeginTx(context.Background(), nil)
}

// BeginTx acquires an exclusive write lock, flushes any pending buffered
// writes, and returns a Tx that executes statements immediately through the
// Writer's background goroutine. No other writes via Exec/ExecContext can
// proceed until Tx.Commit or Tx.Rollback is called.
func (bw *BufferWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	bw.txMu.Lock()
	// Flush pending writes with a blocking send so we don't drop it if the
	// task queue happens to be full at this moment.
	task, err := bw.do(BeginTx())
	if err != nil {
		bw.txMu.Unlock()
		return nil, err
	}

	result := <-task
	if result.Error != nil {
		bw.txMu.Unlock()
		return nil, result.Error
	}

	return &Tx{w: bw}, nil
}

// Close signals the flush goroutine to stop, waits for it to drain and
// commit any remaining buffered work, then marks the writer as closed.
// It is safe to call Close multiple times concurrently; subsequent calls
// return nil immediately.
func (bw *BufferWriter) Close() error {
	bw.once.Do(func() {
		bw.close <- struct{}{}
		<-bw.done // wait for flush to commit and exit
	})
	return nil
}
