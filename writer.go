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

func (w *Writer) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return &Tx{w: w}, nil
}

// Close signals the flush goroutine to stop, drains pending tasks, and waits
// for it to finish. Safe to call multiple times concurrently.
func (w *Writer) Close() error {
	w.once.Do(func() {
		w.close <- struct{}{}
		<-w.done // wait for flush to commit and exit
	})
	return nil
}
