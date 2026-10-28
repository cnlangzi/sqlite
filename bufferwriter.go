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

// ErrQueueFull is returned when the writer's task queue is full and the
// submitted task is rejected without being executed.
//
// This is transient back-pressure, not a terminal condition: the flush
// goroutine is behind, for example because it is blocked on a slow statement
// or on the SQLite write lock. The rejected task did not run, so retrying it
// is safe.
//
// The retry only pays off while the writer is alive: a writer that is
// shutting down reports ErrClosed instead, which is terminal.
//
// ErrClosed and ErrQueueFull describe different conditions — a writer that
// is closed reports ErrClosed, a task rejected because the queue was full
// reports ErrQueueFull — and are never returned for the same situation.
var ErrQueueFull = errors.New("sqlite: writer task queue is full")

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
//
// The task queue is sized from cfg.QueueCapacity; see BufferConfig.
func NewWriter(db *sql.DB, cfg BufferConfig) *BufferWriter {
	cfg.Validate()
	w := &BufferWriter{
		DB:     db,
		cfg:    cfg,
		tasks:  make(chan TaskFunc, cfg.QueueCapacity),
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
		// Nothing is left to commit on exit: the close path below already
		// committed the remaining buffer, and a commit that failed discarded
		// the transaction it belonged to.
		timer.Stop()

		// closed is also set when the close signal is received, before the
		// queue is drained, so that new submissions are rejected for the
		// whole shutdown sequence. Storing it again here is idempotent and
		// covers any other exit path.
		bw.closed.Store(true)
		close(bw.done)
	}()

	for {
		select {
		case <-bw.close:
			slog.Info("close signal received")
			// Mark the writer closed before draining: from here on do()
			// rejects new submissions with ErrClosed instead of accepting
			// them into a queue that nobody will drain any more.
			bw.closed.Store(true)
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
			// A failed commit is logged by commitWithTrigger itself.
			remaining := bw.buffer
			if err := bw.commitWithTrigger(triggerClose); err == nil {
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
				// A failed commit is logged by commitWithTrigger itself.
				_ = bw.commitWithTrigger(trigger)

			}

			task.Notify() <- result

		case <-timer.C:
			if bw.buffer > 0 && !bw.isPending && time.Since(bw.lastCommit) >= bw.cfg.FlushInterval {
				slog.Debug("commit triggered", slog.String("trigger", string(triggerInterval)), slog.Int("buffer", bw.buffer))
				// A failed commit is logged by commitWithTrigger itself.
				_ = bw.commitWithTrigger(triggerInterval)
			}

		}
	}
}

func (bw *BufferWriter) Flush() error {
	bw.txMu.RLock()
	defer bw.txMu.RUnlock()
	// bw.buffer is owned by the flush goroutine, which increments and resets
	// it. Reading it here would race with the goroutine's commit, so the
	// count is only logged there (see the "committed" entry).
	slog.Debug("flush requested")

	task, err := bw.do(Commit())

	if err != nil {
		return err
	}

	result := bw.await(task)

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
		// The transaction is finished either way: database/sql releases the
		// connection when Commit fails, and SQLite has already rolled the
		// transaction back. Drop it, together with the buffered statement
		// count, so the next statement opens a fresh transaction instead of
		// failing with ErrTxDone for the rest of the writer's life.
		bw.discardTx()

		slog.Error("commit failed",
			slog.String("trigger", string(trigger)),
			slog.Int("buffer", count),
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

// discardTx drops the writer's current transaction. It is used after a failed
// commit, where the transaction can no longer be used: the buffered statements
// it held are lost with it.
//
// It must only be called from the flush goroutine, which owns bw.tx,
// bw.buffer and bw.isPending.
func (bw *BufferWriter) discardTx() {
	if bw.tx != nil {
		// No-op when the transaction is already finished, which is the case
		// when the commit itself failed.
		_ = bw.tx.Rollback()
		bw.tx = nil
	}
	bw.buffer = 0
	// A transaction that failed to commit is over: leaving the flag set would
	// keep the size-triggered commit disabled.
	bw.isPending = false
}

// do submits a task to the flush goroutine and returns a channel that
// receives the result.
//
// It returns ErrClosed if the writer is closed, and ErrQueueFull if the task
// queue is full. The latter is transient back-pressure: the task was not
// executed and may be retried once the queue drains.
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
		// The task queue is full: the flush goroutine is behind, e.g. blocked
		// on a slow statement or on the SQLite write lock. This is
		// back-pressure, not a closed writer — report it as such so the
		// caller can retry instead of treating the write as lost for good.
		//
		// len/cap on a channel are safe to read while the flush goroutine
		// sends and receives on it, so the occupancy can be reported without
		// a data race. bw.buffer, in contrast, is owned by the flush
		// goroutine; see Flush().
		slog.Warn("task blocked",
			slog.Int("queued", len(bw.tasks)),
			slog.Int("capacity", cap(bw.tasks)))
		return nil, ErrQueueFull
	}
}

// await waits for the result of a submitted task.
//
// A task can be submitted to a queue that the flush goroutine never picks
// up: the writer may be closed while the task is in flight, and the flush
// goroutine drains the queue and exits before taking it. Such a task is
// never executed and never notified, so a plain receive on the result
// channel would block forever. await therefore also waits for the writer to
// shut down and reports ErrClosed in that case.
//
// A task that did run has its result delivered before the flush goroutine
// exits, so the result channel is checked once more before reporting
// failure: the caller must not be told a statement failed when it actually
// succeeded.
func (bw *BufferWriter) await(ch chan TaskResult) TaskResult {
	select {
	case result := <-ch:
		return result
	case <-bw.done:
		return bw.resultOrClosed(ch)
	}
}

// awaitCtx is await with context cancellation support. A cancelled context
// stops the wait.
func (bw *BufferWriter) awaitCtx(ch chan TaskResult, ctx context.Context) TaskResult {
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		return TaskResult{Error: ctx.Err()}
	case <-bw.done:
		return bw.resultOrClosed(ch)
	}
}

// resultOrClosed returns the task's result if it was delivered, and
// ErrClosed otherwise: the writer shut down before the task ran.
func (bw *BufferWriter) resultOrClosed(ch chan TaskResult) TaskResult {
	select {
	case result := <-ch:
		return result
	default:
		return TaskResult{Error: ErrClosed}
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

	result := bw.awaitCtx(ch, ctx)
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Result.(sql.Result), nil
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
	result := bw.await(ch)
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
//
// It returns ErrQueueFull if the task queue is full (transient back-pressure:
// the transaction was not started and the call may be retried) and ErrClosed
// if the writer is closed.
func (bw *BufferWriter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	bw.txMu.Lock()
	// Submit the begin task to the flush goroutine. The submission is not a
	// blocking send: if the task queue is full the begin is rejected with
	// ErrQueueFull and the caller may retry; if the writer is closed it
	// fails with ErrClosed.
	task, err := bw.do(BeginTx())
	if err != nil {
		bw.txMu.Unlock()
		return nil, err
	}

	result := bw.await(task)
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
