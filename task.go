package sqlite

import (
	"database/sql"
	"log/slog"
)

// TaskFunc is the interface implemented by tasks submitted to the Writer.
// It represents a unit of work to be executed within a SQLite transaction.
type TaskFunc interface {
	// Exec executes the task using the provided transaction. It returns
	// the result of the execution (e.g., sql.Result for writes) or an error.
	Exec(w *sql.Tx) TaskResult

	// Notify returns a channel that receives the TaskResult once Exec completes.
	// The caller uses this to wait for the task to finish.
	Notify() chan TaskResult

	// Flush returns true if this task should trigger an immediate commit.
	// Used by Commit task to force a flush after buffering completes.
	Flush() bool
}

// TaskResult holds the outcome of a task execution.
type TaskResult struct {
	Result any   // the execution result, typically a sql.Result or *sql.Row
	Error  error // any error that occurred during execution
}

// TaskArgs bundles a query string with its parameters for deferred execution.
type TaskArgs struct {
	query string
	args  []any
}

// Task is a concrete implementation of TaskFunc that wraps a query and
// its execution function. It is created by the package-level helper functions
// Query, QueryRow, Exec, and Commit.
type Task struct {
	notify chan TaskResult
	exec   func(tx *sql.Tx) TaskResult
	flush  bool
}

// Notify implements TaskFunc by returning the result channel.
func (t *Task) Notify() chan TaskResult {
	return t.notify
}

// Exec implements TaskFunc by running the stored execution function.
func (t *Task) Exec(tx *sql.Tx) TaskResult {
	return t.exec(tx)
}

// Flush implements TaskFunc by returning the stored flush flag.
func (t *Task) Flush() bool {
	return t.flush
}

// Query creates a read-only TaskFunc that executes a query with the given
// arguments and returns a *sql.Row. The query is not executed until the
// Writer's flush goroutine processes the task.
func Query(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			row := tx.QueryRow(query, args...)
			return TaskResult{Result: row, Error: nil}
		},
	}
}

// QueryRow creates a TaskFunc that executes a query returning at most
// one row. It is equivalent to Query but provided for symmetry with
// the database/sql API.
func QueryRow(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			row := tx.QueryRow(query, args...)
			return TaskResult{Result: row, Error: nil}
		},
	}
}

// Exec creates a TaskFunc that executes a write statement (INSERT, UPDATE,
// DELETE, etc.) with the given arguments. The statement is not executed
// until the Writer's flush goroutine processes the task.
func Exec(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			res, err := tx.Exec(query, args...)
			return TaskResult{Result: res, Error: err}
		},
	}
}

// Commit creates a TaskFunc that commits the current transaction.
// It also sets Flush() to true, ensuring any pending buffered writes
// are flushed before this task is processed.
func Commit(tasks []TaskArgs) TaskFunc {
	return &Task{
		flush:  true,
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			sp, err := NewSavepoint(tx)
			if err != nil {
				slog.Error("sqlite: create savepoint", slog.String("err", err.Error()))
				return TaskResult{Result: nil, Error: err}
			}

			defer sp.Release()

			for _, stmt := range tasks {
				if _, err := tx.Exec(stmt.query, stmt.args...); err != nil {
					sp.Rollback()
					return TaskResult{Result: nil, Error: err}
				}
			}

			return TaskResult{Result: nil, Error: err}
		},
	}
}
