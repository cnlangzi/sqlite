package sqlite

type Action int

const (
	ActionQuery Action = iota
	ActionExec
	ActionBegin
	ActionCommit
	ActionRollback
)

// TaskFunc is the interface implemented by tasks submitted to the Writer.
// It represents a unit of work to be executed within a SQLite transaction.
type TaskFunc interface {
	// Exec executes the task using the provided transaction. It returns
	// the result of the execution (e.g., sql.Result for writes) or an error.
	Exec(bw *BufferWriter) TaskResult

	// Notify returns a channel that receives the TaskResult once Exec completes.
	// The caller uses this to wait for the task to finish.
	Notify() chan TaskResult

	Action() Action
}

// TaskResult holds the outcome of a task execution.
type TaskResult struct {
	Result   any   // the execution result, typically a sql.Result or *sql.Row
	Buffered bool  // true if the task was buffered and not executed immediately
	Error    error // any error that occurred during execution
}

// Task is a concrete implementation of TaskFunc that wraps a query and
// its execution function. It is created by the package-level helper functions
// Query, QueryRow, Exec, and Commit.
type Task struct {
	notify chan TaskResult
	exec   func(bw *BufferWriter) TaskResult
	action Action
}

// Notify implements TaskFunc by returning the result channel.
func (t *Task) Notify() chan TaskResult {
	return t.notify
}

// Exec implements TaskFunc by running the stored execution function.
func (t *Task) Exec(bw *BufferWriter) TaskResult {
	return t.exec(bw)
}

func (t *Task) Action() Action {
	return t.action
}

// Query creates a read-only TaskFunc that executes a query with the given
// arguments and returns a *sql.Row. The query is not executed until the
// Writer's flush goroutine processes the task.
func Query(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			row := bw.tx.QueryRow(query, args...)
			return TaskResult{Result: row, Error: nil}
		},
		action: ActionExec,
	}
}

// QueryRow creates a TaskFunc that executes a query returning at most
// one row. It is equivalent to Query but provided for symmetry with
// the database/sql API.
func QueryRow(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			row := bw.tx.QueryRow(query, args...)
			return TaskResult{Result: row, Error: nil}
		},
		action: ActionQuery,
	}
}

// Exec creates a TaskFunc that executes a write statement (INSERT, UPDATE,
// DELETE, etc.) with the given arguments. The statement is not executed
// until the Writer's flush goroutine processes the task.
func Exec(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			res, err := bw.tx.Exec(query, args...)
			if err != nil {
				return TaskResult{Result: nil, Error: err}
			}

			return TaskResult{Result: res, Buffered: true, Error: err}
		},
		action: ActionExec,
	}
}

func BeginTx() TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {

			err := bw.commitWithTrigger(triggerManual)
			if err != nil {
				return TaskResult{Result: nil, Error: err}
			}

			bw.tx, err = bw.DB.Begin()

			if err != nil {
				return TaskResult{Result: nil, Error: err}
			}

			bw.isPending = true

			return TaskResult{}
		},
		action: ActionBegin,
	}
}

// Commit creates a TaskFunc that commits the current transaction.
// It also sets Flush() to true, ensuring any pending buffered writes
// are flushed before this task is processed.
func Commit() TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			err := bw.commitWithTrigger(triggerManual)
			if err != nil {
				return TaskResult{Result: nil, Error: err}
			}
			bw.isPending = false
			return TaskResult{Result: nil, Error: nil}
		},
		action: ActionCommit,
	}
}

func Rollback() TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(bw *BufferWriter) TaskResult {
			var err error
			if bw.tx != nil {
				err = bw.tx.Rollback()
			}
			bw.isPending = false
			bw.tx = nil
			bw.buffer = 0

			return TaskResult{Result: nil, Error: err}

		},
		action: ActionRollback,
	}
}
