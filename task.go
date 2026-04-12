package sqlite

import (
	"database/sql"
	"log/slog"
)

type TaskFunc interface {
	Exec(w *sql.Tx) TaskResult
	Notify() chan TaskResult
	Flush() bool
}

type TaskResult struct {
	Result any
	Error  error
}

type TaskArgs struct {
	query string
	args  []any
}

type Task struct {
	notify chan TaskResult
	exec   func(tx *sql.Tx) TaskResult
	flush  bool
}

func (t *Task) Notify() chan TaskResult {
	return t.notify
}

func (t *Task) Exec(tx *sql.Tx) TaskResult {
	return t.exec(tx)
}

func (t *Task) Flush() bool {
	return t.flush
}

func Query(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			res, err := tx.Exec(query, args...)
			return TaskResult{Result: res, Error: err}
		},
	}
}

func QueryRow(query string, args ...any) TaskFunc {
	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			row := tx.QueryRow(query, args...)
			return TaskResult{Result: row, Error: nil}
		},
	}
}

func Exec(query string, args ...any) TaskFunc {

	return &Task{
		notify: make(chan TaskResult, 1),
		exec: func(tx *sql.Tx) TaskResult {
			res, err := tx.Exec(query, args...)
			return TaskResult{Result: res, Error: err}
		},
	}
}

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
