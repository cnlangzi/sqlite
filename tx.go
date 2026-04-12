package sqlite

import (
	"database/sql"
	"errors"
)

type Tx struct {
	w *Writer

	buf  []TaskArgs
	done bool
}

// Exec buffers the query instead of executing immediately.
func (btx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	if btx.done {
		return nil, errors.New("transaction already committed or rolled back")
	}
	btx.buf = append(btx.buf, TaskArgs{query: query, args: args})
	return nil, nil
}

// Commit executes all buffered statements atomically in the global transaction.
func (btx *Tx) Commit() error {
	if btx.done {
		return errors.New("transaction already committed or rolled back")
	}

	btx.done = true
	task, err := btx.w.do(Commit(btx.buf))

	if err != nil {
		return err
	}

	result := <-task

	return result.Error

}

// Rollback discards all buffered statements.
// If already committed (done=true), just return.
func (btx *Tx) Rollback() error {

	if btx.done {
		return nil
	}
	btx.done = true
	btx.buf = nil

	return nil
}
