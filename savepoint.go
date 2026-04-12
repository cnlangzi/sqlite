package sqlite

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"log/slog"
)

// Savepoint represents a named SQLite savepoint within a transaction.
// Savepoints allow partial rollback of a transaction without aborting it entirely.
// The savepoint is created with a cryptographically random name to avoid
// collisions when multiple savepoints are used concurrently.
type Savepoint struct {
	tx   *sql.Tx  // the parent transaction this savepoint belongs to
	name string   // unique SQLite savepoint name
	ok   bool     // false once Rollback or Release has been called
}

// NewSavepoint creates and names a new SQLite savepoint within the given
// transaction. The savepoint is immediately active. Use Release() to
// commit the savepoint or Rollback() to undo all statements since it was created.
func NewSavepoint(tx *sql.Tx) (*Savepoint, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	name := "sp_" + hex.EncodeToString(b)

	_, err := tx.Exec("SAVEPOINT " + name)
	if err != nil {
		return nil, err
	}

	return &Savepoint{tx: tx, name: name, ok: true}, nil
}

// Rollback undoes all database changes made after the savepoint was created.
// After Rollback is called, ok is set to false and subsequent calls are no-ops.
func (s *Savepoint) Rollback() {
	if !s.ok {
		return
	}

	_, err := s.tx.Exec("ROLLBACK TO " + s.name)
	if err != nil {
		slog.Error("sqlite: rollback to savepoint", slog.String("err", err.Error()))
	}
}

// Release commits the savepoint, making all changes since its creation permanent.
// After Release is called, ok is set to false and subsequent calls are no-ops.
func (s *Savepoint) Release() {
	if !s.ok {
		return
	}
	_, err := s.tx.Exec("RELEASE " + s.name)
	if err != nil {
		slog.Error("sqlite: release savepoint", slog.String("err", err.Error()))
	}
}

// Name returns the SQLite savepoint name. Useful for debugging and for
// executing raw ROLLBACK TO or RELEASE statements if needed.
func (s *Savepoint) Name() string {
	return s.name
}
