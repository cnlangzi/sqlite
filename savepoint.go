package sqlite

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"log/slog"
)

type Savepoint struct {
	tx   *sql.Tx
	name string
	ok   bool
}

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

	return &Savepoint{tx: tx, name: name}, nil
}

func (s *Savepoint) Rollback() {

	if !s.ok {
		return
	}

	_, err := s.tx.Exec("ROLLBACK TO " + s.name)
	if err != nil {
		slog.Error("sqlite: rollback to savepoint", slog.String("err", err.Error()))
	}

}

func (s *Savepoint) Release() {
	if !s.ok {
		return
	}
	_, err := s.tx.Exec("RELEASE " + s.name)
	if err != nil {
		slog.Error("sqlite: release savepoint", slog.String("err", err.Error()))
	}
}

func (s *Savepoint) Name() string {
	return s.name
}
