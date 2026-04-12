package sqlite

import (
	"context"
	"testing"
)

func TestOpen_Memory(t *testing.T) {
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Test write
	_, err = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	_, err = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "hello")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test read
	row := db.Reader.QueryRow("SELECT name FROM test WHERE id = 1")
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if name != "hello" {
		t.Errorf("expected 'hello', got '%s'", name)
	}
}

func TestOpen_File(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	db, err := Open(context.Background(), tmpFile)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Test write
	_, err = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// flush immediately to ensure data is written to disk
	db.Writer.Flush()

	// Test read after write
	rows, err := db.Reader.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var tableName string
	found := false
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if tableName == "test" {
			found = true
		}
	}

	if !found {
		t.Error("expected to find 'test' table")
	}
}

func TestExecContext(t *testing.T) {
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}
}

func TestQueryContext(t *testing.T) {
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create and insert data using Writer
	_, _ = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "test")

	// Query using Reader
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT name FROM test")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	_ = rows.Close()

	if name != "test" {
		t.Errorf("expected 'test', got '%s'", name)
	}
}

func TestQueryRowContext(t *testing.T) {
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create and insert data
	_, _ = db.Writer.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Writer.Exec("INSERT INTO test (name) VALUES (?)", "row")

	// Query single row
	ctx := context.Background()
	row := db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = 1")

	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("QueryRowContext failed: %v", err)
	}

	if name != "row" {
		t.Errorf("expected 'row', got '%s'", name)
	}
}
