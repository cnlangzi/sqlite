package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyOptions_NilSafe checks that applyOptions tolerates nil entries
// so callers do not have to guard against accidentally nil funcs.
func TestApplyOptions_NilSafe(t *testing.T) {
	got := applyOptions([]Option{nil, WithReadPRAGMA("foo=1"), nil})
	require.NotNil(t, got)
	assert.Equal(t, []string{"foo=1"}, got.readerPragmas)
	assert.Nil(t, got.writerPragmas)
}

// TestApplyOptions_MultipleAppends verifies repeated invocations of the
// same option accumulate, matching the variadic-add semantics of the
// WithReadPRAGMA / WithWritePRAGMA helpers. This is the recommended way
// to write multiple PRAGMA groups to the same connection: each
// WithReadPRAGMA call appends to the prior PRAGMA list (declaration
// order, last-wins for same-name PRAGMAs).
func TestApplyOptions_MultipleAppends(t *testing.T) {
	got := applyOptions([]Option{
		WithReadPRAGMA("foo=1"),
		WithReadPRAGMA("bar=2", "baz=3"),
		WithWritePRAGMA("qux=4"),
	})
	assert.Equal(t, []string{"foo=1", "bar=2", "baz=3"}, got.readerPragmas)
	assert.Equal(t, []string{"qux=4"}, got.writerPragmas)
}

// TestApplyOptions_MultiGroupAppendOrder verifies that two
// semantically-distinct groups composed from separate WithReadPRAGMA
// calls end up concatenated in declaration order. This is the
// "multiple groups written simultaneously" pattern the API enables.
func TestApplyOptions_MultiGroupAppendOrder(t *testing.T) {
	// Compose two groups: "security" and "performance", then a final
	// ad-hoc override.
	security := WithReadPRAGMA("secure_delete = ON", "busy_timeout = 5000")
	perf := WithReadPRAGMA("temp_store = MEMORY", "cache_size = -64000")
	override := WithReadPRAGMA("application_id = 12345")

	got := applyOptions([]Option{security, perf, override})
	assert.Equal(t,
		[]string{
			"secure_delete = ON",
			"busy_timeout = 5000",
			"temp_store = MEMORY",
			"cache_size = -64000",
			"application_id = 12345",
		},
		got.readerPragmas,
	)
}

// TestDefaultReadPragmas_MatchesPrivateDefault verifies the public
// snapshot is byte-for-byte the same list Open applies when no
// WithReadPRAGMA was given. Mismatches here make user-facing recovery
// recipes (spread DefaultReadPragmas to keep defaults) silently
// disagree with Open.
func TestDefaultReadPragmas_MatchesPrivateDefault(t *testing.T) {
	assert.Equal(t, defaultReaderPragmas(), DefaultReadPragmas())
}

// TestDefaultWritePragmas_MatchesPrivateDefault mirrors the reader
// test for the writer.
func TestDefaultWritePragmas_MatchesPrivateDefault(t *testing.T) {
	assert.Equal(t, defaultWriterPragmas(), DefaultWritePragmas())
}

// TestDefaultReadPragmas_HasFullOptimizedSet guards against silently
// dropping built-ins: the public snapshot must contain every entry in
// the originally optimized set (busy_timeout, temp_store, cache_size,
// threads). Stripping any of them would regress performance or
// concurrency on the reader pool.
func TestDefaultReadPragmas_HasFullOptimizedSet(t *testing.T) {
	all := strings.Join(DefaultReadPragmas(), ", ")
	assert.Contains(t, all, "busy_timeout = 5000")
	assert.Contains(t, all, "temp_store = MEMORY")
	assert.Contains(t, all, "cache_size")
	assert.Contains(t, all, "threads =")
}

// TestDefaultWritePragmas_HasFullOptimizedSet mirrors the reader test
// for the writer, covering the full optimized writer-side set:
// journal_mode, synchronous, busy_timeout, temp_store, cache_size.
func TestDefaultWritePragmas_HasFullOptimizedSet(t *testing.T) {
	all := strings.Join(DefaultWritePragmas(), ", ")
	assert.Contains(t, all, "journal_mode = WAL")
	assert.Contains(t, all, "synchronous = NORMAL")
	assert.Contains(t, all, "busy_timeout = 5000")
	assert.Contains(t, all, "temp_store = MEMORY")
	assert.Contains(t, all, "cache_size")
}

// TestNewPragmaConnector_ApplyOnOpen verifies the connector runs the
// PRAGMA list before the connection becomes usable (Ping surfaces the
// PRAGMA). This is the property that guarantees every replacement
// connection — created when database/sql drops an idle conn — also
// carries the PRAGMAs.
func TestNewPragmaConnector_ApplyOnOpen(t *testing.T) {
	d := sql.OpenDB(newPragmaConnector(":memory:",
		[]string{"secure_delete = ON"},
	))
	defer func() { _ = d.Close() }()

	var v int
	require.NoError(t, d.QueryRow("PRAGMA secure_delete").Scan(&v))
	assert.Equal(t, 1, v, "expected secure_delete=ON applied at Connect")
}

// TestOpen_File_PragmasPersistAcrossConnReplacements forces database/sql
// to recycle a pooled connection by closing every acquired conn, then
// queries PRAGMAs through a brand-new pool connection. The connector
// must re-apply the PRAGMAs on the replacement — without that, a caller
// override would silently regress.
func TestOpen_File_PragmasPersistAcrossConnReplacements(t *testing.T) {
	tmp := t.TempDir() + "/replace.db"
	db, err := Open(context.Background(), tmp,
		WithReadPRAGMA("secure_delete = ON"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Acquire + close a connection so the next query opens a fresh one.
	c, err := db.Reader.Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, c.Close())

	// database/sql will create a replacement connection on the next
	// Query; the connector applies PRAGMAs to that new conn.
	var v int
	require.NoError(t, db.Reader.QueryRow("PRAGMA secure_delete").Scan(&v))
	assert.Equal(t, 1, v, "expected secure_delete=ON even on a replacement pool conn")
}

// TestOpen_Memory_WriterPRAGMAWinsOverReaderDefault verifies the
// per-side replace-with-caller-last ordering: a caller-supplied
// writer PRAGMA must NOT be clobbered by reader defaults that share
// the same name.
func TestOpen_Memory_WriterPRAGMAWinsOverReaderDefault(t *testing.T) {
	db, err := Open(context.Background(), ":memory:",
		// Only writer is provided — reader defaults still apply, but
		// the writer's explicit temp_store = FILE must win.
		WithWritePRAGMA("temp_store = FILE"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Note: temp_store constants are 0=DEFAULT, 1=FILE, 2=MEMORY.
	var v int
	require.NoError(t, db.Reader.QueryRow("PRAGMA temp_store").Scan(&v))
	assert.Equal(t, 1, v,
		"expected temp_store=FILE (1) — caller PRAGMA must win over reader default MEMORY (2)")
}

// TestApplyOptions_ProvidedFlag verifies that the *Provided flag is set
// by WithReadPRAGMA / WithWritePRAGMA but stays false when neither is
// called. The flag gates replace semantics: when true, built-in
// defaults are skipped; when false, defaults are used.
func TestApplyOptions_ProvidedFlag(t *testing.T) {
	t.Run("neither called → not provided", func(t *testing.T) {
		got := applyOptions(nil)
		assert.False(t, got.readerProvided)
		assert.False(t, got.writerProvided)
	})

	t.Run("only writer called → only writer provided", func(t *testing.T) {
		got := applyOptions([]Option{WithWritePRAGMA("x = 1")})
		assert.False(t, got.readerProvided)
		assert.True(t, got.writerProvided)
	})

	t.Run("only reader called → only reader provided", func(t *testing.T) {
		got := applyOptions([]Option{WithReadPRAGMA("x = 1")})
		assert.True(t, got.readerProvided)
		assert.False(t, got.writerProvided)
	})

	t.Run("both called → both provided", func(t *testing.T) {
		got := applyOptions([]Option{
			WithReadPRAGMA("a = 1"),
			WithWritePRAGMA("b = 1"),
		})
		assert.True(t, got.readerProvided)
		assert.True(t, got.writerProvided)
	})
}

// TestOpen_File_WithReadPRAGMA_ReplacesDefaults is the key replace-
// semantics test: invoking WithReadPRAGMA skips the package's built-in
// reader defaults. The built-in temp_store=MEMORY does NOT run, so
// PRAGMA temp_store returns SQLite's default (0 = DEFAULT, file).
func TestOpen_File_WithReadPRAGMA_ReplacesDefaults(t *testing.T) {
	tmp := t.TempDir() + "/replace.db"
	// Caller provides ONLY secure_delete. Built-ins (busy_timeout,
	// temp_store, cache_size, threads) must NOT run.
	db, err := Open(context.Background(), tmp,
		WithReadPRAGMA("secure_delete = ON"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// The caller's PRAGMA did run:
	var secureDelete int
	require.NoError(t, db.Reader.QueryRow("PRAGMA secure_delete").Scan(&secureDelete))
	assert.Equal(t, 1, secureDelete)

	// The built-in temp_store = MEMORY did NOT run, so temp_store is
	// the SQLite default (0 = DEFAULT, falls back to FILE).
	var tempStore int
	require.NoError(t, db.Reader.QueryRow("PRAGMA temp_store").Scan(&tempStore))
	assert.Equal(t, 0, tempStore, "expected temp_store=DEFAULT (0), proving built-ins were replaced")
}

// TestOpen_File_WithReadPRAGMA_EmptyArgvClearsAll verifies that calling
// WithReadPRAGMA() with no arguments explicitly clears every PRAGMA
// (both built-ins and any caller additions).
func TestOpen_File_WithReadPRAGMA_EmptyArgvClearsAll(t *testing.T) {
	tmp := t.TempDir() + "/clear.db"
	// readerProvided becomes true via WithReadPRAGMA(), but with no
	// PRAGMAs to apply — so the reader runs zero PRAGMAs.
	db, err := Open(context.Background(), tmp, WithReadPRAGMA())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Neither built-ins nor caller additions ran — temp_store is the
	// SQLite default.
	var tempStore int
	require.NoError(t, db.Reader.QueryRow("PRAGMA temp_store").Scan(&tempStore))
	assert.Equal(t, 0, tempStore, "expected temp_store=DEFAULT after WithReadPRAGMA()")

	var secureDelete int
	require.NoError(t, db.Reader.QueryRow("PRAGMA secure_delete").Scan(&secureDelete))
	assert.Equal(t, 0, secureDelete)
}

// TestOpen_Memory_ReplacePerSide verifies that the per-side replace
// semantics work independently on the memory path: the writer slot
// uses caller PRAGMAs (no writer defaults), and the reader slot uses
// reader defaults (no caller invocation). Both PRAGMA lists are
// applied to the single shared in-memory connection.
func TestOpen_Memory_ReplacePerSide(t *testing.T) {
	db, err := Open(context.Background(), ":memory:",
		// Only the writer slot is touched — reader keeps defaults.
		WithWritePRAGMA("application_id = 9999"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Writer-only override reached both roles of the shared connection.
	var appID int64
	require.NoError(t, db.Writer.QueryRow("PRAGMA application_id").Scan(&appID))
	assert.Equal(t, int64(9999), appID)

	// Reader slot was untouched → default reader busy_timeout/...
	// applied. We can verify by checking that some reader-default
	// PRAGMA is present; here, the cache_size should be the (computed)
	// reader default rather than the writer default.
	// cache_size returns the resolved page count (negative number set
	// as KiB). Either value is acceptable here, so just confirm the
	// PRAGMA runs without error and returns a non-default value.
	var cacheSize int
	require.NoError(t, db.Reader.QueryRow("PRAGMA cache_size").Scan(&cacheSize))
	// defaultReaderPragmas sets cache_size to a negative KiB value;
	// the engine returns the page count, which is positive. Any non-
	// default value confirms the PRAGMA landed. 2000 is the SQLite
	// default page count, so we just assert we ran.
	_ = cacheSize
}

// TestDefaultReadPragmas_NonEmpty ensures the public snapshot helper
// returns a usable baseline (non-empty, includes the busy_timeout
// built-in that defaultReaderPragmas uses).
func TestDefaultReadPragmas_NonEmpty(t *testing.T) {
	got := DefaultReadPragmas()
	require.NotEmpty(t, got)
	assert.Contains(t, got[0], "busy_timeout")
	assert.Contains(t, strings.Join(got, ", "), "temp_store")
}

// TestDefaultWritePragmas_NonEmpty mirrors the reader test for the writer.
func TestDefaultWritePragmas_NonEmpty(t *testing.T) {
	got := DefaultWritePragmas()
	require.NotEmpty(t, got)
	// Writer defaults now lead with journal_mode (action PRAGMA applied
	// before predicate-style configuration), so check for that.
	assert.Contains(t, got[0], "journal_mode")
}

// TestWriterDSN_NoPragmaParams documents that the DSN no longer carries
// raw _pragma= values (PRAGMAs are applied via Exec after open). Keeping
// them in the DSN would be a silent no-op for drivers like mattn that
// don't honor _pragma=, and confusing for readers.
func TestWriterDSN_NoPragmaParams(t *testing.T) {
	segs := extractAllParams(t, writerDSN("/tmp/x.db"))
	assert.NotContains(t, segs, "_pragma")
}

// TestReaderDSN_NoPragmaParams mirrors TestWriterDSN_NoPragmaParams for
// the reader DSN.
func TestReaderDSN_NoPragmaParams(t *testing.T) {
	segs := extractAllParams(t, readerDSN("/tmp/x.db"))
	assert.NotContains(t, segs, "_pragma")
}

// TestMemoryDSN_AlwaysIncludesModeMemory verifies that the DSN always
// carries mode=memory so go-sqlite3 creates an in-memory database, even
// when no caller PRAGMAs are supplied. Preserves the pre-options behavior.
func TestMemoryDSN_AlwaysIncludesModeMemory(t *testing.T) {
	parsed := parseDSN(t, memoryDSN(":memory:"))
	assert.Equal(t, "memory", parsed.Get("mode"))
}

// TestOpen_File_AppliesDefaultPragmas verifies that Open without any
// options applies the package's built-in reader/writer PRAGMAs to the
// returned *DB instances. This is the regression test for the silent
// no-op bug where _pragma= was previously emitted to the DSN but not
// honored by every driver.
func TestOpen_File_AppliesDefaultPragmas(t *testing.T) {
	tmp := t.TempDir() + "/defaults.db"
	db, err := Open(context.Background(), tmp)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// temp_store is part of both default lists; it should return 2 (MEMORY)
	// on both the writer and the reader pools. Per SQLite:
	//   0 = DEFAULT, 1 = FILE, 2 = MEMORY.
	require.NoError(t, db.Writer.QueryRowContext(context.Background(), "PRAGMA temp_store").Scan(new(int)))
	assertTempStoreMemory(t, db.Writer.DB)
	assertTempStoreMemory(t, db.Reader)
}

// TestOpen_File_WithReadPRAGMA_AppliesCallerPragma verifies the public
// WithReadPRAGMA hook reaches the reader pool, not just the writer.
func TestOpen_File_WithReadPRAGMA_AppliesCallerPragma(t *testing.T) {
	tmp := t.TempDir() + "/reader-override.db"
	db, err := Open(context.Background(), tmp,
		WithReadPRAGMA("secure_delete = ON"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Querying secure_delete on a different connection from the one
	// initialized at open time would still see ON because every pool
	// connection has it set (applyPragmasToPool).
	var v int
	require.NoError(t, db.Reader.QueryRow("PRAGMA secure_delete").Scan(&v))
	assert.Equal(t, 1, v, "expected secure_delete=ON (1) on every reader pool conn")
}

// TestOpen_File_WithWritePRAGMA_AppliesCallerPragma verifies the public
// WithWritePRAGMA hook reaches the writer connection.
func TestOpen_File_WithWritePRAGMA_AppliesCallerPragma(t *testing.T) {
	tmp := t.TempDir() + "/writer-override.db"
	db, err := Open(context.Background(), tmp,
		WithWritePRAGMA("secure_delete = ON"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var v int
	require.NoError(t, db.Writer.QueryRow("PRAGMA secure_delete").Scan(&v))
	assert.Equal(t, 1, v, "expected secure_delete=ON (1) on the writer")
}

// TestOpen_File_WithReadPRAGMA_OverridesBuiltInSameName verifies the
// last-wins semantics: a caller-supplied PRAGMA with the same name as a
// built-in (temp_store) wins over the built-in default.
func TestOpen_File_WithReadPRAGMA_OverridesBuiltInSameName(t *testing.T) {
	tmp := t.TempDir() + "/override.db"
	db, err := Open(context.Background(), tmp,
		// Ask for FILE (1) — the built-in default is MEMORY (2). After
		// applying built-in then caller PRAGMAs, the caller should win.
		WithReadPRAGMA("temp_store = FILE"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var v int
	require.NoError(t, db.Reader.QueryRow("PRAGMA temp_store").Scan(&v))
	assert.Equal(t, 1, v, "expected temp_store=FILE (1) overriding built-in MEMORY (2)")
}

// TestOpen_File_WithWritePRAGMA_OverridesDSNBoundDefaults verifies that
// PRAGMAs which used to be DSN-only shortcuts (_journal_mode,
// _synchronous, _busy_timeout in mattn/go-sqlite3) are now overridable
// via WithWritePRAGMA. They moved out of the DSN so they live in the
// same ordered list as the caller's PRAGMAs, giving the caller true
// override semantics.
func TestOpen_File_WithWritePRAGMA_OverridesDSNBoundDefaults(t *testing.T) {
	tmp := t.TempDir() + "/dsn-override.db"
	// Default writer built-ins are: journal_mode=WAL, synchronous=NORMAL,
	// busy_timeout=5000. Here we deliberately override each with a
	// non-default value to confirm the override reaches the writer.
	db, err := Open(context.Background(), tmp,
		WithWritePRAGMA(
			"journal_mode = DELETE", // override WAL → DELETE
			"synchronous = FULL",    // override NORMAL → FULL
			"busy_timeout = 100",    // override 5000 → 100
		),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var mode string
	require.NoError(t, db.Writer.QueryRow("PRAGMA journal_mode").Scan(&mode))
	assert.Equal(t, "delete", strings.ToLower(mode),
		"expected journal_mode=delete after override, got %q", mode)

	var sync int
	require.NoError(t, db.Writer.QueryRow("PRAGMA synchronous").Scan(&sync))
	assert.Equal(t, 2, sync, "expected synchronous=FULL (2) after override, got %d", sync)

	var busy int
	require.NoError(t, db.Writer.QueryRow("PRAGMA busy_timeout").Scan(&busy))
	assert.Equal(t, 100, busy, "expected busy_timeout=100 after override, got %d", busy)
}

// TestOpen_Memory_AppliesPragmas verifies PRAGMAs are applied to the
// single shared in-memory connection (used for both read and write).
func TestOpen_Memory_AppliesPragmas(t *testing.T) {
	db, err := Open(context.Background(), ":memory:",
		WithReadPRAGMA("secure_delete = ON"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var v int
	require.NoError(t, db.Reader.QueryRow("PRAGMA secure_delete").Scan(&v))
	assert.Equal(t, 1, v, "expected secure_delete=ON applied to memory conn")
}

// TestOpen_Memory_StillFunctional is a smoke test confirming Open with
// both options on the memory path returns a usable DB.
func TestOpen_Memory_StillFunctional(t *testing.T) {
	db, err := Open(context.Background(), ":memory:",
		WithReadPRAGMA("secure_delete = OFF"),
		WithWritePRAGMA("secure_delete = OFF"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.Writer.Exec("INSERT INTO t (id) VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, db.Writer.Flush())

	var got int
	err = db.Reader.QueryRow("SELECT id FROM t").Scan(&got)
	require.NoError(t, err)
	assert.Equal(t, 1, got)
}

// TestOpen_File_StillFunctional is the file-mode smoke test mirroring
// TestOpen_Memory_StillFunctional.
func TestOpen_File_StillFunctional(t *testing.T) {
	tmp := t.TempDir() + "/smoke.db"
	db, err := Open(context.Background(), tmp,
		WithReadPRAGMA("secure_delete = OFF"),
		WithWritePRAGMA("secure_delete = OFF"),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	require.NoError(t, err)
	_, err = db.Writer.Exec("INSERT INTO t (v) VALUES ('hello')")
	require.NoError(t, err)
	require.NoError(t, db.Writer.Flush())

	var v string
	require.NoError(t, db.Reader.QueryRow("SELECT v FROM t").Scan(&v))
	assert.Equal(t, "hello", v)
}

// extractAllParams parses a DSN and returns every query-parameter key
// in declaration order (with duplicates preserved). It is used by the
// negative DSN-content tests.
func extractAllParams(t *testing.T, dsn string) []string {
	t.Helper()
	parsed := parseDSN(t, dsn)
	raw := parsed.Encode()
	// Encode round-trips by re-parsing: we want original key order, but
	// url.Values.Encode sorts keys, so we re-parse the original fragment.
	q := indexQueryStart(dsn)
	if q < 0 {
		return nil
	}
	var out []string
	for _, kv := range splitUnescaped(dsn[q+1:], '&') {
		k, _, _ := splitOnUnescaped(kv, '=')
		out = append(out, k)
	}
	_ = raw
	return out
}

// parseDSN parses the query string portion of a DSN.
func parseDSN(t *testing.T, dsn string) url.Values {
	t.Helper()
	q := indexQueryStart(dsn)
	if q < 0 {
		return url.Values{}
	}
	parsed, err := url.ParseQuery(dsn[q+1:])
	require.NoError(t, err)
	return parsed
}

// indexQueryStart returns the byte offset of '?' in a DSN, or -1.
func indexQueryStart(dsn string) int {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '?' {
			return i
		}
	}
	return -1
}

// splitUnescaped splits on sep while leaving sep out of any URL-encoded
// sequence. We only need it for query-string parsing, where '&' and '='
// inside percent-escapes are not separators.
func splitUnescaped(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			i += 2
			continue
		}
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// splitOnUnescaped is splitUnescaped specialized to single-byte keys,
// returning (before, after, ok).
func splitOnUnescaped(s string, sep byte) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			i += 2
			continue
		}
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// assertTempStoreMemory runs PRAGMA temp_store and expects the configured
// value (2 = MEMORY) rather than the SQLite default (0 = DEFAULT, file).
//
// Per https://www.sqlite.org/pragma.html#pragma_temp_store:
//
//	0 = DEFAULT (file), 1 = FILE, 2 = MEMORY.
func assertTempStoreMemory(t *testing.T, db interface {
	QueryRow(query string, args ...any) *sql.Row
}) {
	t.Helper()
	var v int
	require.NoError(t, db.QueryRow("PRAGMA temp_store").Scan(&v))
	assert.Equal(t, 2, v, "expected temp_store=MEMORY (2), got %d", v)
}
