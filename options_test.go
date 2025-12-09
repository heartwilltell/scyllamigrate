package scyllamigrate

import (
	"log"
	"log/slog"
	"os"
	"testing"
	"testing/fstest"

	"github.com/gocql/gocql"
	td "github.com/maxatome/go-testdeep/td"
)

func TestWithSource(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}
	source, err := NewFSSource(fsys)
	td.CmpNoError(t, err)

	m := &Migrator{}
	opt := WithSource(source)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.source, source)
}

func TestWithFS(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}

	m := &Migrator{}
	opt := WithFS(fsys)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.source, td.NotNil())
}

func TestWithDir(t *testing.T) {
	// Create a temporary directory with a migration file
	tmpDir := t.TempDir()
	migrationFile := tmpDir + "/000001_create_users.up.cql"
	td.CmpNoError(t, os.WriteFile(migrationFile, []byte("CREATE TABLE users;"), 0644))

	m := &Migrator{}
	opt := WithDir(tmpDir)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.source, td.NotNil())
}

func TestWithDir_InvalidPath(t *testing.T) {
	m := &Migrator{}
	opt := WithDir("/nonexistent/path/that/does/not/exist")

	td.CmpError(t, opt(m))
}

func TestWithKeyspace(t *testing.T) {
	m := &Migrator{}
	opt := WithKeyspace("test_keyspace")

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.keyspace, "test_keyspace")
}

func TestWithHistoryTable(t *testing.T) {
	m := &Migrator{}
	opt := WithHistoryTable("custom_migrations")

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.historyTable, "custom_migrations")
}

func TestWithLogger(t *testing.T) {
	logger := slog.Default()

	m := &Migrator{}
	opt := WithLogger(logger)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.logger, logger)
}

func TestWithLogger_Nil(t *testing.T) {
	m := &Migrator{}
	opt := WithLogger(nil)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.logger, td.Nil())
}

func TestWithStdLogger(t *testing.T) {
	stdLogger := log.New(os.Stderr, "test: ", log.LstdFlags)

	m := &Migrator{}
	opt := WithStdLogger(stdLogger)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.logger, td.NotNil())
}

func TestWithStdLogger_Nil(t *testing.T) {
	m := &Migrator{}
	opt := WithStdLogger(nil)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.logger, td.Nil())
}

func TestWithConsistency(t *testing.T) {
	m := &Migrator{}
	opt := WithConsistency(gocql.All)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.consistency, gocql.All)
}

func TestWithSchemaAgreement(t *testing.T) {
	m := &Migrator{}
	opt := WithSchemaAgreement(false)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.waitForSchemaAgreement, false)

	opt = WithSchemaAgreement(true)
	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.waitForSchemaAgreement, true)
}

func TestWithSchemaAgreementTimeout(t *testing.T) {
	m := &Migrator{}
	opt := WithSchemaAgreementTimeout(5000)

	td.CmpNoError(t, opt(m))
	td.Cmp(t, m.schemaAgreementTimeout, 5000)
}

func TestMultipleOptions(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}

	m := &Migrator{}
	opts := []Option{
		WithFS(fsys),
		WithKeyspace("test_keyspace"),
		WithHistoryTable("custom_table"),
		WithLogger(slog.Default()),
		WithConsistency(gocql.Quorum),
		WithSchemaAgreement(true),
		WithSchemaAgreementTimeout(1000),
	}

	for _, opt := range opts {
		td.CmpNoError(t, opt(m))
	}

	td.Cmp(t, m.source, td.NotNil())
	td.Cmp(t, m.keyspace, "test_keyspace")
	td.Cmp(t, m.historyTable, "custom_table")
	td.Cmp(t, m.logger, td.NotNil())
	td.Cmp(t, m.consistency, gocql.Quorum)
	td.Cmp(t, m.waitForSchemaAgreement, true)
	td.Cmp(t, m.schemaAgreementTimeout, 1000)
}

