package scyllamigrate

import (
	"log"
	"log/slog"
	"os"
	"testing"
	"testing/fstest"

	"github.com/gocql/gocql"
)

func TestWithSource(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}
	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	m := &Migrator{}
	opt := WithSource(source)

	if err := opt(m); err != nil {
		t.Fatalf("WithSource() error = %v", err)
	}

	if m.source != source {
		t.Error("WithSource() did not set source")
	}
}

func TestWithFS(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}

	m := &Migrator{}
	opt := WithFS(fsys)

	if err := opt(m); err != nil {
		t.Fatalf("WithFS() error = %v", err)
	}

	if m.source == nil {
		t.Fatal("WithFS() did not set source")
	}
}

func TestWithDir(t *testing.T) {
	// Create a temporary directory with a migration file
	tmpDir := t.TempDir()
	migrationFile := tmpDir + "/000001_create_users.up.cql"
	if err := os.WriteFile(migrationFile, []byte("CREATE TABLE users;"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	m := &Migrator{}
	opt := WithDir(tmpDir)

	if err := opt(m); err != nil {
		t.Fatalf("WithDir() error = %v", err)
	}

	if m.source == nil {
		t.Fatal("WithDir() did not set source")
	}
}

func TestWithDir_InvalidPath(t *testing.T) {
	m := &Migrator{}
	opt := WithDir("/nonexistent/path/that/does/not/exist")

	if err := opt(m); err == nil {
		t.Error("WithDir() error = nil, want error for invalid path")
	}
}

func TestWithKeyspace(t *testing.T) {
	m := &Migrator{}
	opt := WithKeyspace("test_keyspace")

	if err := opt(m); err != nil {
		t.Fatalf("WithKeyspace() error = %v", err)
	}

	if m.keyspace != "test_keyspace" {
		t.Errorf("WithKeyspace() keyspace = %q, want %q", m.keyspace, "test_keyspace")
	}
}

func TestWithHistoryTable(t *testing.T) {
	m := &Migrator{}
	opt := WithHistoryTable("custom_migrations")

	if err := opt(m); err != nil {
		t.Fatalf("WithHistoryTable() error = %v", err)
	}

	if m.historyTable != "custom_migrations" {
		t.Errorf("WithHistoryTable() historyTable = %q, want %q", m.historyTable, "custom_migrations")
	}
}

func TestWithLogger(t *testing.T) {
	logger := slog.Default()

	m := &Migrator{}
	opt := WithLogger(logger)

	if err := opt(m); err != nil {
		t.Fatalf("WithLogger() error = %v", err)
	}

	if m.logger != logger {
		t.Error("WithLogger() did not set logger")
	}
}

func TestWithLogger_Nil(t *testing.T) {
	m := &Migrator{}
	opt := WithLogger(nil)

	if err := opt(m); err != nil {
		t.Fatalf("WithLogger() error = %v", err)
	}

	if m.logger != nil {
		t.Error("WithLogger(nil) should set logger to nil")
	}
}

func TestWithStdLogger(t *testing.T) {
	stdLogger := log.New(os.Stderr, "test: ", log.LstdFlags)

	m := &Migrator{}
	opt := WithStdLogger(stdLogger)

	if err := opt(m); err != nil {
		t.Fatalf("WithStdLogger() error = %v", err)
	}

	if m.logger == nil {
		t.Fatal("WithStdLogger() did not set logger")
	}
}

func TestWithStdLogger_Nil(t *testing.T) {
	m := &Migrator{}
	opt := WithStdLogger(nil)

	if err := opt(m); err != nil {
		t.Fatalf("WithStdLogger() error = %v", err)
	}

	if m.logger != nil {
		t.Error("WithStdLogger(nil) should set logger to nil")
	}
}

func TestWithConsistency(t *testing.T) {
	m := &Migrator{}
	opt := WithConsistency(gocql.All)

	if err := opt(m); err != nil {
		t.Fatalf("WithConsistency() error = %v", err)
	}

	if m.consistency != gocql.All {
		t.Errorf("WithConsistency() consistency = %v, want %v", m.consistency, gocql.All)
	}
}

func TestWithSchemaAgreement(t *testing.T) {
	m := &Migrator{}
	opt := WithSchemaAgreement(false)

	if err := opt(m); err != nil {
		t.Fatalf("WithSchemaAgreement() error = %v", err)
	}

	if m.waitForSchemaAgreement {
		t.Error("WithSchemaAgreement(false) did not set waitForSchemaAgreement to false")
	}

	opt = WithSchemaAgreement(true)
	if err := opt(m); err != nil {
		t.Fatalf("WithSchemaAgreement() error = %v", err)
	}

	if !m.waitForSchemaAgreement {
		t.Error("WithSchemaAgreement(true) did not set waitForSchemaAgreement to true")
	}
}

func TestWithSchemaAgreementTimeout(t *testing.T) {
	m := &Migrator{}
	opt := WithSchemaAgreementTimeout(5000)

	if err := opt(m); err != nil {
		t.Fatalf("WithSchemaAgreementTimeout() error = %v", err)
	}

	if m.schemaAgreementTimeout != 5000 {
		t.Errorf("WithSchemaAgreementTimeout() timeout = %d, want %d", m.schemaAgreementTimeout, 5000)
	}
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
		if err := opt(m); err != nil {
			t.Fatalf("Option error = %v", err)
		}
	}

	if m.source == nil {
		t.Error("source not set")
	}
	if m.keyspace != "test_keyspace" {
		t.Errorf("keyspace = %q, want %q", m.keyspace, "test_keyspace")
	}
	if m.historyTable != "custom_table" {
		t.Errorf("historyTable = %q, want %q", m.historyTable, "custom_table")
	}
	if m.logger == nil {
		t.Error("logger not set")
	}
	if m.consistency != gocql.Quorum {
		t.Errorf("consistency = %v, want %v", m.consistency, gocql.Quorum)
	}
	if !m.waitForSchemaAgreement {
		t.Error("waitForSchemaAgreement not set")
	}
	if m.schemaAgreementTimeout != 1000 {
		t.Errorf("schemaAgreementTimeout = %d, want %d", m.schemaAgreementTimeout, 1000)
	}
}

