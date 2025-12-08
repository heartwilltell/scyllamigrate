package scyllamigrate

import (
	"io/fs"
	"log"

	"github.com/gocql/gocql"
)

// Option configures a Migrator.
type Option func(*Migrator) error

// WithSource sets the migration source.
func WithSource(source Source) Option {
	return func(m *Migrator) error {
		m.source = source
		return nil
	}
}

// WithFS sets the migration source from an fs.FS instance.
// This is useful for embedded migrations via go:embed.
func WithFS(fsys fs.FS) Option {
	return func(m *Migrator) error {
		source, err := NewFSSource(fsys)
		if err != nil {
			return err
		}
		m.source = source
		return nil
	}
}

// WithDir sets the migration source from a filesystem directory path.
func WithDir(path string) Option {
	return func(m *Migrator) error {
		source, err := NewDirSource(path)
		if err != nil {
			return err
		}
		m.source = source
		return nil
	}
}

// WithKeyspace sets the keyspace for the migration history table.
func WithKeyspace(keyspace string) Option {
	return func(m *Migrator) error {
		m.keyspace = keyspace
		return nil
	}
}

// WithHistoryTable sets the name of the migration history table.
// Default is "schema_migrations".
func WithHistoryTable(table string) Option {
	return func(m *Migrator) error {
		m.historyTable = table
		return nil
	}
}

// Logger is the interface for logging migration progress.
type Logger interface {
	Printf(format string, v ...any)
}

// WithLogger sets a logger for migration progress.
func WithLogger(logger Logger) Option {
	return func(m *Migrator) error {
		m.logger = logger
		return nil
	}
}

// WithStdLogger sets the standard library logger for migration progress.
func WithStdLogger(l *log.Logger) Option {
	return func(m *Migrator) error {
		m.logger = l
		return nil
	}
}

// WithConsistency sets the consistency level for migration queries.
// Default is gocql.Quorum.
func WithConsistency(consistency gocql.Consistency) Option {
	return func(m *Migrator) error {
		m.consistency = consistency
		return nil
	}
}

// WithSchemaAgreement sets whether to wait for schema agreement after each migration.
// Default is true.
func WithSchemaAgreement(wait bool) Option {
	return func(m *Migrator) error {
		m.waitForSchemaAgreement = wait
		return nil
	}
}

// WithSchemaAgreementTimeout sets the timeout for waiting for schema agreement.
// If not set, the session's default timeout is used.
func WithSchemaAgreementTimeout(timeout int) Option {
	return func(m *Migrator) error {
		m.schemaAgreementTimeout = timeout
		return nil
	}
}
