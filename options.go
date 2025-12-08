package scyllamigrate

import (
	"context"
	"io/fs"
	"log"
	"log/slog"

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

// WithLogger sets a logger for migration progress.
func WithLogger(logger *slog.Logger) Option {
	return func(m *Migrator) error {
		m.logger = logger
		return nil
	}
}

// WithStdLogger sets the standard library logger for migration progress.
// It wraps the log.Logger in a slog.Logger for compatibility.
func WithStdLogger(l *log.Logger) Option {
	return func(m *Migrator) error {
		if l == nil {
			m.logger = nil
			return nil
		}
		m.logger = slog.New(&logHandler{logger: l})
		return nil
	}
}

// logHandler adapts a *log.Logger to slog.Handler for backward compatibility.
type logHandler struct {
	logger *log.Logger
}

func (h *logHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *logHandler) Handle(_ context.Context, _ slog.Record) error {
	// This method is not used since we use LogAttrs.
	return nil
}

func (h *logHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *logHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *logHandler) LogAttrs(_ context.Context, _ slog.Level, msg string, _ ...slog.Attr) {
	h.logger.Printf("%s", msg)
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
