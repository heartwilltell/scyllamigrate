package scyllamigrate

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// Migrator manages database migrations.
type Migrator struct {
	session                *gocql.Session
	source                 Source
	keyspace               string
	historyTable           string
	logger                 Logger
	consistency            gocql.Consistency
	waitForSchemaAgreement bool
	schemaAgreementTimeout int
}

// New creates a new Migrator with the given gocql session and options.
func New(session *gocql.Session, opts ...Option) (*Migrator, error) {
	if session == nil {
		return nil, ErrNoSession
	}

	m := &Migrator{
		session:                session,
		historyTable:           "schema_migrations",
		consistency:            gocql.Quorum,
		waitForSchemaAgreement: true,
	}

	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	if m.source == nil {
		return nil, ErrNoSource
	}

	if m.keyspace == "" {
		return nil, ErrNoKeyspace
	}

	return m, nil
}

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) (int, error) {
	if err := m.ensureHistoryTable(ctx); err != nil {
		return 0, err
	}

	pending, err := m.Pending(ctx)
	if err != nil {
		return 0, err
	}

	if len(pending) == 0 {
		return 0, nil
	}

	applied := 0
	for _, pair := range pending {
		if err := m.applyUp(ctx, pair); err != nil {
			return applied, err
		}
		applied++
	}

	return applied, nil
}

// UpTo applies migrations up to and including the specified version.
func (m *Migrator) UpTo(ctx context.Context, version uint64) (int, error) {
	if err := m.ensureHistoryTable(ctx); err != nil {
		return 0, err
	}

	pending, err := m.Pending(ctx)
	if err != nil {
		return 0, err
	}

	applied := 0
	for _, pair := range pending {
		if pair.Version > version {
			break
		}
		if err := m.applyUp(ctx, pair); err != nil {
			return applied, err
		}
		applied++
	}

	return applied, nil
}

// Down rolls back the last applied migration.
func (m *Migrator) Down(ctx context.Context) error {
	return m.Steps(ctx, -1)
}

// DownTo rolls back migrations down to (but not including) the specified version.
func (m *Migrator) DownTo(ctx context.Context, version uint64) (int, error) {
	if err := m.ensureHistoryTable(ctx); err != nil {
		return 0, err
	}

	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return 0, err
	}

	// Sort by version descending
	sort.Slice(applied, func(i, j int) bool {
		return applied[i].Version > applied[j].Version
	})

	rolledBack := 0
	for _, am := range applied {
		if am.Version <= version {
			break
		}
		if err := m.applyDown(ctx, am.Version); err != nil {
			return rolledBack, err
		}
		rolledBack++
	}

	return rolledBack, nil
}

// Steps applies n migrations. Positive n moves up, negative moves down.
func (m *Migrator) Steps(ctx context.Context, n int) error {
	if err := m.ensureHistoryTable(ctx); err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	if n > 0 {
		// Apply up migrations
		pending, err := m.Pending(ctx)
		if err != nil {
			return err
		}

		if len(pending) == 0 {
			return ErrNoChange
		}

		count := n
		if count > len(pending) {
			count = len(pending)
		}

		for i := 0; i < count; i++ {
			if err := m.applyUp(ctx, pending[i]); err != nil {
				return err
			}
		}
	} else {
		// Rollback migrations
		applied, err := m.getAppliedMigrations(ctx)
		if err != nil {
			return err
		}

		if len(applied) == 0 {
			return ErrNoChange
		}

		// Sort by version descending
		sort.Slice(applied, func(i, j int) bool {
			return applied[i].Version > applied[j].Version
		})

		count := -n
		if count > len(applied) {
			count = len(applied)
		}

		for i := 0; i < count; i++ {
			if err := m.applyDown(ctx, applied[i].Version); err != nil {
				return err
			}
		}
	}

	return nil
}

// Status returns the current migration status.
func (m *Migrator) Status(ctx context.Context) (*Status, error) {
	if err := m.ensureHistoryTable(ctx); err != nil {
		return nil, err
	}

	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	pending, err := m.Pending(ctx)
	if err != nil {
		return nil, err
	}

	var currentVersion uint64
	for _, am := range applied {
		if am.Version > currentVersion {
			currentVersion = am.Version
		}
	}

	return &Status{
		CurrentVersion: currentVersion,
		Applied:        applied,
		Pending:        pending,
	}, nil
}

// Version returns the current migration version (0 if none applied).
func (m *Migrator) Version(ctx context.Context) (uint64, error) {
	exists, err := m.historyTableExists(ctx)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	return m.getLatestVersion(ctx)
}

// Pending returns migrations that have not been applied yet.
func (m *Migrator) Pending(ctx context.Context) ([]*MigrationPair, error) {
	all, err := m.source.List()
	if err != nil {
		return nil, err
	}

	appliedVersions, err := m.getAppliedVersions(ctx)
	if err != nil {
		return nil, err
	}

	var pending []*MigrationPair
	for _, pair := range all {
		if !appliedVersions[pair.Version] {
			pending = append(pending, pair)
		}
	}

	return pending, nil
}

// Applied returns migrations that have been applied.
func (m *Migrator) Applied(ctx context.Context) ([]*AppliedMigration, error) {
	exists, err := m.historyTableExists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return m.getAppliedMigrations(ctx)
}

// Close releases resources.
func (m *Migrator) Close() error {
	if m.source != nil {
		return m.source.Close()
	}
	return nil
}

// applyUp applies a single up migration.
func (m *Migrator) applyUp(ctx context.Context, pair *MigrationPair) error {
	if !pair.HasUp() {
		return &MigrationError{
			Version:   pair.Version,
			Direction: Up,
			Err:       ErrMissingUp,
		}
	}

	m.log("Applying migration %d: %s", pair.Version, pair.Description)

	content, err := m.readMigrationContent(pair.Version, Up)
	if err != nil {
		return err
	}

	checksum := m.checksum(content)

	start := time.Now()
	if err := m.executeStatements(ctx, pair.Version, Up, content); err != nil {
		return err
	}
	duration := time.Since(start)

	if err := m.recordMigration(ctx, pair.Version, pair.Description, checksum, duration); err != nil {
		return err
	}

	m.log("Applied migration %d in %v", pair.Version, duration)

	return nil
}

// applyDown applies a single down migration.
func (m *Migrator) applyDown(ctx context.Context, version uint64) error {
	pairs, err := m.source.List()
	if err != nil {
		return err
	}

	var pair *MigrationPair
	for _, p := range pairs {
		if p.Version == version {
			pair = p
			break
		}
	}

	if pair == nil {
		return &MigrationError{
			Version:   version,
			Direction: Down,
			Err:       ErrVersionNotFound,
		}
	}

	if !pair.HasDown() {
		return &MigrationError{
			Version:   version,
			Direction: Down,
			Err:       ErrMissingDown,
		}
	}

	m.log("Rolling back migration %d: %s", pair.Version, pair.Description)

	content, err := m.readMigrationContent(version, Down)
	if err != nil {
		return err
	}

	start := time.Now()
	if err := m.executeStatements(ctx, version, Down, content); err != nil {
		return err
	}
	duration := time.Since(start)

	if err := m.removeMigration(ctx, version); err != nil {
		return err
	}

	m.log("Rolled back migration %d in %v", version, duration)

	return nil
}

// readMigrationContent reads the content of a migration file.
func (m *Migrator) readMigrationContent(version uint64, direction Direction) ([]byte, error) {
	var reader io.ReadCloser
	var err error

	switch direction {
	case Up:
		reader, err = m.source.ReadUp(version)
	case Down:
		reader, err = m.source.ReadDown(version)
	}

	if err != nil {
		return nil, err
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, &SourceError{Version: version, Op: "read", Err: err}
	}

	return content, nil
}

// executeStatements parses and executes CQL statements from migration content.
func (m *Migrator) executeStatements(ctx context.Context, version uint64, direction Direction, content []byte) error {
	statements := m.parseStatements(string(content))

	for i, stmt := range statements {
		if err := m.session.Query(stmt).WithContext(ctx).Consistency(m.consistency).Exec(); err != nil {
			return &MigrationError{
				Version:   version,
				Direction: direction,
				Statement: i + 1,
				Err:       err,
			}
		}
	}

	if m.waitForSchemaAgreement {
		if err := m.session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("failed to wait for schema agreement: %w", err)
		}
	}

	return nil
}

// parseStatements splits migration content into individual CQL statements.
// Statements are separated by semicolons. Lines starting with -- are comments.
func (m *Migrator) parseStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		current.WriteString(line)
		current.WriteString("\n")

		// Check if line ends with semicolon
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			// Remove trailing semicolon for gocql
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle case where last statement doesn't end with semicolon
	remaining := strings.TrimSpace(current.String())
	if remaining != "" {
		statements = append(statements, remaining)
	}

	return statements
}

// checksum calculates MD5 checksum of migration content.
func (m *Migrator) checksum(content []byte) string {
	hash := md5.Sum(content)
	return hex.EncodeToString(hash[:])
}

// log logs a message if a logger is configured.
func (m *Migrator) log(format string, v ...any) {
	if m.logger != nil {
		m.logger.Printf(format, v...)
	}
}
