package scyllamigrate

import (
	"context"
	"fmt"
	"time"
)

const historySchemaTemplate = `
CREATE TABLE IF NOT EXISTS %s.%s (
    version bigint,
    description text,
    checksum text,
    applied_at timestamp,
    execution_ms bigint,
    PRIMARY KEY (version)
)`

// ensureHistoryTable creates the migration history table if it doesn't exist.
func (m *Migrator) ensureHistoryTable(ctx context.Context) error {
	query := fmt.Sprintf(historySchemaTemplate, m.keyspace, m.historyTable)

	if err := m.session.Query(query).WithContext(ctx).Consistency(m.consistency).Exec(); err != nil {
		return fmt.Errorf("failed to create history table: %w", err)
	}

	if m.waitForSchemaAgreement {
		if err := m.session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("failed to wait for schema agreement: %w", err)
		}
	}

	return nil
}

// recordMigration records a successfully applied migration to the history table.
func (m *Migrator) recordMigration(ctx context.Context, version uint64, description, checksum string, duration time.Duration) error {
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (version, description, checksum, applied_at, execution_ms) VALUES (?, ?, ?, ?, ?)",
		m.keyspace, m.historyTable,
	)

	err := m.session.Query(query,
		version,
		description,
		checksum,
		time.Now(),
		duration.Milliseconds(),
	).WithContext(ctx).Consistency(m.consistency).Exec()

	if err != nil {
		return fmt.Errorf("failed to record migration %d: %w", version, err)
	}

	return nil
}

// removeMigration removes a migration record from the history table (for rollbacks).
func (m *Migrator) removeMigration(ctx context.Context, version uint64) error {
	query := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE version = ?",
		m.keyspace, m.historyTable,
	)

	err := m.session.Query(query, version).WithContext(ctx).Consistency(m.consistency).Exec()
	if err != nil {
		return fmt.Errorf("failed to remove migration record %d: %w", version, err)
	}

	return nil
}

// getAppliedMigrations returns all applied migrations from the history table.
func (m *Migrator) getAppliedMigrations(ctx context.Context) ([]*AppliedMigration, error) {
	query := fmt.Sprintf(
		"SELECT version, description, checksum, applied_at, execution_ms FROM %s.%s",
		m.keyspace, m.historyTable,
	)

	iter := m.session.Query(query).WithContext(ctx).Consistency(m.consistency).Iter()

	var migrations []*AppliedMigration
	var version uint64
	var description, checksum string
	var appliedAt time.Time
	var executionMs int64

	for iter.Scan(&version, &description, &checksum, &appliedAt, &executionMs) {
		migrations = append(migrations, &AppliedMigration{
			Version:     version,
			Description: description,
			Checksum:    checksum,
			AppliedAt:   appliedAt,
			ExecutionMs: executionMs,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to read applied migrations: %w", err)
	}

	return migrations, nil
}

// getLatestVersion returns the highest applied migration version.
// Returns 0 if no migrations have been applied.
func (m *Migrator) getLatestVersion(ctx context.Context) (uint64, error) {
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return 0, err
	}

	var maxVersion uint64
	for _, am := range applied {
		if am.Version > maxVersion {
			maxVersion = am.Version
		}
	}

	return maxVersion, nil
}

// getAppliedVersions returns a set of all applied migration versions.
func (m *Migrator) getAppliedVersions(ctx context.Context) (map[uint64]bool, error) {
	applied, err := m.getAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	versions := make(map[uint64]bool, len(applied))
	for _, am := range applied {
		versions[am.Version] = true
	}

	return versions, nil
}

// historyTableExists checks if the history table exists.
func (m *Migrator) historyTableExists(ctx context.Context) (bool, error) {
	query := `
		SELECT table_name
		FROM system_schema.tables
		WHERE keyspace_name = ? AND table_name = ?
	`

	var tableName string
	err := m.session.Query(query, m.keyspace, m.historyTable).
		WithContext(ctx).
		Consistency(m.consistency).
		Scan(&tableName)

	if err != nil {
		// Table doesn't exist
		return false, nil
	}

	return true, nil
}
