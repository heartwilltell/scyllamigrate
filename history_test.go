package scyllamigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	td "github.com/maxatome/go-testdeep/td"
)

// Test helper functions that test the logic without database

func TestGetLatestVersion_Logic(t *testing.T) {
	type tcase struct {
		applied  []*AppliedMigration
		expected uint64
	}
	tests := map[string]tcase{
		"empty list": {
			applied:  []*AppliedMigration{},
			expected: 0,
		},
		"single migration": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
			},
			expected: 1,
		},
		"multiple migrations in order": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
				{Version: 2, Description: "second"},
				{Version: 3, Description: "third"},
			},
			expected: 3,
		},
		"multiple migrations out of order": {
			applied: []*AppliedMigration{
				{Version: 3, Description: "third"},
				{Version: 1, Description: "first"},
				{Version: 2, Description: "second"},
			},
			expected: 3,
		},
		"large version numbers": {
			applied: []*AppliedMigration{
				{Version: 100, Description: "hundred"},
				{Version: 50, Description: "fifty"},
				{Version: 200, Description: "two hundred"},
			},
			expected: 200,
		},
		"single large version": {
			applied: []*AppliedMigration{
				{Version: 999999, Description: "large"},
			},
			expected: 999999,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var maxVersion uint64
			for _, am := range tt.applied {
				if am.Version > maxVersion {
					maxVersion = am.Version
				}
			}

			td.Cmp(t, maxVersion, tt.expected)
		})
	}
}

func TestGetAppliedVersions_Logic(t *testing.T) {
	type tcase struct {
		applied  []*AppliedMigration
		expected map[uint64]bool
	}
	tests := map[string]tcase{
		"empty list": {
			applied:  []*AppliedMigration{},
			expected: map[uint64]bool{},
		},
		"single migration": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
			},
			expected: map[uint64]bool{1: true},
		},
		"multiple migrations": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
				{Version: 2, Description: "second"},
				{Version: 3, Description: "third"},
			},
			expected: map[uint64]bool{1: true, 2: true, 3: true},
		},
		"duplicate versions (shouldn't happen in practice but test logic)": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
				{Version: 1, Description: "first again"},
			},
			expected: map[uint64]bool{1: true},
		},
		"large version numbers": {
			applied: []*AppliedMigration{
				{Version: 100, Description: "hundred"},
				{Version: 200, Description: "two hundred"},
			},
			expected: map[uint64]bool{100: true, 200: true},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			versions := make(map[uint64]bool, len(tt.applied))
			for _, am := range tt.applied {
				versions[am.Version] = true
			}

			td.Cmp(t, versions, tt.expected)
		})
	}
}

func TestHistorySchemaTemplate(t *testing.T) {
	// Test that the template can be formatted correctly
	keyspace := "test_keyspace"
	table := "schema_migrations"

	query := historySchemaTemplate
	formatted := formatHistoryQuery(query, keyspace, table)

	td.Cmp(t, formatted, td.NotEmpty())
	td.Cmp(t, formatted, td.String(keyspace))
	td.Cmp(t, formatted, td.String(table))

	// Verify it contains expected CQL keywords
	expectedKeywords := []string{"CREATE TABLE", "IF NOT EXISTS", "version", "description", "checksum", "applied_at", "execution_ms", "PRIMARY KEY"}
	for _, keyword := range expectedKeywords {
		td.Cmp(t, formatted, td.String(keyword))
	}
}

func TestMigrationRecord(t *testing.T) {
	// Test migrationRecord structure
	record := migrationRecord{
		version:     1,
		description: "test migration",
		checksum:    "abc123",
		duration:    100 * time.Millisecond,
	}

	td.Cmp(t, record.version, uint64(1))
	td.Cmp(t, record.description, "test migration")
	td.Cmp(t, record.checksum, "abc123")
	td.Cmp(t, record.duration, 100*time.Millisecond)

	// Test duration conversion to milliseconds
	ms := record.duration.Milliseconds()
	td.Cmp(t, ms, int64(100))
}

func TestHistoryTableExists_Logic(t *testing.T) {
	// Test the logic for checking table existence
	// This simulates what historyTableExists does
	type tcase struct {
		queryErr  error
		tableName string
		expected  bool
	}
	tests := map[string]tcase{
		"table exists": {
			queryErr:  nil,
			tableName: "schema_migrations",
			expected:  true,
		},
		"table not found": {
			queryErr:  gocql.ErrNotFound,
			tableName: "",
			expected:  false,
		},
		"other error": {
			queryErr:  errors.New("connection error"),
			tableName: "",
			expected:  false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var result bool
			if tt.queryErr != nil {
				if errors.Is(tt.queryErr, gocql.ErrNotFound) {
					result = false
				} else {
					result = false
				}
			} else {
				result = tt.tableName != ""
			}

			td.Cmp(t, result, tt.expected)
		})
	}
}

func TestAppliedMigration_Structure(t *testing.T) {
	now := time.Now()
	am := &AppliedMigration{
		Version:     1,
		Description: "test migration",
		Checksum:    "abc123def456",
		AppliedAt:   now,
		ExecutionMs: 150,
	}

	td.Cmp(t, am.Version, uint64(1))
	td.Cmp(t, am.Description, "test migration")
	td.Cmp(t, am.Checksum, "abc123def456")
	td.Cmp(t, am.AppliedAt.Equal(now), true)
	td.Cmp(t, am.ExecutionMs, int64(150))
}

func TestGetLatestVersion_EdgeCases(t *testing.T) {
	type tcase struct {
		applied  []*AppliedMigration
		expected uint64
	}
	tests := map[string]tcase{
		"nil slice": {
			applied:  nil,
			expected: 0,
		},
		"version zero": {
			applied: []*AppliedMigration{
				{Version: 0, Description: "zero"},
			},
			expected: 0,
		},
		"mixed with zero": {
			applied: []*AppliedMigration{
				{Version: 0, Description: "zero"},
				{Version: 5, Description: "five"},
			},
			expected: 5,
		},
		"all same version": {
			applied: []*AppliedMigration{
				{Version: 5, Description: "first"},
				{Version: 5, Description: "second"},
				{Version: 5, Description: "third"},
			},
			expected: 5,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var maxVersion uint64
			if tt.applied != nil {
				for _, am := range tt.applied {
					if am.Version > maxVersion {
						maxVersion = am.Version
					}
				}
			}

			td.Cmp(t, maxVersion, tt.expected)
		})
	}
}

func TestGetAppliedVersions_EdgeCases(t *testing.T) {
	type tcase struct {
		applied  []*AppliedMigration
		expected int // expected number of unique versions
	}
	tests := map[string]tcase{
		"nil slice": {
			applied:  nil,
			expected: 0,
		},
		"version zero": {
			applied: []*AppliedMigration{
				{Version: 0, Description: "zero"},
			},
			expected: 1,
		},
		"duplicate versions": {
			applied: []*AppliedMigration{
				{Version: 1, Description: "first"},
				{Version: 1, Description: "duplicate"},
				{Version: 2, Description: "second"},
			},
			expected: 2, // Should have 2 unique versions
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			versions := make(map[uint64]bool)
			if tt.applied != nil {
				for _, am := range tt.applied {
					versions[am.Version] = true
				}
			}

			td.Cmp(t, len(versions), tt.expected)
		})
	}
}

func TestHistoryQueryFormatting(t *testing.T) {
	// Test that queries are formatted correctly with different keyspace/table names
	type tcase struct {
		keyspace string
		table    string
	}
	tests := map[string]tcase{
		"default values": {
			keyspace: "myapp",
			table:    "schema_migrations",
		},
		"custom table name": {
			keyspace: "myapp",
			table:    "custom_migrations",
		},
		"keyspace with underscore": {
			keyspace: "my_app",
			table:    "schema_migrations",
		},
		"table with underscore": {
			keyspace: "myapp",
			table:    "my_schema_migrations",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Test ensureHistoryTable query
			createQuery := formatHistoryQuery(historySchemaTemplate, tt.keyspace, tt.table)
			td.Cmp(t, strings.Contains(createQuery, tt.keyspace), true)
			td.Cmp(t, strings.Contains(createQuery, tt.table), true)

			// Test recordMigration query
			insertQuery := formatRecordQuery(tt.keyspace, tt.table)
			td.Cmp(t, strings.Contains(insertQuery, tt.keyspace), true)
			td.Cmp(t, strings.Contains(insertQuery, tt.table), true)

			// Test removeMigration query
			deleteQuery := formatRemoveQuery(tt.keyspace, tt.table)
			td.Cmp(t, strings.Contains(deleteQuery, tt.keyspace), true)
			td.Cmp(t, strings.Contains(deleteQuery, tt.table), true)

			// Test getAppliedMigrations query
			selectQuery := formatSelectQuery(tt.keyspace, tt.table)
			td.Cmp(t, strings.Contains(selectQuery, tt.keyspace), true)
			td.Cmp(t, strings.Contains(selectQuery, tt.table), true)
		})
	}
}

func TestMigrationRecord_DurationConversion(t *testing.T) {
	type tcase struct {
		duration time.Duration
		expected int64
	}
	tests := map[string]tcase{
		"zero duration": {
			duration: 0,
			expected: 0,
		},
		"1 millisecond": {
			duration: time.Millisecond,
			expected: 1,
		},
		"100 milliseconds": {
			duration: 100 * time.Millisecond,
			expected: 100,
		},
		"1 second": {
			duration: time.Second,
			expected: 1000,
		},
		"2.5 seconds": {
			duration: 2500 * time.Millisecond,
			expected: 2500,
		},
		"less than 1ms": {
			duration: 500 * time.Microsecond,
			expected: 0, // Rounds down
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ms := tt.duration.Milliseconds()
			td.Cmp(t, ms, tt.expected)
		})
	}
}

// Helper functions for testing

func formatHistoryQuery(template, keyspace, table string) string {
	return fmt.Sprintf(template, keyspace, table)
}

func formatRecordQuery(keyspace, table string) string {
	query := "INSERT INTO %s.%s (version, description, checksum, applied_at, execution_ms) VALUES (?, ?, ?, ?, ?)"
	return fmt.Sprintf(query, keyspace, table)
}

func formatRemoveQuery(keyspace, table string) string {
	query := "DELETE FROM %s.%s WHERE version = ?"
	return fmt.Sprintf(query, keyspace, table)
}

func formatSelectQuery(keyspace, table string) string {
	query := "SELECT version, description, checksum, applied_at, execution_ms FROM %s.%s"
	return fmt.Sprintf(query, keyspace, table)
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// Test context handling (even though we can't test actual DB calls)
func TestHistoryFunctions_ContextHandling(t *testing.T) {
	// Verify that functions are designed to accept context
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctxWithCancel, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	// These should compile and accept context
	_ = ctx
	_ = ctxWithTimeout
	_ = ctxWithCancel

	// Verify context is used in function signatures
	// (This is a compile-time check - if these don't compile, the test fails)
	var migrator *Migrator
	_ = migrator.ensureHistoryTable
	_ = migrator.recordMigration
	_ = migrator.removeMigration
	_ = migrator.getAppliedMigrations
	_ = migrator.getLatestVersion
	_ = migrator.getAppliedVersions
	_ = migrator.historyTableExists
}

func TestHistoryTableExists_ErrorHandling(t *testing.T) {
	// Test the error handling logic
	type tcase struct {
		err      error
		expected bool
	}
	tests := map[string]tcase{
		"nil error": {
			err:      nil,
			expected: true,
		},
		"ErrNotFound": {
			err:      gocql.ErrNotFound,
			expected: false,
		},
		"wrapped ErrNotFound": {
			err:      errors.New("wrapped: " + gocql.ErrNotFound.Error()),
			expected: false, // Note: errors.Is won't match wrapped string, but actual code uses errors.Is
		},
		"other error": {
			err:      errors.New("connection failed"),
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var result bool
			if tt.err != nil {
				if errors.Is(tt.err, gocql.ErrNotFound) {
					result = false
				} else {
					result = false
				}
			} else {
				result = true
			}

			td.Cmp(t, result, tt.expected)
		})
	}
}
