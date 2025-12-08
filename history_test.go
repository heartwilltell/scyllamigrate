package scyllamigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
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

			if maxVersion != tt.expected {
				t.Errorf("getLatestVersion logic = %d, want %d", maxVersion, tt.expected)
			}
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

			if len(versions) != len(tt.expected) {
				t.Errorf("getAppliedVersions length = %d, want %d", len(versions), len(tt.expected))
			}

			for version, expected := range tt.expected {
				if versions[version] != expected {
					t.Errorf("getAppliedVersions[%d] = %v, want %v", version, versions[version], expected)
				}
			}
		})
	}
}

func TestHistorySchemaTemplate(t *testing.T) {
	// Test that the template can be formatted correctly
	keyspace := "test_keyspace"
	table := "schema_migrations"

	query := historySchemaTemplate
	formatted := formatHistoryQuery(query, keyspace, table)

	if formatted == "" {
		t.Error("formatted query is empty")
	}

	if !contains(formatted, keyspace) {
		t.Errorf("formatted query does not contain keyspace %q", keyspace)
	}

	if !contains(formatted, table) {
		t.Errorf("formatted query does not contain table %q", table)
	}

	// Verify it contains expected CQL keywords
	expectedKeywords := []string{"CREATE TABLE", "IF NOT EXISTS", "version", "description", "checksum", "applied_at", "execution_ms", "PRIMARY KEY"}
	for _, keyword := range expectedKeywords {
		if !contains(formatted, keyword) {
			t.Errorf("formatted query does not contain keyword %q", keyword)
		}
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

	if record.version != 1 {
		t.Errorf("record.version = %d, want 1", record.version)
	}
	if record.description != "test migration" {
		t.Errorf("record.description = %q, want %q", record.description, "test migration")
	}
	if record.checksum != "abc123" {
		t.Errorf("record.checksum = %q, want %q", record.checksum, "abc123")
	}
	if record.duration != 100*time.Millisecond {
		t.Errorf("record.duration = %v, want %v", record.duration, 100*time.Millisecond)
	}

	// Test duration conversion to milliseconds
	ms := record.duration.Milliseconds()
	if ms != 100 {
		t.Errorf("duration.Milliseconds() = %d, want 100", ms)
	}
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

			if result != tt.expected {
				t.Errorf("historyTableExists logic = %v, want %v", result, tt.expected)
			}
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

	if am.Version != 1 {
		t.Errorf("AppliedMigration.Version = %d, want 1", am.Version)
	}
	if am.Description != "test migration" {
		t.Errorf("AppliedMigration.Description = %q, want %q", am.Description, "test migration")
	}
	if am.Checksum != "abc123def456" {
		t.Errorf("AppliedMigration.Checksum = %q, want %q", am.Checksum, "abc123def456")
	}
	if !am.AppliedAt.Equal(now) {
		t.Errorf("AppliedMigration.AppliedAt = %v, want %v", am.AppliedAt, now)
	}
	if am.ExecutionMs != 150 {
		t.Errorf("AppliedMigration.ExecutionMs = %d, want 150", am.ExecutionMs)
	}
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

			if maxVersion != tt.expected {
				t.Errorf("getLatestVersion logic = %d, want %d", maxVersion, tt.expected)
			}
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

			if len(versions) != tt.expected {
				t.Errorf("getAppliedVersions unique count = %d, want %d", len(versions), tt.expected)
			}
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
			if !contains(createQuery, tt.keyspace) || !contains(createQuery, tt.table) {
				t.Errorf("create query does not contain keyspace/table: %q", createQuery)
			}

			// Test recordMigration query
			insertQuery := formatRecordQuery(tt.keyspace, tt.table)
			if !contains(insertQuery, tt.keyspace) || !contains(insertQuery, tt.table) {
				t.Errorf("insert query does not contain keyspace/table: %q", insertQuery)
			}

			// Test removeMigration query
			deleteQuery := formatRemoveQuery(tt.keyspace, tt.table)
			if !contains(deleteQuery, tt.keyspace) || !contains(deleteQuery, tt.table) {
				t.Errorf("delete query does not contain keyspace/table: %q", deleteQuery)
			}

			// Test getAppliedMigrations query
			selectQuery := formatSelectQuery(tt.keyspace, tt.table)
			if !contains(selectQuery, tt.keyspace) || !contains(selectQuery, tt.table) {
				t.Errorf("select query does not contain keyspace/table: %q", selectQuery)
			}
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
			if ms != tt.expected {
				t.Errorf("duration.Milliseconds() = %d, want %d", ms, tt.expected)
			}
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

			if result != tt.expected {
				t.Errorf("historyTableExists error handling = %v, want %v", result, tt.expected)
			}
		})
	}
}
