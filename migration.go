package scyllamigrate

import (
	"regexp"
	"strconv"
	"time"
)

// Direction represents the migration direction (up or down).
type Direction string

const (
	// Up represents a forward migration.
	Up Direction = "up"
	// Down represents a rollback migration.
	Down Direction = "down"
)

// String returns the string representation of the direction.
func (d Direction) String() string {
	return string(d)
}

// Migration represents a single migration file.
type Migration struct {
	// Version is the sequential version number (e.g., 1, 2, 3).
	Version uint64

	// Description is the human-readable description from the filename.
	Description string

	// Direction indicates whether this is an up or down migration.
	Direction Direction

	// Raw is the original filename.
	Raw string
}

// MigrationPair holds both up and down migrations for a version.
type MigrationPair struct {
	// Version is the sequential version number.
	Version uint64

	// Description is the human-readable description.
	Description string

	// Up is the forward migration (may be nil if not found).
	Up *Migration

	// Down is the rollback migration (may be nil if not found).
	Down *Migration
}

// HasUp returns true if an up migration exists.
func (p *MigrationPair) HasUp() bool {
	return p.Up != nil
}

// HasDown returns true if a down migration exists.
func (p *MigrationPair) HasDown() bool {
	return p.Down != nil
}

// AppliedMigration represents a migration that has been applied to the database.
type AppliedMigration struct {
	// Version is the sequential version number.
	Version uint64

	// Description is the human-readable description.
	Description string

	// Checksum is the MD5 hash of the migration content.
	Checksum string

	// AppliedAt is when the migration was applied.
	AppliedAt time.Time

	// ExecutionMs is how long the migration took to execute in milliseconds.
	ExecutionMs int64
}

// Status represents the current migration status.
type Status struct {
	// CurrentVersion is the latest applied migration version (0 if none).
	CurrentVersion uint64

	// Applied is the list of applied migrations.
	Applied []*AppliedMigration

	// Pending is the list of migrations that have not been applied yet.
	Pending []*MigrationPair
}

// migrationRegex matches migration filenames.
// Pattern: {version}_{description}.{direction}.{extension}
// Examples:
//   - 000001_create_users.up.cql
//   - 000001_create_users.down.sql
//   - 1_initial.up.cql
var migrationRegex = regexp.MustCompile(`^([0-9]+)_(.+)\.(up|down)\.(cql|sql)$`)

// ParseMigration parses a filename into a Migration struct.
// Returns an error if the filename does not match the expected pattern.
func ParseMigration(filename string) (*Migration, error) {
	matches := migrationRegex.FindStringSubmatch(filename)
	if len(matches) != 5 {
		return nil, &ParseError{Filename: filename}
	}

	version, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return nil, &ParseError{Filename: filename, Err: err}
	}

	return &Migration{
		Version:     version,
		Description: matches[2],
		Direction:   Direction(matches[3]),
		Raw:         filename,
	}, nil
}

// IsMigrationFile checks if a filename matches the migration file pattern.
func IsMigrationFile(filename string) bool {
	return migrationRegex.MatchString(filename)
}
