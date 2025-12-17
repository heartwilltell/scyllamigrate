package scyllamigrate

import (
	"fmt"
)

// Error represents a custom error type.
type Error string

// Error returns the error message.
func (e Error) Error() string { return string(e) }

// Sentinel errors for common migration scenarios.
const (
	// ErrNoSource indicates no migration source was configured.
	ErrNoSource Error = "scyllamigrate: no migration source configured"

	// ErrNoChange indicates there are no migrations to apply.
	ErrNoChange Error = "scyllamigrate: no migrations to apply"

	// ErrMissingDown indicates a down migration is missing for rollback.
	ErrMissingDown Error = "scyllamigrate: down migration not found"

	// ErrMissingUp indicates an up migration is missing.
	ErrMissingUp Error = "scyllamigrate: up migration not found"

	// ErrMissingVersion indicates a migration version is missing.
	ErrMissingVersion Error = "scyllamigrate: migration version not found"

	// ErrChecksumMismatch indicates a migration file was modified after being applied.
	ErrChecksumMismatch Error = "scyllamigrate: migration file was modified after being applied"

	// ErrVersionNotFound indicates the requested migration version does not exist.
	ErrVersionNotFound Error = "scyllamigrate: migration version not found"

	// ErrNoKeyspace indicates no keyspace was configured.
	ErrNoKeyspace Error = "scyllamigrate: no keyspace configured"

	// ErrNoSession indicates no database session was provided.
	ErrNoSession Error = "scyllamigrate: no database session provided"
)

// ParseError indicates a migration filename could not be parsed.
type ParseError struct {
	Filename string
	Err      error
}

// Error implements the error interface.
func (e *ParseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("scyllamigrate: failed to parse migration filename %q: %v", e.Filename, e.Err)
	}

	return fmt.Sprintf("scyllamigrate: failed to parse migration filename %q", e.Filename)
}

// Unwrap returns the underlying error.
func (e *ParseError) Unwrap() error { return e.Err }

// MigrationError wraps an error that occurred during migration execution.
type MigrationError struct {
	Version   uint64
	Direction Direction
	Statement int
	Err       error
}

// Error implements the error interface.
func (e *MigrationError) Error() string {
	if e.Statement > 0 {
		return fmt.Sprintf("scyllamigrate: failed to execute %s migration %d (statement %d): %v",
			e.Direction, e.Version, e.Statement, e.Err,
		)
	}

	return fmt.Sprintf("scyllamigrate: failed to execute %s migration %d: %v",
		e.Direction, e.Version, e.Err,
	)
}

// Unwrap returns the underlying error.
func (e *MigrationError) Unwrap() error { return e.Err }

// SourceError wraps an error that occurred while reading from a migration source.
type SourceError struct {
	Version uint64
	Op      string
	Err     error
}

// Error implements the error interface.
func (e *SourceError) Error() string {
	return fmt.Sprintf("scyllamigrate: source error for version %d (%s): %v", e.Version, e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *SourceError) Unwrap() error { return e.Err }

// KeyspaceError wraps an error that occurred during keyspace operations.
type KeyspaceError struct {
	Keyspace string
	Op       string
	Err      error
}

// Error implements the error interface.
func (e *KeyspaceError) Error() string {
	return fmt.Sprintf("scyllamigrate: keyspace error for %q (%s): %v", e.Keyspace, e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *KeyspaceError) Unwrap() error { return e.Err }
