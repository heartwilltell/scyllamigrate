package scyllamigrate

import (
	"errors"
	"fmt"
	"testing"
)

func TestError_Error(t *testing.T) {
	type tcase struct {
		err      Error
		expected string
	}
	tests := map[string]tcase{
		"ErrNoSource": {
			err:      ErrNoSource,
			expected: "scyllamigrate: no migration source configured",
		},
		"ErrNoChange": {
			err:      ErrNoChange,
			expected: "scyllamigrate: no migrations to apply",
		},
		"ErrMissingDown": {
			err:      ErrMissingDown,
			expected: "scyllamigrate: down migration not found",
		},
		"ErrMissingUp": {
			err:      ErrMissingUp,
			expected: "scyllamigrate: up migration not found",
		},
		"ErrMissingVersion": {
			err:      ErrMissingVersion,
			expected: "scyllamigrate: migration version not found",
		},
		"ErrChecksumMismatch": {
			err:      ErrChecksumMismatch,
			expected: "scyllamigrate: migration file was modified after being applied",
		},
		"ErrVersionNotFound": {
			err:      ErrVersionNotFound,
			expected: "scyllamigrate: migration version not found",
		},
		"ErrNoKeyspace": {
			err:      ErrNoKeyspace,
			expected: "scyllamigrate: no keyspace configured",
		},
		"ErrNoSession": {
			err:      ErrNoSession,
			expected: "scyllamigrate: no database session provided",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestError_Is(t *testing.T) {
	type tcase struct {
		err      error
		target   error
		expected bool
	}
	tests := map[string]tcase{
		"ErrNoSource matches itself": {
			err:      ErrNoSource,
			target:   ErrNoSource,
			expected: true,
		},
		"ErrNoChange matches itself": {
			err:      ErrNoChange,
			target:   ErrNoChange,
			expected: true,
		},
		"ErrMissingDown matches itself": {
			err:      ErrMissingDown,
			target:   ErrMissingDown,
			expected: true,
		},
		"ErrMissingUp matches itself": {
			err:      ErrMissingUp,
			target:   ErrMissingUp,
			expected: true,
		},
		"Different errors don't match": {
			err:      ErrNoSource,
			target:   ErrNoChange,
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := errors.Is(tt.err, tt.target); got != tt.expected {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", tt.err, tt.target, got, tt.expected)
			}
		})
	}
}

func TestParseError_Error(t *testing.T) {
	type tcase struct {
		err      *ParseError
		expected string
	}
	tests := map[string]tcase{
		"with underlying error": {
			err: &ParseError{
				Filename: "invalid_file.cql",
				Err:      fmt.Errorf("invalid format"),
			},
			expected: `scyllamigrate: failed to parse migration filename "invalid_file.cql": invalid format`,
		},
		"without underlying error": {
			err: &ParseError{
				Filename: "invalid_file.cql",
				Err:      nil,
			},
			expected: `scyllamigrate: failed to parse migration filename "invalid_file.cql"`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseError_Unwrap(t *testing.T) {
	underlyingErr := fmt.Errorf("invalid format")
	err := &ParseError{
		Filename: "invalid_file.cql",
		Err:      underlyingErr,
	}

	if got := err.Unwrap(); got != underlyingErr {
		t.Errorf("Unwrap() = %v, want %v", got, underlyingErr)
	}

	errNoUnderlying := &ParseError{
		Filename: "invalid_file.cql",
		Err:      nil,
	}

	if got := errNoUnderlying.Unwrap(); got != nil {
		t.Errorf("Unwrap() = %v, want nil", got)
	}
}

func TestMigrationError_Error(t *testing.T) {
	type tcase struct {
		err      *MigrationError
		expected string
	}
	tests := map[string]tcase{
		"with statement number": {
			err: &MigrationError{
				Version:   1,
				Direction: Up,
				Statement: 2,
				Err:       fmt.Errorf("syntax error"),
			},
			expected: "scyllamigrate: failed to execute up migration 1 (statement 2): syntax error",
		},
		"without statement number": {
			err: &MigrationError{
				Version:   1,
				Direction: Up,
				Statement: 0,
				Err:       fmt.Errorf("syntax error"),
			},
			expected: "scyllamigrate: failed to execute up migration 1: syntax error",
		},
		"down migration": {
			err: &MigrationError{
				Version:   5,
				Direction: Down,
				Statement: 0,
				Err:       fmt.Errorf("table not found"),
			},
			expected: "scyllamigrate: failed to execute down migration 5: table not found",
		},
		"down migration with statement": {
			err: &MigrationError{
				Version:   5,
				Direction: Down,
				Statement: 3,
				Err:       fmt.Errorf("table not found"),
			},
			expected: "scyllamigrate: failed to execute down migration 5 (statement 3): table not found",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestMigrationError_Unwrap(t *testing.T) {
	underlyingErr := fmt.Errorf("syntax error")
	err := &MigrationError{
		Version:   1,
		Direction: Up,
		Statement: 0,
		Err:       underlyingErr,
	}

	if got := err.Unwrap(); got != underlyingErr {
		t.Errorf("Unwrap() = %v, want %v", got, underlyingErr)
	}
}

func TestMigrationError_Is(t *testing.T) {
	underlyingErr := ErrMissingUp
	err := &MigrationError{
		Version:   1,
		Direction: Up,
		Statement: 0,
		Err:       underlyingErr,
	}

	// Should match the wrapped error
	if !errors.Is(err, ErrMissingUp) {
		t.Error("errors.Is() should match wrapped ErrMissingUp")
	}

	// Should not match unrelated errors
	if errors.Is(err, ErrNoChange) {
		t.Error("errors.Is() should not match unrelated error")
	}
}

func TestSourceError_Error(t *testing.T) {
	type tcase struct {
		err      *SourceError
		expected string
	}
	tests := map[string]tcase{
		"read operation": {
			err: &SourceError{
				Version: 1,
				Op:      "read",
				Err:     fmt.Errorf("file not found"),
			},
			expected: "scyllamigrate: source error for version 1 (read): file not found",
		},
		"scan operation": {
			err: &SourceError{
				Version: 5,
				Op:      "scan",
				Err:     fmt.Errorf("permission denied"),
			},
			expected: "scyllamigrate: source error for version 5 (scan): permission denied",
		},
		"read up operation": {
			err: &SourceError{
				Version: 2,
				Op:      "read up",
				Err:     ErrVersionNotFound,
			},
			expected: "scyllamigrate: source error for version 2 (read up): scyllamigrate: migration version not found",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestSourceError_Unwrap(t *testing.T) {
	underlyingErr := fmt.Errorf("file not found")
	err := &SourceError{
		Version: 1,
		Op:      "read",
		Err:     underlyingErr,
	}

	if got := err.Unwrap(); got != underlyingErr {
		t.Errorf("Unwrap() = %v, want %v", got, underlyingErr)
	}
}

func TestSourceError_Is(t *testing.T) {
	underlyingErr := ErrVersionNotFound
	err := &SourceError{
		Version: 1,
		Op:      "read up",
		Err:     underlyingErr,
	}

	// Should match the wrapped error
	if !errors.Is(err, ErrVersionNotFound) {
		t.Error("errors.Is() should match wrapped ErrVersionNotFound")
	}

	// Should not match unrelated errors
	if errors.Is(err, ErrNoChange) {
		t.Error("errors.Is() should not match unrelated error")
	}
}

func TestNestedErrorUnwrapping(t *testing.T) {
	// Test that errors.Is works with nested wrapped errors
	originalErr := fmt.Errorf("original error")
	sourceErr := &SourceError{
		Version: 1,
		Op:      "read",
		Err:     originalErr,
	}
	migrationErr := &MigrationError{
		Version:   1,
		Direction: Up,
		Statement: 0,
		Err:       sourceErr,
	}

	// Should be able to unwrap through multiple layers
	if !errors.Is(migrationErr, originalErr) {
		t.Error("errors.Is() should match deeply nested error")
	}

	if !errors.Is(migrationErr, sourceErr) {
		t.Error("errors.Is() should match intermediate wrapped error")
	}
}
