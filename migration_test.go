package scyllamigrate

import (
	"testing"
)

func TestDirection_String(t *testing.T) {
	type tcase struct {
		d        Direction
		expected string
	}
	tests := map[string]tcase{
		"Up direction": {
			d:        Up,
			expected: "up",
		},
		"Down direction": {
			d:        Down,
			expected: "down",
		},
		"Empty direction": {
			d:        Direction(""),
			expected: "",
		},
		"Custom direction": {
			d:        Direction("custom"),
			expected: "custom",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.d.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseMigration(t *testing.T) {
	type tcase struct {
		filename  string
		wantErr   bool
		checkFunc func(*testing.T, *Migration, error)
	}
	tests := map[string]tcase{
		"valid up migration with cql extension": {
			filename: "000001_create_users.up.cql",
			wantErr:  false,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err != nil {
					t.Fatalf("ParseMigration() error = %v, want nil", err)
				}
				if m.Version != 1 {
					t.Errorf("Version = %d, want 1", m.Version)
				}
				if m.Description != "create_users" {
					t.Errorf("Description = %q, want %q", m.Description, "create_users")
				}
				if m.Direction != Up {
					t.Errorf("Direction = %q, want %q", m.Direction, Up)
				}
				if m.Raw != "000001_create_users.up.cql" {
					t.Errorf("Raw = %q, want %q", m.Raw, "000001_create_users.up.cql")
				}
			},
		},
		"valid down migration with sql extension": {
			filename: "000001_create_users.down.sql",
			wantErr:  false,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err != nil {
					t.Fatalf("ParseMigration() error = %v, want nil", err)
				}
				if m.Version != 1 {
					t.Errorf("Version = %d, want 1", m.Version)
				}
				if m.Description != "create_users" {
					t.Errorf("Description = %q, want %q", m.Description, "create_users")
				}
				if m.Direction != Down {
					t.Errorf("Direction = %q, want %q", m.Direction, Down)
				}
			},
		},
		"valid migration with multi-word description": {
			filename: "000042_add_user_profile_table.up.cql",
			wantErr:  false,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err != nil {
					t.Fatalf("ParseMigration() error = %v, want nil", err)
				}
				if m.Version != 42 {
					t.Errorf("Version = %d, want 42", m.Version)
				}
				if m.Description != "add_user_profile_table" {
					t.Errorf("Description = %q, want %q", m.Description, "add_user_profile_table")
				}
			},
		},
		"valid migration with single digit version": {
			filename: "1_initial.up.cql",
			wantErr:  false,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err != nil {
					t.Fatalf("ParseMigration() error = %v, want nil", err)
				}
				if m.Version != 1 {
					t.Errorf("Version = %d, want 1", m.Version)
				}
			},
		},
		"valid migration with large version number": {
			filename: "999999_final_migration.up.cql",
			wantErr:  false,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err != nil {
					t.Fatalf("ParseMigration() error = %v, want nil", err)
				}
				if m.Version != 999999 {
					t.Errorf("Version = %d, want 999999", m.Version)
				}
			},
		},
		"invalid - missing version": {
			filename: "_create_users.up.cql",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
				if _, ok := err.(*ParseError); !ok {
					t.Errorf("error type = %T, want *ParseError", err)
				}
			},
		},
		"invalid - missing description": {
			filename: "000001.up.cql",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		"invalid - wrong direction": {
			filename: "000001_create_users.sideways.cql",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		"invalid - wrong extension": {
			filename: "000001_create_users.up.txt",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		"invalid - missing extension": {
			filename: "000001_create_users.up",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		"invalid - empty filename": {
			filename: "",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		"invalid - no underscores": {
			filename: "000001createusers.up.cql",
			wantErr:  true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseMigration(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMigration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, got, err)
			}
		})
	}
}

func TestIsMigrationFile(t *testing.T) {
	type tcase struct {
		filename string
		want     bool
	}
	tests := map[string]tcase{
		"valid up migration cql": {
			filename: "000001_create_users.up.cql",
			want:     true,
		},
		"valid down migration sql": {
			filename: "000001_create_users.down.sql",
			want:     true,
		},
		"valid single digit version": {
			filename: "1_initial.up.cql",
			want:     true,
		},
		"invalid - missing version": {
			filename: "_create_users.up.cql",
			want:     false,
		},
		"invalid - wrong extension": {
			filename: "000001_create_users.up.txt",
			want:     false,
		},
		"invalid - wrong direction": {
			filename: "000001_create_users.sideways.cql",
			want:     false,
		},
		"invalid - empty filename": {
			filename: "",
			want:     false,
		},
		"invalid - regular file": {
			filename: "README.md",
			want:     false,
		},
		"invalid - no extension": {
			filename: "000001_create_users.up",
			want:     false,
		},
		"valid with underscores in description": {
			filename: "000001_add_user_profile_table.up.cql",
			want:     true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsMigrationFile(tt.filename); got != tt.want {
				t.Errorf("IsMigrationFile(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

func TestMigrationPair_HasUp(t *testing.T) {
	type tcase struct {
		pair *MigrationPair
		want bool
	}
	tests := map[string]tcase{
		"has up migration": {
			pair: &MigrationPair{
				Version: 1,
				Up:      &Migration{Version: 1, Direction: Up},
			},
			want: true,
		},
		"no up migration": {
			pair: &MigrationPair{
				Version: 1,
				Up:      nil,
			},
			want: false,
		},
		"empty pair": {
			pair: &MigrationPair{},
			want: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.pair.HasUp(); got != tt.want {
				t.Errorf("HasUp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrationPair_HasDown(t *testing.T) {
	type tcase struct {
		pair *MigrationPair
		want bool
	}
	tests := map[string]tcase{
		"has down migration": {
			pair: &MigrationPair{
				Version: 1,
				Down:    &Migration{Version: 1, Direction: Down},
			},
			want: true,
		},
		"no down migration": {
			pair: &MigrationPair{
				Version: 1,
				Down:    nil,
			},
			want: false,
		},
		"empty pair": {
			pair: &MigrationPair{},
			want: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.pair.HasDown(); got != tt.want {
				t.Errorf("HasDown() = %v, want %v", got, tt.want)
			}
		})
	}
}
