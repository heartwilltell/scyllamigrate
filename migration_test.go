package scyllamigrate

import (
	"testing"
)

func TestDirection_String(t *testing.T) {
	tests := []struct {
		name     string
		d        Direction
		expected string
	}{
		{
			name:     "Up direction",
			d:        Up,
			expected: "up",
		},
		{
			name:     "Down direction",
			d:        Down,
			expected: "down",
		},
		{
			name:     "Empty direction",
			d:        Direction(""),
			expected: "",
		},
		{
			name:     "Custom direction",
			d:        Direction("custom"),
			expected: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseMigration(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		wantErr   bool
		checkFunc func(*testing.T, *Migration, error)
	}{
		{
			name:     "valid up migration with cql extension",
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
		{
			name:     "valid down migration with sql extension",
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
		{
			name:     "valid migration with multi-word description",
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
		{
			name:     "valid migration with single digit version",
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
		{
			name:     "valid migration with large version number",
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
		{
			name:    "invalid - missing version",
			filename: "_create_users.up.cql",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
				if _, ok := err.(*ParseError); !ok {
					t.Errorf("error type = %T, want *ParseError", err)
				}
			},
		},
		{
			name:    "invalid - missing description",
			filename: "000001.up.cql",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		{
			name:    "invalid - wrong direction",
			filename: "000001_create_users.sideways.cql",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		{
			name:    "invalid - wrong extension",
			filename: "000001_create_users.up.txt",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		{
			name:    "invalid - missing extension",
			filename: "000001_create_users.up",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		{
			name:    "invalid - empty filename",
			filename: "",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
		{
			name:    "invalid - no underscores",
			filename: "000001createusers.up.cql",
			wantErr: true,
			checkFunc: func(t *testing.T, m *Migration, err error) {
				if err == nil {
					t.Fatal("ParseMigration() error = nil, want error")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		{
			name:     "valid up migration cql",
			filename: "000001_create_users.up.cql",
			want:     true,
		},
		{
			name:     "valid down migration sql",
			filename: "000001_create_users.down.sql",
			want:     true,
		},
		{
			name:     "valid single digit version",
			filename: "1_initial.up.cql",
			want:     true,
		},
		{
			name:     "invalid - missing version",
			filename: "_create_users.up.cql",
			want:     false,
		},
		{
			name:     "invalid - wrong extension",
			filename: "000001_create_users.up.txt",
			want:     false,
		},
		{
			name:     "invalid - wrong direction",
			filename: "000001_create_users.sideways.cql",
			want:     false,
		},
		{
			name:     "invalid - empty filename",
			filename: "",
			want:     false,
		},
		{
			name:     "invalid - regular file",
			filename: "README.md",
			want:     false,
		},
		{
			name:     "invalid - no extension",
			filename: "000001_create_users.up",
			want:     false,
		},
		{
			name:     "valid with underscores in description",
			filename: "000001_add_user_profile_table.up.cql",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsMigrationFile(tt.filename); got != tt.want {
				t.Errorf("IsMigrationFile(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

func TestMigrationPair_HasUp(t *testing.T) {
	tests := []struct {
		name string
		pair *MigrationPair
		want bool
	}{
		{
			name: "has up migration",
			pair: &MigrationPair{
				Version: 1,
				Up:      &Migration{Version: 1, Direction: Up},
			},
			want: true,
		},
		{
			name: "no up migration",
			pair: &MigrationPair{
				Version: 1,
				Up:      nil,
			},
			want: false,
		},
		{
			name: "empty pair",
			pair: &MigrationPair{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pair.HasUp(); got != tt.want {
				t.Errorf("HasUp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrationPair_HasDown(t *testing.T) {
	tests := []struct {
		name string
		pair *MigrationPair
		want bool
	}{
		{
			name: "has down migration",
			pair: &MigrationPair{
				Version: 1,
				Down:    &Migration{Version: 1, Direction: Down},
			},
			want: true,
		},
		{
			name: "no down migration",
			pair: &MigrationPair{
				Version: 1,
				Down:    nil,
			},
			want: false,
		},
		{
			name: "empty pair",
			pair: &MigrationPair{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pair.HasDown(); got != tt.want {
				t.Errorf("HasDown() = %v, want %v", got, tt.want)
			}
		})
	}
}

