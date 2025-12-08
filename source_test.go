package scyllamigrate

import (
	"io"
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestNewFSSource(t *testing.T) {
	type tcase struct {
		files     map[string]string
		wantErr   bool
		checkFunc func(*testing.T, *FSSource, error)
	}
	tests := map[string]tcase{
		"valid migrations": {
			files: map[string]string{
				"000001_create_users.up.cql":   "CREATE TABLE users;",
				"000001_create_users.down.cql": "DROP TABLE users;",
				"000002_add_index.up.cql":      "CREATE INDEX idx ON users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil", err)
				}
				if s == nil {
					t.Fatal("NewFSSource() returned nil source")
				}
				pairs, err := s.List()
				if err != nil {
					t.Fatalf("List() error = %v", err)
				}
				if len(pairs) != 2 {
					t.Errorf("List() returned %d pairs, want 2", len(pairs))
				}
			},
		},
		"only up migrations": {
			files: map[string]string{
				"000001_create_users.up.cql": "CREATE TABLE users;",
				"000002_add_index.up.cql":    "CREATE INDEX idx ON users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil", err)
				}
				pairs, _ := s.List()
				if len(pairs) != 2 {
					t.Errorf("List() returned %d pairs, want 2", len(pairs))
				}
				if pairs[0].HasUp() && !pairs[0].HasDown() {
					// Expected: has up but no down
				} else {
					t.Error("Expected first pair to have up but no down")
				}
			},
		},
		"mixed extensions": {
			files: map[string]string{
				"000001_create_users.up.cql":   "CREATE TABLE users;",
				"000001_create_users.down.sql": "DROP TABLE users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil", err)
				}
			},
		},
		"non-migration files ignored": {
			files: map[string]string{
				"000001_create_users.up.cql": "CREATE TABLE users;",
				"README.md":                  "# Documentation",
				"config.yaml":                "config: value",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil", err)
				}
				pairs, _ := s.List()
				if len(pairs) != 1 {
					t.Errorf("List() returned %d pairs, want 1", len(pairs))
				}
			},
		},
		"invalid migration filename ignored": {
			files: map[string]string{
				"invalid_file.cql": "CREATE TABLE users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil (invalid files are ignored)", err)
				}
				pairs, _ := s.List()
				if len(pairs) != 0 {
					t.Errorf("List() returned %d pairs, want 0 (invalid files should be ignored)", len(pairs))
				}
			},
		},
		"empty filesystem": {
			files:   map[string]string{},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				if err != nil {
					t.Fatalf("NewFSSource() error = %v, want nil", err)
				}
				pairs, _ := s.List()
				if len(pairs) != 0 {
					t.Errorf("List() returned %d pairs, want 0", len(pairs))
				}
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fsys := fstest.MapFS{}
			for fileName, content := range tt.files {
				fsys[fileName] = &fstest.MapFile{
					Data: []byte(content),
				}
			}

			got, err := NewFSSource(fsys)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFSSource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, got, err)
			}
		})
	}
}

func TestFSSource_List(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
		"000002_add_index.up.cql":      {Data: []byte("CREATE INDEX idx;")},
		"000002_add_index.down.cql":    {Data: []byte("DROP INDEX idx;")},
		"000003_final.up.cql":          {Data: []byte("ALTER TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	pairs, err := source.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(pairs) != 3 {
		t.Fatalf("List() returned %d pairs, want 3", len(pairs))
	}

	// Check sorting
	for i := 0; i < len(pairs)-1; i++ {
		if pairs[i].Version >= pairs[i+1].Version {
			t.Errorf("Pairs not sorted: %d >= %d", pairs[i].Version, pairs[i+1].Version)
		}
	}

	// Check versions
	expectedVersions := []uint64{1, 2, 3}
	for i, pair := range pairs {
		if pair.Version != expectedVersions[i] {
			t.Errorf("Pairs[%d].Version = %d, want %d", i, pair.Version, expectedVersions[i])
		}
	}
}

func TestFSSource_ReadUp(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	type tcase struct {
		version   uint64
		wantErr   bool
		checkFunc func(*testing.T, io.ReadCloser, error)
	}
	tests := map[string]tcase{
		"valid version": {
			version: 1,
			wantErr: false,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err != nil {
					t.Fatalf("ReadUp() error = %v, want nil", err)
				}
				if r == nil {
					t.Fatal("ReadUp() returned nil reader")
				}
				defer r.Close()
				data, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("ReadAll() error = %v", err)
				}
				if string(data) != "CREATE TABLE users;" {
					t.Errorf("ReadUp() content = %q, want %q", string(data), "CREATE TABLE users;")
				}
			},
		},
		"version not found": {
			version: 999,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err == nil {
					t.Fatal("ReadUp() error = nil, want error")
				}
				if _, ok := err.(*SourceError); !ok {
					t.Errorf("error type = %T, want *SourceError", err)
				}
			},
		},
		"up migration missing": {
			version: 2,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err == nil {
					t.Fatal("ReadUp() error = nil, want error")
				}
				if _, ok := err.(*SourceError); !ok {
					t.Errorf("error type = %T, want *SourceError", err)
				}
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r, err := source.ReadUp(tt.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadUp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, r, err)
			}
			if r != nil {
				r.Close()
			}
		})
	}
}

func TestFSSource_ReadDown(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	type tcase struct {
		version   uint64
		wantErr   bool
		checkFunc func(*testing.T, io.ReadCloser, error)
	}
	tests := map[string]tcase{
		"valid version": {
			version: 1,
			wantErr: false,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err != nil {
					t.Fatalf("ReadDown() error = %v, want nil", err)
				}
				if r == nil {
					t.Fatal("ReadDown() returned nil reader")
				}
				defer r.Close()
				data, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("ReadAll() error = %v", err)
				}
				if string(data) != "DROP TABLE users;" {
					t.Errorf("ReadDown() content = %q, want %q", string(data), "DROP TABLE users;")
				}
			},
		},
		"version not found": {
			version: 999,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err == nil {
					t.Fatal("ReadDown() error = nil, want error")
				}
			},
		},
		"down migration missing": {
			version: 2,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				if err == nil {
					t.Fatal("ReadDown() error = nil, want error")
				}
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r, err := source.ReadDown(tt.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadDown() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, r, err)
			}
			if r != nil {
				r.Close()
			}
		})
	}
}

func TestFSSource_Close(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	if err := source.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}

	// Should be safe to close multiple times
	if err := source.Close(); err != nil {
		t.Errorf("Close() second time error = %v, want nil", err)
	}
}

func TestFSSource_Get(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
		"000002_add_index.up.cql":      {Data: []byte("CREATE INDEX idx;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	type tcase struct {
		version uint64
		want    bool
	}
	tests := map[string]tcase{
		"existing version": {
			version: 1,
			want:    true,
		},
		"non-existent version": {
			version: 999,
			want:    false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			pair, got := source.Get(tt.version)
			if got != tt.want {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
			if tt.want && pair == nil {
				t.Error("Get() returned nil pair for existing version")
			}
			if !tt.want && pair != nil {
				t.Error("Get() returned non-nil pair for non-existent version")
			}
		})
	}
}

func TestFSSource_Versions(t *testing.T) {
	fsys := fstest.MapFS{
		"000003_final.up.cql":        {Data: []byte("ALTER TABLE users;")},
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
		"000002_add_index.up.cql":    {Data: []byte("CREATE INDEX idx;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	versions := source.Versions()
	expected := []uint64{1, 2, 3}

	if len(versions) != len(expected) {
		t.Fatalf("Versions() returned %d versions, want %d", len(versions), len(expected))
	}

	for i, v := range versions {
		if v != expected[i] {
			t.Errorf("Versions()[%d] = %d, want %d", i, v, expected[i])
		}
	}

	// Verify it's a copy (modifying shouldn't affect source)
	versions[0] = 999
	versions2 := source.Versions()
	if versions2[0] == 999 {
		t.Error("Versions() did not return a copy")
	}
}

func TestFSSource_scanError(t *testing.T) {
	// Test with a filesystem that has an invalid migration file
	// Invalid files are ignored, not errors
	fsys := fstest.MapFS{
		"invalid_file.cql": {Data: []byte("CREATE TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v, want nil (invalid files are ignored)", err)
	}

	pairs, err := source.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(pairs) != 0 {
		t.Errorf("List() returned %d pairs, want 0 (invalid files should be ignored)", len(pairs))
	}
}

func TestFSSource_scanWithDirectories(t *testing.T) {
	// Create a filesystem with directories that should be ignored
	fsys := fstest.MapFS{
		"000001_create_users.up.cql": {Data: []byte("CREATE TABLE users;")},
		"subdir": {
			Mode: fs.ModeDir,
		},
	}

	source, err := NewFSSource(fsys)
	if err != nil {
		t.Fatalf("NewFSSource() error = %v", err)
	}

	pairs, err := source.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(pairs) != 1 {
		t.Errorf("List() returned %d pairs, want 1 (directories should be ignored)", len(pairs))
	}
}
