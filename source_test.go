package scyllamigrate

import (
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	td "github.com/maxatome/go-testdeep/td"
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
				td.CmpNoError(t, err)
				td.Cmp(t, s, td.NotNil())
				pairs, err := s.List()
				td.CmpNoError(t, err)
				td.Cmp(t, len(pairs), 2)
			},
		},
		"only up migrations": {
			files: map[string]string{
				"000001_create_users.up.cql": "CREATE TABLE users;",
				"000002_add_index.up.cql":    "CREATE INDEX idx ON users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				td.CmpNoError(t, err)
				pairs, _ := s.List()
				td.Cmp(t, len(pairs), 2)
				td.Cmp(t, pairs[0].HasUp() && !pairs[0].HasDown(), true)
			},
		},
		"mixed extensions": {
			files: map[string]string{
				"000001_create_users.up.cql":   "CREATE TABLE users;",
				"000001_create_users.down.sql": "DROP TABLE users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				td.CmpNoError(t, err)
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
				td.CmpNoError(t, err)
				pairs, _ := s.List()
				td.Cmp(t, len(pairs), 1)
			},
		},
		"invalid migration filename ignored": {
			files: map[string]string{
				"invalid_file.cql": "CREATE TABLE users;",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				td.CmpNoError(t, err)
				pairs, _ := s.List()
				td.Cmp(t, len(pairs), 0)
			},
		},
		"empty filesystem": {
			files:   map[string]string{},
			wantErr: false,
			checkFunc: func(t *testing.T, s *FSSource, err error) {
				td.CmpNoError(t, err)
				pairs, _ := s.List()
				td.Cmp(t, len(pairs), 0)
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
			if tt.wantErr {
				td.CmpError(t, err)
			} else {
				td.CmpNoError(t, err)
			}
			if tt.wantErr {
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
	td.CmpNoError(t, err)

	pairs, err := source.List()
	td.CmpNoError(t, err)

	td.Cmp(t, len(pairs), 3)

	// Check sorting
	for i := 0; i < len(pairs)-1; i++ {
		td.Cmp(t, pairs[i].Version < pairs[i+1].Version, true)
	}

	// Check versions
	expectedVersions := []uint64{1, 2, 3}
	for i, pair := range pairs {
		td.Cmp(t, pair.Version, expectedVersions[i])
	}
}

func TestFSSource_ReadUp(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
	}

	source, err := NewFSSource(fsys)
	td.CmpNoError(t, err)

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
				td.CmpNoError(t, err)
				td.Cmp(t, r, td.NotNil())
				defer r.Close()
				data, err := io.ReadAll(r)
				td.CmpNoError(t, err)
				td.Cmp(t, string(data), "CREATE TABLE users;")
			},
		},
		"version not found": {
			version: 999,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				td.CmpError(t, err)
				td.Cmp(t, err, td.Isa((*SourceError)(nil)))
			},
		},
		"up migration missing": {
			version: 2,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				td.CmpError(t, err)
				td.Cmp(t, err, td.Isa((*SourceError)(nil)))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r, err := source.ReadUp(tt.version)
			if tt.wantErr {
				td.CmpError(t, err)
			} else {
				td.CmpNoError(t, err)
			}
			if tt.wantErr {
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
	td.CmpNoError(t, err)

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
				td.CmpNoError(t, err)
				td.Cmp(t, r, td.NotNil())
				defer r.Close()
				data, err := io.ReadAll(r)
				td.CmpNoError(t, err)
				td.Cmp(t, string(data), "DROP TABLE users;")
			},
		},
		"version not found": {
			version: 999,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				td.CmpError(t, err)
			},
		},
		"down migration missing": {
			version: 2,
			wantErr: true,
			checkFunc: func(t *testing.T, r io.ReadCloser, err error) {
				td.CmpError(t, err)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r, err := source.ReadDown(tt.version)
			if tt.wantErr {
				td.CmpError(t, err)
			} else {
				td.CmpNoError(t, err)
			}
			if tt.wantErr {
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
	td.CmpNoError(t, err)

	td.CmpNoError(t, source.Close())

	// Should be safe to close multiple times
	td.CmpNoError(t, source.Close())
}

func TestFSSource_Get(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_create_users.up.cql":   {Data: []byte("CREATE TABLE users;")},
		"000001_create_users.down.cql": {Data: []byte("DROP TABLE users;")},
		"000002_add_index.up.cql":      {Data: []byte("CREATE INDEX idx;")},
	}

	source, err := NewFSSource(fsys)
	td.CmpNoError(t, err)

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
			td.Cmp(t, got, tt.want)
			if tt.want {
				td.Cmp(t, pair, td.NotNil())
			} else {
				td.Cmp(t, pair, td.Nil())
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
	td.CmpNoError(t, err)

	versions := source.Versions()
	expected := []uint64{1, 2, 3}

	td.Cmp(t, versions, expected)

	// Verify it's a copy (modifying shouldn't affect source)
	versions[0] = 999
	versions2 := source.Versions()
	td.Cmp(t, versions2[0] == 999, false)
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
	td.CmpNoError(t, err)

	td.Cmp(t, len(pairs), 0)
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
	td.CmpNoError(t, err)

	pairs, err := source.List()
	td.CmpNoError(t, err)

	td.Cmp(t, len(pairs), 1)
}
