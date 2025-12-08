package scyllamigrate

import (
	"io"
	"io/fs"
	"os"
	"sort"
)

// Source provides access to migration files.
type Source interface {
	// List returns all available migration pairs sorted by version.
	List() ([]*MigrationPair, error)

	// ReadUp returns the content of the up migration for the given version.
	ReadUp(version uint64) (io.ReadCloser, error)

	// ReadDown returns the content of the down migration for the given version.
	ReadDown(version uint64) (io.ReadCloser, error)

	// Close releases any resources held by the source.
	Close() error
}

// FSSource implements Source using fs.FS (supports go:embed).
type FSSource struct {
	fsys       fs.FS
	migrations map[uint64]*MigrationPair
	versions   []uint64 // sorted
}

// NewFSSource creates a Source from an fs.FS instance.
// This supports embedded migrations via go:embed.
func NewFSSource(fsys fs.FS) (*FSSource, error) {
	s := &FSSource{
		fsys:       fsys,
		migrations: make(map[uint64]*MigrationPair),
	}

	if err := s.scan(); err != nil {
		return nil, err
	}

	return s, nil
}

// NewDirSource creates a Source from a filesystem directory path.
func NewDirSource(path string) (*FSSource, error) {
	return NewFSSource(os.DirFS(path))
}

// scan reads the fs.FS and indexes all migration files.
func (s *FSSource) scan() error {
	entries, err := fs.ReadDir(s.fsys, ".")
	if err != nil {
		return &SourceError{Op: "scan", Err: err}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !IsMigrationFile(name) {
			continue
		}

		m, err := ParseMigration(name)
		if err != nil {
			return err
		}

		pair, ok := s.migrations[m.Version]
		if !ok {
			pair = &MigrationPair{
				Version:     m.Version,
				Description: m.Description,
			}
			s.migrations[m.Version] = pair
			s.versions = append(s.versions, m.Version)
		}

		switch m.Direction {
		case Up:
			pair.Up = m
		case Down:
			pair.Down = m
		}
	}

	// Sort versions in ascending order
	sort.Slice(s.versions, func(i, j int) bool {
		return s.versions[i] < s.versions[j]
	})

	return nil
}

// List returns all available migration pairs sorted by version.
func (s *FSSource) List() ([]*MigrationPair, error) {
	pairs := make([]*MigrationPair, 0, len(s.versions))
	for _, version := range s.versions {
		pairs = append(pairs, s.migrations[version])
	}
	return pairs, nil
}

// ReadUp returns the content of the up migration for the given version.
func (s *FSSource) ReadUp(version uint64) (io.ReadCloser, error) {
	pair, ok := s.migrations[version]
	if !ok {
		return nil, &SourceError{Version: version, Op: "read up", Err: ErrVersionNotFound}
	}

	if pair.Up == nil {
		return nil, &SourceError{Version: version, Op: "read up", Err: ErrMissingUp}
	}

	f, err := s.fsys.Open(pair.Up.Raw)
	if err != nil {
		return nil, &SourceError{Version: version, Op: "read up", Err: err}
	}

	return f, nil
}

// ReadDown returns the content of the down migration for the given version.
func (s *FSSource) ReadDown(version uint64) (io.ReadCloser, error) {
	pair, ok := s.migrations[version]
	if !ok {
		return nil, &SourceError{Version: version, Op: "read down", Err: ErrVersionNotFound}
	}

	if pair.Down == nil {
		return nil, &SourceError{Version: version, Op: "read down", Err: ErrMissingDown}
	}

	f, err := s.fsys.Open(pair.Down.Raw)
	if err != nil {
		return nil, &SourceError{Version: version, Op: "read down", Err: err}
	}

	return f, nil
}

// Close releases any resources held by the source.
// For FSSource, this is a no-op as fs.FS doesn't require cleanup.
func (s *FSSource) Close() error {
	return nil
}

// Get returns the migration pair for the given version.
func (s *FSSource) Get(version uint64) (*MigrationPair, bool) {
	pair, ok := s.migrations[version]
	return pair, ok
}

// Versions returns all available versions in sorted order.
func (s *FSSource) Versions() []uint64 {
	result := make([]uint64, len(s.versions))
	copy(result, s.versions)
	return result
}
