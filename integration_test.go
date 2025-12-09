package scyllamigrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/gocql/gocql"
	td "github.com/maxatome/go-testdeep/td"
)

// shouldRunIntegrationTests checks if integration tests should run based on environment variables.
func shouldRunIntegrationTests() bool {
	return os.Getenv("SCYLLA_HOSTS") != "" && os.Getenv("SCYLLA_KEYSPACE") != ""
}

// getTestSession creates a gocql session for testing with a unique keyspace per test.
func getTestSession(t *testing.T) (*gocql.Session, string) {
	hosts := os.Getenv("SCYLLA_HOSTS")
	if hosts == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	baseKeyspace := os.Getenv("SCYLLA_KEYSPACE")
	if baseKeyspace == "" {
		t.Skip("SCYLLA_KEYSPACE not set, skipping integration test")
	}

	// Create unique keyspace per test to avoid interference
	// Sanitize test name for CQL identifier (replace invalid chars with underscores)
	testName := regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(t.Name(), "_")
	keyspace := fmt.Sprintf("%s_%s_%d", baseKeyspace, testName, time.Now().UnixNano())

	cluster := gocql.NewCluster(hosts)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Consistency = gocql.Quorum

	// Connect without keyspace first to create it
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Create keyspace
	err = session.Query(`CREATE KEYSPACE ` + keyspace + ` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec()
	if err != nil {
		session.Close()
		t.Fatalf("Failed to create keyspace: %v", err)
	}

	// Wait for schema agreement to ensure keyspace is available
	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		session.Close()
		// Try to cleanup
		cleanupSession, _ := gocql.NewCluster(hosts).CreateSession()
		if cleanupSession != nil {
			cleanupSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec()
			cleanupSession.Close()
		}
		t.Fatalf("Failed to wait for schema agreement: %v", err)
	}

	session.Close()

	// Verify keyspace exists and wait for it to be available
	// Retry checking for keyspace existence before connecting
	var keyspaceExists bool
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		checkSession, checkErr := cluster.CreateSession()
		if checkErr == nil {
			var count int
			checkErr = checkSession.Query(`SELECT COUNT(*) FROM system_schema.keyspaces WHERE keyspace_name = ?`, keyspace).Scan(&count)
			checkSession.Close()
			if checkErr == nil && count > 0 {
				keyspaceExists = true
				break
			}
		}
		if i < maxRetries-1 {
			time.Sleep(300 * time.Millisecond)
		}
	}

	if !keyspaceExists {
		// Cleanup keyspace on failure
		cleanupSession, _ := gocql.NewCluster(hosts).CreateSession()
		if cleanupSession != nil {
			cleanupSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec()
			cleanupSession.Close()
		}
		t.Fatalf("Keyspace %s not available after %d retries", keyspace, maxRetries)
	}

	// Now connect with keyspace
	cluster.Keyspace = keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		// Cleanup keyspace on failure
		cleanupSession, _ := gocql.NewCluster(hosts).CreateSession()
		if cleanupSession != nil {
			cleanupSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec()
			cleanupSession.Close()
		}
		t.Fatalf("Failed to create session with keyspace: %v", err)
	}

	// Register cleanup function
	t.Cleanup(func() {
		session.Close()
		// Cleanup keyspace after test
		cleanupSession, err := gocql.NewCluster(hosts).CreateSession()
		if err == nil && cleanupSession != nil {
			cleanupSession.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec()
			cleanupSession.Close()
		}
	})

	return session, keyspace
}

// createTestMigrations creates temporary migration files for testing.
func createTestMigrations(t *testing.T) string {
	tmpDir := t.TempDir()

	// Migration 1: Create users table
	migration1Up := filepath.Join(tmpDir, "000001_create_users.up.cql")
	err := os.WriteFile(migration1Up, []byte(`
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS users_email_idx ON users (email);
`), 0644)
	td.CmpNoError(t, err)

	migration1Down := filepath.Join(tmpDir, "000001_create_users.down.cql")
	err = os.WriteFile(migration1Down, []byte(`
DROP INDEX IF EXISTS users_email_idx;
DROP TABLE IF EXISTS users;
`), 0644)
	td.CmpNoError(t, err)

	// Migration 2: Create posts table
	migration2Up := filepath.Join(tmpDir, "000002_create_posts.up.cql")
	err = os.WriteFile(migration2Up, []byte(`
CREATE TABLE IF NOT EXISTS posts (
    id UUID PRIMARY KEY,
    user_id UUID,
    title TEXT,
    content TEXT,
    created_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS posts_user_id_idx ON posts (user_id);
`), 0644)
	td.CmpNoError(t, err)

	migration2Down := filepath.Join(tmpDir, "000002_create_posts.down.cql")
	err = os.WriteFile(migration2Down, []byte(`
DROP INDEX IF EXISTS posts_user_id_idx;
DROP TABLE IF EXISTS posts;
`), 0644)
	td.CmpNoError(t, err)

	return tmpDir
}

func TestIntegration_Up(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Apply all migrations
	applied, err := migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 2)

	// Verify tables exist
	var tableName string
	err = session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "users").Scan(&tableName)
	td.CmpNoError(t, err)
	td.Cmp(t, tableName, "users")

	err = session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "posts").Scan(&tableName)
	td.CmpNoError(t, err)
	td.Cmp(t, tableName, "posts")

	// Running Up again should not apply anything
	applied, err = migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 0)
}

func TestIntegration_Down(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Apply migrations first
	applied, err := migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 2)

	// Rollback last migration
	err = migrator.Down(ctx)
	td.CmpNoError(t, err)

	// Verify posts table is gone
	var count int
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "posts").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 0)

	// Verify users table still exists
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "users").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 1)
}

func TestIntegration_Status(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Check initial status
	status, err := migrator.Status(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, status.CurrentVersion, uint64(0))
	td.Cmp(t, len(status.Applied), 0)
	td.Cmp(t, len(status.Pending), 2)

	// Apply migrations
	applied, err := migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 2)

	// Check status after applying
	status, err = migrator.Status(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, status.CurrentVersion, uint64(2))
	td.Cmp(t, len(status.Applied), 2)
	td.Cmp(t, len(status.Pending), 0)
}

func TestIntegration_UpTo(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Apply up to version 1
	applied, err := migrator.UpTo(ctx, 1)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 1)

	// Verify only users table exists
	var count int
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "users").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 1)

	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "posts").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 0)

	// Apply remaining migrations
	applied, err = migrator.UpTo(ctx, 2)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 1)

	// Verify both tables exist now
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "posts").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 1)
}

func TestIntegration_DownTo(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Apply all migrations first
	applied, err := migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 2)

	// Rollback to version 1 (exclusive, so version 2 should be rolled back)
	rolledBack, err := migrator.DownTo(ctx, 1)
	td.CmpNoError(t, err)
	td.Cmp(t, rolledBack, 1)

	// Verify posts table is gone
	var count int
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "posts").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 0)

	// Verify users table still exists
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "users").Scan(&count)
	td.CmpNoError(t, err)
	td.Cmp(t, count, 1)
}

func TestIntegration_Version(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Initial version should be 0
	version, err := migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(0))

	// Apply first migration
	applied, err := migrator.UpTo(ctx, 1)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 1)

	version, err = migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(1))

	// Apply second migration
	applied, err = migrator.UpTo(ctx, 2)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 1)

	version, err = migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(2))
}

func TestIntegration_Steps(t *testing.T) {
	if !shouldRunIntegrationTests() {
		t.Skip("Integration tests disabled (set SCYLLA_HOSTS and SCYLLA_KEYSPACE to enable)")
	}

	session, keyspace := getTestSession(t)

	migrationDir := createTestMigrations(t)

	migrator, err := New(session,
		WithDir(migrationDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	ctx := context.Background()

	// Apply 1 migration forward
	err = migrator.Steps(ctx, 1)
	td.CmpNoError(t, err)

	version, err := migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(1))

	// Apply 1 more migration forward
	err = migrator.Steps(ctx, 1)
	td.CmpNoError(t, err)

	version, err = migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(2))

	// Rollback 1 migration
	err = migrator.Steps(ctx, -1)
	td.CmpNoError(t, err)

	version, err = migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(1))

	// Rollback 1 more migration
	err = migrator.Steps(ctx, -1)
	td.CmpNoError(t, err)

	version, err = migrator.Version(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, version, uint64(0))
}
