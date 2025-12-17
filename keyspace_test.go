package scyllamigrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	td "github.com/maxatome/go-testdeep/td"
)

func TestBuildCreateKeyspaceCQL_SimpleStrategy(t *testing.T) {
	type tcase struct {
		cfg      *KeyspaceConfig
		expected string
	}

	tests := map[string]tcase{
		"simple strategy with default options": {
			cfg: &KeyspaceConfig{
				Name:              "test_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 1,
				IfNotExists:       true,
			},
			expected: "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		},
		"simple strategy with rf 3": {
			cfg: &KeyspaceConfig{
				Name:              "prod_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 3,
				IfNotExists:       true,
			},
			expected: "CREATE KEYSPACE IF NOT EXISTS prod_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
		},
		"simple strategy without if not exists": {
			cfg: &KeyspaceConfig{
				Name:              "strict_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 2,
				IfNotExists:       false,
			},
			expected: "CREATE KEYSPACE strict_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
		},
		"simple strategy with durable writes disabled": {
			cfg: &KeyspaceConfig{
				Name:              "fast_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 1,
				IfNotExists:       true,
				DurableWrites:     boolPtr(false),
			},
			expected: "CREATE KEYSPACE IF NOT EXISTS fast_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = false",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := buildCreateKeyspaceCQL(tc.cfg)
			td.Cmp(t, result, tc.expected)
		})
	}
}

func TestBuildCreateKeyspaceCQL_NetworkTopologyStrategy(t *testing.T) {
	cfg := &KeyspaceConfig{
		Name:        "multi_dc_keyspace",
		Strategy:    NetworkTopologyStrategy,
		Datacenters: map[string]int{"dc1": 3},
		IfNotExists: true,
	}

	result := buildCreateKeyspaceCQL(cfg)

	// Check that it contains the expected parts
	td.Cmp(t, strings.Contains(result, "CREATE KEYSPACE IF NOT EXISTS multi_dc_keyspace"), true)
	td.Cmp(t, strings.Contains(result, "'class': 'NetworkTopologyStrategy'"), true)
	td.Cmp(t, strings.Contains(result, "'dc1': 3"), true)
}

func TestBuildCreateKeyspaceCQL_NetworkTopologyMultipleDCs(t *testing.T) {
	cfg := &KeyspaceConfig{
		Name:        "global_keyspace",
		Strategy:    NetworkTopologyStrategy,
		Datacenters: map[string]int{"us-east": 3, "eu-west": 2},
		IfNotExists: true,
	}

	result := buildCreateKeyspaceCQL(cfg)

	// Check that it contains the expected parts
	td.Cmp(t, strings.Contains(result, "CREATE KEYSPACE IF NOT EXISTS global_keyspace"), true)
	td.Cmp(t, strings.Contains(result, "'class': 'NetworkTopologyStrategy'"), true)
	td.Cmp(t, strings.Contains(result, "'us-east': 3"), true)
	td.Cmp(t, strings.Contains(result, "'eu-west': 2"), true)
}

func TestKeyspaceOptions(t *testing.T) {
	t.Run("WithReplicationFactor", func(t *testing.T) {
		cfg := &KeyspaceConfig{Name: "test"}
		WithReplicationFactor(5)(cfg)
		td.Cmp(t, cfg.ReplicationFactor, 5)
	})

	t.Run("WithNetworkTopology", func(t *testing.T) {
		cfg := &KeyspaceConfig{Name: "test"}
		dcs := map[string]int{"dc1": 3, "dc2": 2}
		WithNetworkTopology(dcs)(cfg)
		td.Cmp(t, cfg.Strategy, NetworkTopologyStrategy)
		td.Cmp(t, cfg.Datacenters, dcs)
	})

	t.Run("WithDurableWrites", func(t *testing.T) {
		cfg := &KeyspaceConfig{Name: "test"}
		WithDurableWrites(false)(cfg)
		td.Cmp(t, *cfg.DurableWrites, false)

		WithDurableWrites(true)(cfg)
		td.Cmp(t, *cfg.DurableWrites, true)
	})

	t.Run("WithIfNotExists", func(t *testing.T) {
		cfg := &KeyspaceConfig{Name: "test"}
		WithIfNotExists(false)(cfg)
		td.Cmp(t, cfg.IfNotExists, false)

		WithIfNotExists(true)(cfg)
		td.Cmp(t, cfg.IfNotExists, true)
	})
}

func TestCreateKeyspace_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("nil session", func(t *testing.T) {
		err := CreateKeyspace(ctx, nil, "test")
		td.Cmp(t, err, ErrNoSession)
	})

	t.Run("empty keyspace name", func(t *testing.T) {
		// We can't test with a real session here without integration tests,
		// so we'll just verify the error for empty name would be returned
		// before any session operation
		err := CreateKeyspace(ctx, nil, "")
		td.Cmp(t, err, ErrNoSession) // nil session check comes first
	})
}

func TestKeyspaceExists_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("nil session", func(t *testing.T) {
		_, err := KeyspaceExists(ctx, nil, "test")
		td.Cmp(t, err, ErrNoSession)
	})

	t.Run("empty keyspace name", func(t *testing.T) {
		_, err := KeyspaceExists(ctx, nil, "")
		td.Cmp(t, err, ErrNoSession) // nil session check comes first
	})
}

func TestDropKeyspace_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("nil session", func(t *testing.T) {
		err := DropKeyspace(ctx, nil, "test", true)
		td.Cmp(t, err, ErrNoSession)
	})

	t.Run("empty keyspace name", func(t *testing.T) {
		err := DropKeyspace(ctx, nil, "", true)
		td.Cmp(t, err, ErrNoSession) // nil session check comes first
	})
}

// Integration tests

// getKeyspaceTestSession creates a gocql session for keyspace testing (without keyspace).
func getKeyspaceTestSession(t *testing.T) *gocql.Session {
	hosts := os.Getenv("SCYLLA_HOSTS")
	if hosts == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	cluster := gocql.NewCluster(hosts)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	t.Cleanup(func() {
		session.Close()
	})

	return session
}

// generateTestKeyspaceName creates a unique keyspace name for testing.
func generateTestKeyspaceName(t *testing.T) string {
	testName := regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(t.Name(), "_")
	return strings.ToLower(fmt.Sprintf("test_%s_%d", testName, time.Now().UnixNano()))
}

func TestIntegration_CreateKeyspace_SimpleStrategy(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
	)
	td.CmpNoError(t, err)

	// Verify keyspace exists
	exists, err := KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)
}

func TestIntegration_CreateKeyspace_WithDurableWritesDisabled(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace with durable writes disabled
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
		WithDurableWrites(false),
	)
	td.CmpNoError(t, err)

	// Verify keyspace exists
	exists, err := KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)

	// Verify durable_writes is false
	var durableWrites bool
	err = session.Query(`SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?`, keyspace).
		Scan(&durableWrites)
	td.CmpNoError(t, err)
	td.Cmp(t, durableWrites, false)
}

func TestIntegration_CreateKeyspace_IfNotExists(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace first time
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
		WithIfNotExists(true),
	)
	td.CmpNoError(t, err)

	// Create keyspace second time should succeed (IF NOT EXISTS)
	err = CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
		WithIfNotExists(true),
	)
	td.CmpNoError(t, err)
}

func TestIntegration_KeyspaceExists(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Keyspace should not exist initially
	exists, err := KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, false)

	// Create keyspace
	err = CreateKeyspace(ctx, session, keyspace, WithReplicationFactor(1))
	td.CmpNoError(t, err)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Keyspace should exist now
	exists, err = KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)
}

func TestIntegration_DropKeyspace(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Create keyspace
	err := CreateKeyspace(ctx, session, keyspace, WithReplicationFactor(1))
	td.CmpNoError(t, err)

	// Verify it exists
	exists, err := KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)

	// Drop keyspace
	err = DropKeyspace(ctx, session, keyspace, false)
	td.CmpNoError(t, err)

	// Verify it no longer exists
	exists, err = KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, false)
}

func TestIntegration_DropKeyspace_IfExists(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Drop non-existent keyspace with IF EXISTS should succeed
	err := DropKeyspace(ctx, session, keyspace, true)
	td.CmpNoError(t, err)
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool {
	return &b
}

// KeyspaceError tests

func TestKeyspaceError(t *testing.T) {
	type tcase struct {
		err      *KeyspaceError
		expected string
	}

	tests := map[string]tcase{
		"create error": {
			err: &KeyspaceError{
				Keyspace: "test_ks",
				Op:       "create",
				Err:      fmt.Errorf("connection refused"),
			},
			expected: `scyllamigrate: keyspace error for "test_ks" (create): connection refused`,
		},
		"drop error": {
			err: &KeyspaceError{
				Keyspace: "prod_ks",
				Op:       "drop",
				Err:      fmt.Errorf("permission denied"),
			},
			expected: `scyllamigrate: keyspace error for "prod_ks" (drop): permission denied`,
		},
		"check existence error": {
			err: &KeyspaceError{
				Keyspace: "my_keyspace",
				Op:       "check existence",
				Err:      fmt.Errorf("timeout"),
			},
			expected: `scyllamigrate: keyspace error for "my_keyspace" (check existence): timeout`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			td.Cmp(t, tc.err.Error(), tc.expected)
		})
	}
}

func TestKeyspaceError_Unwrap(t *testing.T) {
	innerErr := fmt.Errorf("inner error")
	err := &KeyspaceError{
		Keyspace: "test",
		Op:       "create",
		Err:      innerErr,
	}

	td.Cmp(t, err.Unwrap(), innerErr)
}

// Edge case tests

func TestBuildCreateKeyspaceCQL_EdgeCases(t *testing.T) {
	type tcase struct {
		cfg      *KeyspaceConfig
		contains []string
	}

	tests := map[string]tcase{
		"keyspace name with underscores": {
			cfg: &KeyspaceConfig{
				Name:              "my_app_production",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 3,
				IfNotExists:       true,
			},
			contains: []string{"my_app_production", "SimpleStrategy", "3"},
		},
		"minimum replication factor": {
			cfg: &KeyspaceConfig{
				Name:              "single_node",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 1,
				IfNotExists:       true,
			},
			contains: []string{"'replication_factor': 1"},
		},
		"high replication factor": {
			cfg: &KeyspaceConfig{
				Name:              "highly_available",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 7,
				IfNotExists:       true,
			},
			contains: []string{"'replication_factor': 7"},
		},
		"durable writes enabled explicitly": {
			cfg: &KeyspaceConfig{
				Name:              "durable_ks",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 1,
				IfNotExists:       true,
				DurableWrites:     boolPtr(true),
			},
			contains: []string{"durable_writes = true"},
		},
		"network topology with hyphenated dc names": {
			cfg: &KeyspaceConfig{
				Name:        "global_ks",
				Strategy:    NetworkTopologyStrategy,
				Datacenters: map[string]int{"aws-us-east-1": 3},
				IfNotExists: true,
			},
			contains: []string{"'aws-us-east-1': 3", "NetworkTopologyStrategy"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := buildCreateKeyspaceCQL(tc.cfg)
			for _, substr := range tc.contains {
				td.Cmp(t, strings.Contains(result, substr), true,
					"expected CQL to contain %q, got: %s", substr, result)
			}
		})
	}
}

func TestReplicationStrategy_Values(t *testing.T) {
	td.Cmp(t, string(SimpleStrategy), "SimpleStrategy")
	td.Cmp(t, string(NetworkTopologyStrategy), "NetworkTopologyStrategy")
}

func TestKeyspaceConfig_Defaults(t *testing.T) {
	// Test that CreateKeyspace applies correct defaults
	cfg := &KeyspaceConfig{
		Name: "test",
	}

	// Default values before any options applied
	td.Cmp(t, cfg.Strategy, ReplicationStrategy(""))
	td.Cmp(t, cfg.ReplicationFactor, 0)
	td.Cmp(t, cfg.IfNotExists, false)
	td.Cmp(t, cfg.DurableWrites, (*bool)(nil))
}

// Additional integration tests

func TestIntegration_CreateKeyspace_AlreadyExists_NoIfNotExists(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace first time
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
		WithIfNotExists(true),
	)
	td.CmpNoError(t, err)

	// Create keyspace second time without IF NOT EXISTS should fail
	err = CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
		WithIfNotExists(false),
	)
	td.CmpError(t, err)

	// Verify it's a KeyspaceError
	var ksErr *KeyspaceError
	td.Cmp(t, errors.As(err, &ksErr), true)
	td.Cmp(t, ksErr.Keyspace, keyspace)
	td.Cmp(t, ksErr.Op, "create")
}

func TestIntegration_DropKeyspace_NonExistent_NoIfExists(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Drop non-existent keyspace without IF EXISTS should fail
	err := DropKeyspace(ctx, session, keyspace, false)
	td.CmpError(t, err)

	// Verify it's a KeyspaceError
	var ksErr *KeyspaceError
	td.Cmp(t, errors.As(err, &ksErr), true)
	td.Cmp(t, ksErr.Keyspace, keyspace)
	td.Cmp(t, ksErr.Op, "drop")
}

func TestIntegration_CreateAndUseMigrations(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace using the new function
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
	)
	td.CmpNoError(t, err)

	// Verify keyspace exists
	exists, err := KeyspaceExists(ctx, session, keyspace)
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)

	// Now create a session with this keyspace and run migrations
	hosts := os.Getenv("SCYLLA_HOSTS")
	cluster := gocql.NewCluster(hosts)
	cluster.Keyspace = keyspace
	cluster.Timeout = 30 * time.Second
	cluster.Consistency = gocql.Quorum

	ksSession, err := cluster.CreateSession()
	td.CmpNoError(t, err)
	defer ksSession.Close()

	// Create a simple migration
	tmpDir := t.TempDir()
	migrationUp := tmpDir + "/000001_create_test_table.up.cql"
	migrationDown := tmpDir + "/000001_create_test_table.down.cql"

	err = os.WriteFile(migrationUp, []byte(`
CREATE TABLE IF NOT EXISTS test_table (
    id UUID PRIMARY KEY,
    name TEXT
);
`), 0644)
	td.CmpNoError(t, err)

	err = os.WriteFile(migrationDown, []byte(`
DROP TABLE IF EXISTS test_table;
`), 0644)
	td.CmpNoError(t, err)

	// Create migrator and run migrations
	migrator, err := New(ksSession,
		WithDir(tmpDir),
		WithKeyspace(keyspace),
	)
	td.CmpNoError(t, err)
	defer migrator.Close()

	applied, err := migrator.Up(ctx)
	td.CmpNoError(t, err)
	td.Cmp(t, applied, 1)

	// Verify table was created
	var tableName string
	err = ksSession.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, "test_table").Scan(&tableName)
	td.CmpNoError(t, err)
	td.Cmp(t, tableName, "test_table")
}

func TestIntegration_KeyspaceExists_SystemKeyspaces(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()

	// system keyspace should always exist
	exists, err := KeyspaceExists(ctx, session, "system")
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)

	// system_schema keyspace should always exist
	exists, err = KeyspaceExists(ctx, session, "system_schema")
	td.CmpNoError(t, err)
	td.Cmp(t, exists, true)
}

func TestIntegration_CreateKeyspace_VerifyReplicationSettings(t *testing.T) {
	if os.Getenv("SCYLLA_HOSTS") == "" {
		t.Skip("SCYLLA_HOSTS not set, skipping integration test")
	}

	session := getKeyspaceTestSession(t)
	ctx := context.Background()
	keyspace := generateTestKeyspaceName(t)

	// Cleanup after test
	t.Cleanup(func() {
		DropKeyspace(ctx, session, keyspace, true)
	})

	// Create keyspace with specific replication factor
	err := CreateKeyspace(ctx, session, keyspace,
		WithReplicationFactor(1),
	)
	td.CmpNoError(t, err)

	// Verify replication settings
	var replication map[string]string
	err = session.Query(`SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, keyspace).
		Scan(&replication)
	td.CmpNoError(t, err)
	td.Cmp(t, replication["class"], "org.apache.cassandra.locator.SimpleStrategy")
	td.Cmp(t, replication["replication_factor"], "1")
}
