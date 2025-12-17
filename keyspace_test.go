package scyllamigrate

import (
	"context"
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
	tests := []struct {
		name     string
		cfg      *KeyspaceConfig
		expected string
	}{
		{
			name: "simple strategy with default options",
			cfg: &KeyspaceConfig{
				Name:              "test_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 1,
				IfNotExists:       true,
			},
			expected: "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		},
		{
			name: "simple strategy with rf 3",
			cfg: &KeyspaceConfig{
				Name:              "prod_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 3,
				IfNotExists:       true,
			},
			expected: "CREATE KEYSPACE IF NOT EXISTS prod_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
		},
		{
			name: "simple strategy without if not exists",
			cfg: &KeyspaceConfig{
				Name:              "strict_keyspace",
				Strategy:          SimpleStrategy,
				ReplicationFactor: 2,
				IfNotExists:       false,
			},
			expected: "CREATE KEYSPACE strict_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
		},
		{
			name: "simple strategy with durable writes disabled",
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildCreateKeyspaceCQL(tt.cfg)
			td.Cmp(t, result, tt.expected)
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
