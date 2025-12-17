package scyllamigrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

// ReplicationStrategy represents the keyspace replication strategy.
type ReplicationStrategy string

const (
	// SimpleStrategy is for single datacenter deployments.
	SimpleStrategy ReplicationStrategy = "SimpleStrategy"

	// NetworkTopologyStrategy is for multi-datacenter deployments.
	NetworkTopologyStrategy ReplicationStrategy = "NetworkTopologyStrategy"
)

// KeyspaceConfig holds configuration for creating a keyspace.
type KeyspaceConfig struct {
	// Name is the keyspace name (required).
	Name string

	// Strategy is the replication strategy (default: SimpleStrategy).
	Strategy ReplicationStrategy

	// ReplicationFactor is the replication factor for SimpleStrategy (default: 1).
	ReplicationFactor int

	// Datacenters maps datacenter names to replication factors for NetworkTopologyStrategy.
	// Example: {"dc1": 3, "dc2": 2}
	Datacenters map[string]int

	// DurableWrites enables durable writes (default: true).
	// Set to false for faster writes at the cost of durability.
	DurableWrites *bool

	// IfNotExists skips creation if keyspace already exists (default: true).
	IfNotExists bool
}

// KeyspaceOption configures a KeyspaceConfig.
type KeyspaceOption func(*KeyspaceConfig)

// WithReplicationFactor sets the replication factor for SimpleStrategy.
func WithReplicationFactor(factor int) KeyspaceOption {
	return func(c *KeyspaceConfig) {
		c.ReplicationFactor = factor
	}
}

// WithNetworkTopology configures NetworkTopologyStrategy with datacenter replication.
func WithNetworkTopology(datacenters map[string]int) KeyspaceOption {
	return func(c *KeyspaceConfig) {
		c.Strategy = NetworkTopologyStrategy
		c.Datacenters = datacenters
	}
}

// WithDurableWrites sets whether durable writes are enabled.
func WithDurableWrites(enabled bool) KeyspaceOption {
	return func(c *KeyspaceConfig) {
		c.DurableWrites = &enabled
	}
}

// WithIfNotExists sets whether to use IF NOT EXISTS clause.
func WithIfNotExists(ifNotExists bool) KeyspaceOption {
	return func(c *KeyspaceConfig) {
		c.IfNotExists = ifNotExists
	}
}

// CreateKeyspace creates a new keyspace with the specified configuration.
// The session should be connected without a keyspace (system keyspace is used).
func CreateKeyspace(ctx context.Context, session *gocql.Session, name string, opts ...KeyspaceOption) error {
	if session == nil {
		return ErrNoSession
	}

	if name == "" {
		return ErrNoKeyspace
	}

	// Default configuration.
	cfg := &KeyspaceConfig{
		Name:              name,
		Strategy:          SimpleStrategy,
		ReplicationFactor: 1,
		IfNotExists:       true,
	}

	// Apply options.
	for _, opt := range opts {
		opt(cfg)
	}

	// Build and execute the CQL statement.
	cql := buildCreateKeyspaceCQL(cfg)

	if err := session.Query(cql).WithContext(ctx).Exec(); err != nil {
		return &KeyspaceError{
			Keyspace: name,
			Op:       "create",
			Err:      err,
		}
	}

	// Wait for schema agreement.
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("failed to wait for schema agreement after keyspace creation: %w", err)
	}

	return nil
}

// KeyspaceExists checks if a keyspace exists.
func KeyspaceExists(ctx context.Context, session *gocql.Session, name string) (bool, error) {
	if session == nil {
		return false, ErrNoSession
	}

	if name == "" {
		return false, ErrNoKeyspace
	}

	var count int

	err := session.Query(`SELECT COUNT(*) FROM system_schema.keyspaces WHERE keyspace_name = ?`, name).
		WithContext(ctx).
		Scan(&count)
	if err != nil {
		return false, &KeyspaceError{
			Keyspace: name,
			Op:       "check existence",
			Err:      err,
		}
	}

	return count > 0, nil
}

// DropKeyspace drops a keyspace.
func DropKeyspace(ctx context.Context, session *gocql.Session, name string, ifExists bool) error {
	if session == nil {
		return ErrNoSession
	}

	if name == "" {
		return ErrNoKeyspace
	}

	var cql string
	if ifExists {
		cql = fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", name)
	} else {
		cql = fmt.Sprintf("DROP KEYSPACE %s", name)
	}

	if err := session.Query(cql).WithContext(ctx).Exec(); err != nil {
		return &KeyspaceError{
			Keyspace: name,
			Op:       "drop",
			Err:      err,
		}
	}

	// Wait for schema agreement.
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("failed to wait for schema agreement after keyspace drop: %w", err)
	}

	return nil
}

// buildCreateKeyspaceCQL builds the CREATE KEYSPACE CQL statement.
func buildCreateKeyspaceCQL(cfg *KeyspaceConfig) string {
	var sb strings.Builder

	sb.WriteString("CREATE KEYSPACE ")

	if cfg.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}

	sb.WriteString(cfg.Name)
	sb.WriteString(" WITH replication = {")

	switch cfg.Strategy {
	case NetworkTopologyStrategy:
		sb.WriteString("'class': 'NetworkTopologyStrategy'")

		for dc, rf := range cfg.Datacenters {
			sb.WriteString(fmt.Sprintf(", '%s': %d", dc, rf))
		}

	default: // SimpleStrategy
		sb.WriteString(fmt.Sprintf("'class': 'SimpleStrategy', 'replication_factor': %d", cfg.ReplicationFactor))
	}

	sb.WriteString("}")

	if cfg.DurableWrites != nil {
		sb.WriteString(fmt.Sprintf(" AND durable_writes = %t", *cfg.DurableWrites))
	}

	return sb.String()
}
