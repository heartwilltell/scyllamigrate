package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/heartwilltell/scotty"
	"github.com/heartwilltell/scyllamigrate"
)

// default chmod permissions.
const (
	migrationsDirMode os.FileMode = 0o755
	migrationFileMode os.FileMode = 0o600
)

// config holds global configuration flags.
type config struct {
	hosts       string
	keyspace    string
	dir         string
	consistency string
	timeout     time.Duration
	table       string
	datacenter  string
}

// Global configuration flags.
var cfg config

type managedMigrator struct {
	*scyllamigrate.Migrator
	cleanup func()
}

func (m *managedMigrator) Close() {
	if m.cleanup != nil {
		m.cleanup()
	}
}

func main() {
	rootCmd := &scotty.Command{
		Name:  "scyllamigrate",
		Short: "ScyllaDB schema migration tool",
		Long:  "A tool for managing ScyllaDB schema migrations with up/down support.",
		SetFlags: func(f *scotty.FlagSet) {
			f.StringVarE(&cfg.hosts, "hosts", "SCYLLA_HOSTS", "localhost:9042",
				"Comma-separated list of ScyllaDB hosts",
			)
			f.StringVarE(&cfg.keyspace, "keyspace", "SCYLLA_KEYSPACE", "",
				"Target keyspace (required)",
			)
			f.StringVarE(&cfg.dir, "dir", "MIGRATIONS_DIR", "./migrations",
				"Migrations directory",
			)
			f.StringVarE(&cfg.consistency, "consistency", "SCYLLA_CONSISTENCY", "quorum",
				"Consistency level (any, one, two, three, quorum, all, local_quorum, each_quorum, local_one)",
			)
			f.DurationVarE(&cfg.timeout, "timeout", "SCYLLA_TIMEOUT", 30*time.Second,
				"Operation timeout",
			)
			f.StringVarE(&cfg.table, "table", "SCYLLA_MIGRATIONS_TABLE", "schema_migrations",
				"Migration history table name",
			)
			f.StringVarE(&cfg.datacenter, "datacenter", "SCYLLA_DATACENTER", "",
				"Local datacenter for DC-aware routing (enables TokenAwareHostPolicy with DCAwareRoundRobinPolicy)",
			)
		},
	}

	rootCmd.AddSubcommands(
		upCmd(),
		downCmd(),
		statusCmd(),
		createCmd(),
		versionCmd(),
		createKeyspaceCmd(),
	)

	if err := rootCmd.Exec(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func upCmd() *scotty.Command {
	var steps int

	return &scotty.Command{
		Name:  "up",
		Short: "Apply pending migrations",
		Long:  "Apply all pending migrations or a specific number of migrations.",
		SetFlags: func(f *scotty.FlagSet) {
			f.IntVar(&steps, "n", 0, "Number of migrations to apply (0 = all)")
		},
		Run: func(_ *scotty.Command, _ []string) error {
			migrator, err := createMigrator()
			if err != nil {
				return err
			}
			defer migrator.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()

			var applied int

			switch {
			case steps > 0:
				if err := migrator.Steps(ctx, steps); err != nil {
					return err
				}

				applied = steps

			default:
				applied, err = migrator.Up(ctx)
				if err != nil {
					return err
				}
			}

			if applied == 0 {
				fmt.Println("No migrations to apply")

				return nil
			}

			fmt.Printf("Applied %d migration(s)\n", applied)

			return nil
		},
	}
}

func downCmd() *scotty.Command {
	var steps int

	return &scotty.Command{
		Name:  "down",
		Short: "Rollback migrations",
		Long:  "Rollback the last migration or a specific number of migrations.",
		SetFlags: func(f *scotty.FlagSet) {
			f.IntVar(&steps, "n", 1, "Number of migrations to rollback")
		},
		Run: func(_ *scotty.Command, _ []string) error {
			migrator, err := createMigrator()
			if err != nil {
				return err
			}
			defer migrator.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()

			if err := migrator.Steps(ctx, -steps); err != nil {
				if errors.Is(err, scyllamigrate.ErrNoChange) {
					fmt.Println("No migrations to rollback")

					return nil
				}

				return err
			}

			fmt.Printf("Rolled back %d migration(s)\n", steps)

			return nil
		},
	}
}

func statusCmd() *scotty.Command {
	return &scotty.Command{
		Name:  "status",
		Short: "Show migration status",
		Long:  "Display the current migration status including applied and pending migrations.",
		Run: func(_ *scotty.Command, _ []string) error {
			migrator, err := createMigrator()
			if err != nil {
				return err
			}
			defer migrator.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()

			status, err := migrator.Status(ctx)
			if err != nil {
				return err
			}

			fmt.Printf("Current Version: %d\n\n", status.CurrentVersion)

			if len(status.Applied) > 0 {
				fmt.Println("Applied Migrations:")
				fmt.Println("-------------------")

				for _, m := range status.Applied {
					fmt.Printf("  [%d] %s (applied at %s, took %dms)\n",
						m.Version, m.Description, m.AppliedAt.Format(time.RFC3339), m.ExecutionMs)
				}

				fmt.Println()
			}

			if len(status.Pending) > 0 {
				fmt.Println("Pending Migrations:")
				fmt.Println("-------------------")

				for _, m := range status.Pending {
					fmt.Printf("  [%d] %s\n", m.Version, m.Description)
				}
			} else {
				fmt.Println("No pending migrations")
			}

			return nil
		},
	}
}

func createCmd() *scotty.Command {
	var ext string

	return &scotty.Command{
		Name:  "create",
		Short: "Create new migration files",
		Long:  "Create a new pair of up/down migration files with the next sequential version number.",
		SetFlags: func(f *scotty.FlagSet) {
			f.StringVar(&ext, "ext", "cql", "File extension (cql or sql)")
		},
		Run: func(_ *scotty.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("migration name is required")
			}

			name := args[0]

			// Validate extension.
			if ext != "cql" && ext != "sql" {
				return fmt.Errorf("invalid extension: %s (must be cql or sql)", ext)
			}

			// Ensure migrations directory exists.
			if err := os.MkdirAll(cfg.dir, migrationsDirMode); err != nil {
				return fmt.Errorf("failed to create migrations directory: %w", err)
			}

			// Find the next version number.
			nextVersion, err := findNextVersion(cfg.dir)
			if err != nil {
				return err
			}

			// Create filenames.
			upFile := fmt.Sprintf("%06d_%s.up.%s", nextVersion, name, ext)
			downFile := fmt.Sprintf("%06d_%s.down.%s", nextVersion, name, ext)

			upPath := filepath.Join(cfg.dir, upFile)
			downPath := filepath.Join(cfg.dir, downFile)

			// Create up migration file.
			if err := os.WriteFile(upPath, []byte("-- Migration: "+name+" (up)\n\n"), migrationFileMode); err != nil {
				return fmt.Errorf("failed to create up migration: %w", err)
			}

			// Create down migration file.
			if err := os.WriteFile(downPath, []byte("-- Migration: "+name+" (down)\n\n"), migrationFileMode); err != nil {
				return fmt.Errorf("failed to create down migration: %w", err)
			}

			fmt.Println("Created migration files:")
			fmt.Printf("  %s\n", upPath)
			fmt.Printf("  %s\n", downPath)

			return nil
		},
	}
}

func versionCmd() *scotty.Command {
	return &scotty.Command{
		Name:  "version",
		Short: "Show current migration version",
		Long:  "Display the current migration version number.",
		Run: func(_ *scotty.Command, _ []string) error {
			migrator, err := createMigrator()
			if err != nil {
				return err
			}
			defer migrator.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()

			version, err := migrator.Version(ctx)
			if err != nil {
				return err
			}

			if version == 0 {
				fmt.Println("No migrations applied")
			} else {
				fmt.Printf("Current version: %d\n", version)
			}

			return nil
		},
	}
}

// createMigrator creates a new Migrator instance with the configured options.
// Returns a managed migrator with a cleanup hook.
func createMigrator() (*managedMigrator, error) {
	if cfg.keyspace == "" {
		return nil, errors.New("keyspace is required (use -keyspace or SCYLLA_KEYSPACE)")
	}

	// Parse hosts.
	hostList := strings.Split(cfg.hosts, ",")
	for i := range hostList {
		hostList[i] = strings.TrimSpace(hostList[i])
	}

	// Create cluster configuration.
	cluster := gocql.NewCluster(hostList...)
	cluster.Keyspace = cfg.keyspace
	cluster.Consistency = parseConsistency(cfg.consistency)
	cluster.Timeout = cfg.timeout

	// Configure datacenter-aware routing if datacenter is specified.
	if cfg.datacenter != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.DCAwareRoundRobinPolicy(cfg.datacenter),
		)
	}

	// Create session.
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	// Create migrator.
	migrator, err := scyllamigrate.New(session,
		scyllamigrate.WithDir(cfg.dir),
		scyllamigrate.WithKeyspace(cfg.keyspace),
		scyllamigrate.WithHistoryTable(cfg.table),
		scyllamigrate.WithConsistency(parseConsistency(cfg.consistency)),
		scyllamigrate.WithStdLogger(nil), // Use default logger.
	)
	if err != nil {
		session.Close()
		return nil, err
	}

	cleanup := func() {
		migrator.Close()
		session.Close()
	}

	return &managedMigrator{
		Migrator: migrator,
		cleanup:  cleanup,
	}, nil
}

// parseConsistency converts a string to gocql.Consistency.
func parseConsistency(s string) gocql.Consistency {
	switch strings.ToLower(s) {
	case "any":
		return gocql.Any
	case "one":
		return gocql.One
	case "two":
		return gocql.Two
	case "three":
		return gocql.Three
	case "all":
		return gocql.All
	case "local_quorum", "localquorum":
		return gocql.LocalQuorum
	case "each_quorum", "eachquorum":
		return gocql.EachQuorum
	case "local_one", "localone":
		return gocql.LocalOne
	}

	return gocql.Quorum
}

func createKeyspaceCmd() *scotty.Command {
	var (
		replicationFactor int
		networkTopology   string
		durableWrites     bool
		ifNotExists       bool
	)

	return &scotty.Command{
		Name:  "create-keyspace",
		Short: "Create a new keyspace",
		Long: `Create a new keyspace with the specified replication settings.

Examples:
  # Create keyspace with SimpleStrategy (default)
  scyllamigrate create-keyspace -keyspace myapp -rf 3

  # Create keyspace with NetworkTopologyStrategy
  scyllamigrate create-keyspace -keyspace myapp -network-topology "dc1:3,dc2:2"

  # Create keyspace without durable writes (for testing)
  scyllamigrate create-keyspace -keyspace myapp -durable-writes=false`,
		SetFlags: func(f *scotty.FlagSet) {
			f.IntVar(&replicationFactor, "rf", 1, "Replication factor for SimpleStrategy")
			f.StringVar(&networkTopology, "network-topology", "",
				"Datacenter replication for NetworkTopologyStrategy (format: dc1:rf1,dc2:rf2)")
			f.BoolVar(&durableWrites, "durable-writes", true, "Enable durable writes")
			f.BoolVar(&ifNotExists, "if-not-exists", true, "Only create if keyspace doesn't exist")
		},
		Run: func(_ *scotty.Command, _ []string) error {
			if cfg.keyspace == "" {
				return errors.New("keyspace is required (use -keyspace or SCYLLA_KEYSPACE)")
			}

			// Parse hosts.
			hostList := strings.Split(cfg.hosts, ",")
			for i := range hostList {
				hostList[i] = strings.TrimSpace(hostList[i])
			}

			// Create cluster configuration without keyspace.
			cluster := gocql.NewCluster(hostList...)
			cluster.Consistency = parseConsistency(cfg.consistency)
			cluster.Timeout = cfg.timeout

			// Configure datacenter-aware routing if datacenter is specified.
			if cfg.datacenter != "" {
				cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
					gocql.DCAwareRoundRobinPolicy(cfg.datacenter),
				)
			}

			// Create session.
			session, err := cluster.CreateSession()
			if err != nil {
				return fmt.Errorf("failed to connect to ScyllaDB: %w", err)
			}
			defer session.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
			defer cancel()

			// Build keyspace options.
			var opts []scyllamigrate.KeyspaceOption

			if networkTopology != "" {
				datacenters, err := parseNetworkTopology(networkTopology)
				if err != nil {
					return err
				}

				opts = append(opts, scyllamigrate.WithNetworkTopology(datacenters))
			} else {
				opts = append(opts, scyllamigrate.WithReplicationFactor(replicationFactor))
			}

			opts = append(opts, scyllamigrate.WithDurableWrites(durableWrites))
			opts = append(opts, scyllamigrate.WithIfNotExists(ifNotExists))

			// Create keyspace.
			if err := scyllamigrate.CreateKeyspace(ctx, session, cfg.keyspace, opts...); err != nil {
				return err
			}

			fmt.Printf("Keyspace %q created successfully\n", cfg.keyspace)

			return nil
		},
	}
}

// parseNetworkTopology parses a network topology string in format "dc1:rf1,dc2:rf2".
func parseNetworkTopology(s string) (map[string]int, error) {
	result := make(map[string]int)

	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid network topology format %q: expected dc:rf", pair)
		}

		dc := strings.TrimSpace(parts[0])
		if dc == "" {
			return nil, fmt.Errorf("invalid network topology: empty datacenter name")
		}

		var rf int
		if _, err := fmt.Sscanf(parts[1], "%d", &rf); err != nil {
			return nil, fmt.Errorf("invalid replication factor %q for datacenter %q", parts[1], dc)
		}

		if rf < 1 {
			return nil, fmt.Errorf("replication factor must be at least 1, got %d for datacenter %q", rf, dc)
		}

		result[dc] = rf
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("network topology must specify at least one datacenter")
	}

	return result, nil
}

// findNextVersion scans the migrations directory and returns the next version number.
func findNextVersion(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}

		return 0, err
	}

	var maxVersion uint64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		m, err := scyllamigrate.ParseMigration(entry.Name())
		if err != nil {
			continue
		}

		if m.Version > maxVersion {
			maxVersion = m.Version
		}
	}

	return maxVersion + 1, nil
}
