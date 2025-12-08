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

// Global configuration flags
var (
	hosts       string
	keyspace    string
	dir         string
	consistency string
	timeout     time.Duration
	table       string
)

func main() {
	rootCmd := &scotty.Command{
		Name:  "scyllamigrate",
		Short: "ScyllaDB schema migration tool",
		Long:  "A tool for managing ScyllaDB schema migrations with up/down support.",
		SetFlags: func(f *scotty.FlagSet) {
			f.StringVarE(&hosts, "hosts", "SCYLLA_HOSTS", "localhost:9042", "Comma-separated list of ScyllaDB hosts")
			f.StringVarE(&keyspace, "keyspace", "SCYLLA_KEYSPACE", "", "Target keyspace (required)")
			f.StringVarE(&dir, "dir", "MIGRATIONS_DIR", "./migrations", "Migrations directory")
			f.StringVarE(&consistency, "consistency", "SCYLLA_CONSISTENCY", "quorum", "Consistency level (any, one, two, three, quorum, all, local_quorum, each_quorum, local_one)")
			f.DurationVarE(&timeout, "timeout", "SCYLLA_TIMEOUT", 30*time.Second, "Operation timeout")
			f.StringVarE(&table, "table", "SCYLLA_MIGRATIONS_TABLE", "schema_migrations", "Migration history table name")
		},
	}

	rootCmd.AddSubcommands(
		upCmd(),
		downCmd(),
		statusCmd(),
		createCmd(),
		versionCmd(),
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
		Run: func(cmd *scotty.Command, args []string) error {
			migrator, cleanup, err := createMigrator()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			var applied int
			if steps > 0 {
				if err := migrator.Steps(ctx, steps); err != nil {
					return err
				}
				applied = steps
			} else {
				applied, err = migrator.Up(ctx)
				if err != nil {
					return err
				}
			}

			if applied == 0 {
				fmt.Println("No migrations to apply")
			} else {
				fmt.Printf("Applied %d migration(s)\n", applied)
			}

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
		Run: func(cmd *scotty.Command, args []string) error {
			migrator, cleanup, err := createMigrator()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		Run: func(cmd *scotty.Command, args []string) error {
			migrator, cleanup, err := createMigrator()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		Run: func(cmd *scotty.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("migration name is required")
			}

			name := args[0]

			// Validate extension
			if ext != "cql" && ext != "sql" {
				return fmt.Errorf("invalid extension: %s (must be cql or sql)", ext)
			}

			// Ensure migrations directory exists
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create migrations directory: %w", err)
			}

			// Find the next version number
			nextVersion, err := findNextVersion(dir)
			if err != nil {
				return err
			}

			// Create filenames
			upFile := fmt.Sprintf("%06d_%s.up.%s", nextVersion, name, ext)
			downFile := fmt.Sprintf("%06d_%s.down.%s", nextVersion, name, ext)

			upPath := filepath.Join(dir, upFile)
			downPath := filepath.Join(dir, downFile)

			// Create up migration file
			if err := os.WriteFile(upPath, []byte("-- Migration: "+name+" (up)\n\n"), 0644); err != nil {
				return fmt.Errorf("failed to create up migration: %w", err)
			}

			// Create down migration file
			if err := os.WriteFile(downPath, []byte("-- Migration: "+name+" (down)\n\n"), 0644); err != nil {
				return fmt.Errorf("failed to create down migration: %w", err)
			}

			fmt.Printf("Created migration files:\n")
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
		Run: func(cmd *scotty.Command, args []string) error {
			migrator, cleanup, err := createMigrator()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
// Returns the migrator, a cleanup function, and any error.
func createMigrator() (*scyllamigrate.Migrator, func(), error) {
	if keyspace == "" {
		return nil, nil, fmt.Errorf("keyspace is required (use --keyspace or SCYLLA_KEYSPACE)")
	}

	// Parse hosts
	hostList := strings.Split(hosts, ",")
	for i := range hostList {
		hostList[i] = strings.TrimSpace(hostList[i])
	}

	// Create cluster configuration
	cluster := gocql.NewCluster(hostList...)
	cluster.Keyspace = keyspace
	cluster.Consistency = parseConsistency(consistency)
	cluster.Timeout = timeout

	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	// Create migrator
	migrator, err := scyllamigrate.New(session,
		scyllamigrate.WithDir(dir),
		scyllamigrate.WithKeyspace(keyspace),
		scyllamigrate.WithHistoryTable(table),
		scyllamigrate.WithConsistency(parseConsistency(consistency)),
		scyllamigrate.WithStdLogger(nil), // Use default logger
	)
	if err != nil {
		session.Close()
		return nil, nil, err
	}

	cleanup := func() {
		migrator.Close()
		session.Close()
	}

	return migrator, cleanup, nil
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
	case "quorum":
		return gocql.Quorum
	case "all":
		return gocql.All
	case "local_quorum", "localquorum":
		return gocql.LocalQuorum
	case "each_quorum", "eachquorum":
		return gocql.EachQuorum
	case "local_one", "localone":
		return gocql.LocalOne
	default:
		return gocql.Quorum
	}
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
