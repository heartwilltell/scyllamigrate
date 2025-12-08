# scyllamigrate

A lightweight, feature-rich schema migration library for ScyllaDB written in Go. Supports embedded migrations via `go:embed`, filesystem-based migrations, and provides both a programmatic API and CLI tool.

## Features

- **Multiple migration sources**: Load migrations from embedded `fs.FS` or filesystem directories
- **Bidirectional migrations**: Full support for UP and DOWN migrations
- **Sequential versioning**: Simple numeric versioning (`000001_create_users.up.cql`)
- **Multiple file extensions**: Supports both `.cql` and `.sql` files
- **Multi-statement migrations**: Execute multiple CQL statements per migration file
- **Schema agreement**: Automatically waits for ScyllaDB schema agreement after DDL operations
- **Checksum tracking**: Detects modified migration files
- **CLI tool**: Full-featured command-line interface for managing migrations
- **Programmatic API**: Clean Go API with functional options pattern

## Installation

### Library

```bash
go get github.com/heartwilltell/scyllamigrate
```

### CLI Tool

```bash
go install github.com/heartwilltell/scyllamigrate/cmd/scyllamigrate@latest
```

## Quick Start

### 1. Create Migration Files

Create a `migrations` directory and add your first migration:

```bash
mkdir migrations
```

**migrations/000001_create_users.up.cql**
```cql
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS users_email_idx ON users (email);
```

**migrations/000001_create_users.down.cql**
```cql
DROP INDEX IF EXISTS users_email_idx;
DROP TABLE IF EXISTS users;
```

### 2. Run Migrations

#### Using the CLI

```bash
# Apply all pending migrations
scyllamigrate up --hosts=localhost:9042 --keyspace=myapp --dir=./migrations

# Check migration status
scyllamigrate status --hosts=localhost:9042 --keyspace=myapp

# Rollback the last migration
scyllamigrate down --hosts=localhost:9042 --keyspace=myapp
```

#### Using the Go API

```go
package main

import (
    "context"
    "log"
    "log/slog"

    "github.com/gocql/gocql"
    "github.com/heartwilltell/scyllamigrate"
)

func main() {
    // Create ScyllaDB session
    cluster := gocql.NewCluster("localhost:9042")
    cluster.Keyspace = "myapp"
    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()

    // Create migrator
    migrator, err := scyllamigrate.New(session,
        scyllamigrate.WithDir("./migrations"),
        scyllamigrate.WithKeyspace("myapp"),
        scyllamigrate.WithLogger(slog.Default()), // Optional: progress logging
    )
    if err != nil {
        log.Fatal(err)
    }
    defer migrator.Close()

    // Apply all pending migrations
    applied, err := migrator.Up(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Applied %d migrations", applied)
}
```

## Embedded Migrations

For production deployments, embed migrations directly into your binary:

```go
package main

import (
    "context"
    "embed"
    "log"

    "github.com/gocql/gocql"
    "github.com/heartwilltell/scyllamigrate"
)

//go:embed migrations/*.cql
var migrations embed.FS

func main() {
    cluster := gocql.NewCluster("localhost:9042")
    cluster.Keyspace = "myapp"
    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()

    // Use embedded migrations
    migrator, err := scyllamigrate.New(session,
        scyllamigrate.WithFS(migrations),
        scyllamigrate.WithKeyspace("myapp"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer migrator.Close()

    if _, err := migrator.Up(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Migration File Naming Convention

Migration files must follow this naming pattern:

```
{version}_{description}.{direction}.{extension}
```

- **version**: Sequential number (e.g., `000001`, `000002`, or `1`, `2`)
- **description**: Human-readable description using underscores (e.g., `create_users`)
- **direction**: Either `up` or `down`
- **extension**: Either `cql` or `sql`

### Examples

```
000001_create_users.up.cql
000001_create_users.down.cql
000002_add_email_index.up.sql
000002_add_email_index.down.sql
```

## CLI Reference

### Global Flags

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--hosts` | `SCYLLA_HOSTS` | `localhost:9042` | Comma-separated list of ScyllaDB hosts |
| `--keyspace` | `SCYLLA_KEYSPACE` | (required) | Target keyspace |
| `--dir` | `MIGRATIONS_DIR` | `./migrations` | Migrations directory |
| `--consistency` | `SCYLLA_CONSISTENCY` | `quorum` | Consistency level |
| `--timeout` | `SCYLLA_TIMEOUT` | `30s` | Operation timeout |
| `--table` | `SCYLLA_MIGRATIONS_TABLE` | `schema_migrations` | Migration history table name |

### Commands

#### `up` - Apply Migrations

Apply all pending migrations or a specific number:

```bash
# Apply all pending migrations
scyllamigrate up --keyspace=myapp

# Apply next 3 migrations
scyllamigrate up -n 3 --keyspace=myapp
```

#### `down` - Rollback Migrations

Rollback the last migration or a specific number:

```bash
# Rollback the last migration
scyllamigrate down --keyspace=myapp

# Rollback last 3 migrations
scyllamigrate down -n 3 --keyspace=myapp
```

#### `status` - Show Migration Status

Display applied and pending migrations:

```bash
scyllamigrate status --keyspace=myapp
```

Output:
```
Current Version: 3

Applied Migrations:
-------------------
  [1] create_users (applied at 2024-01-15T10:30:00Z, took 45ms)
  [2] add_email_index (applied at 2024-01-15T10:30:01Z, took 23ms)
  [3] create_posts (applied at 2024-01-15T10:30:02Z, took 38ms)

No pending migrations
```

#### `create` - Create Migration Files

Generate a new migration file pair:

```bash
# Create with .cql extension (default)
scyllamigrate create add_comments_table --dir=./migrations

# Create with .sql extension
scyllamigrate create add_comments_table --ext=sql --dir=./migrations
```

This creates:
```
migrations/000004_add_comments_table.up.cql
migrations/000004_add_comments_table.down.cql
```

#### `version` - Show Current Version

Display the current migration version:

```bash
scyllamigrate version --keyspace=myapp
```

## Programmatic API

### Creating a Migrator

```go
migrator, err := scyllamigrate.New(session,
    scyllamigrate.WithDir("./migrations"),           // Load from directory
    // OR
    scyllamigrate.WithFS(embeddedFS),                // Load from embed.FS
    // OR
    scyllamigrate.WithSource(customSource),          // Use custom source

    scyllamigrate.WithKeyspace("myapp"),             // Required: target keyspace
    scyllamigrate.WithHistoryTable("migrations"),    // Optional: custom table name
    scyllamigrate.WithConsistency(gocql.Quorum),     // Optional: consistency level
    scyllamigrate.WithLogger(slog.Default()),        // Optional: progress logging (slog.Logger)
    scyllamigrate.WithSchemaAgreement(true),         // Optional: wait for schema agreement
)
```

### Available Methods

```go
// Apply all pending migrations, returns count of applied migrations
applied, err := migrator.Up(ctx)

// Apply migrations up to a specific version
applied, err := migrator.UpTo(ctx, 5)

// Rollback the last migration
err := migrator.Down(ctx)

// Rollback to a specific version (exclusive)
rolledBack, err := migrator.DownTo(ctx, 3)

// Apply or rollback N migrations (positive = up, negative = down)
err := migrator.Steps(ctx, 3)   // Apply 3
err := migrator.Steps(ctx, -2)  // Rollback 2

// Get current version (0 if no migrations applied)
version, err := migrator.Version(ctx)

// Get full migration status
status, err := migrator.Status(ctx)
// status.CurrentVersion - current version number
// status.Applied - slice of applied migrations
// status.Pending - slice of pending migrations

// Get pending migrations
pending, err := migrator.Pending(ctx)

// Get applied migrations
applied, err := migrator.Applied(ctx)

// Clean up resources
err := migrator.Close()
```

## Multi-Statement Migrations

A single migration file can contain multiple CQL statements separated by semicolons:

```cql
-- Create the users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT,
    name TEXT
);

-- Create an index on email
CREATE INDEX IF NOT EXISTS users_email_idx ON users (email);

-- Create a materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS users_by_email AS
    SELECT * FROM users
    WHERE email IS NOT NULL
    PRIMARY KEY (email, id);
```

Comments (lines starting with `--`) are automatically skipped.

## Migration History Table

Scyllamigrate automatically creates and manages a history table to track applied migrations:

```cql
CREATE TABLE IF NOT EXISTS {keyspace}.schema_migrations (
    version bigint PRIMARY KEY,
    description text,
    checksum text,
    applied_at timestamp,
    execution_ms bigint
)
```

## Custom Migration Source

Implement the `Source` interface for custom migration sources:

```go
type Source interface {
    List() ([]*MigrationPair, error)
    ReadUp(version uint64) (io.ReadCloser, error)
    ReadDown(version uint64) (io.ReadCloser, error)
    Close() error
}
```

## Error Handling

The library provides specific error types for different scenarios:

```go
import "github.com/heartwilltell/scyllamigrate"

_, err := migrator.Up(ctx)
if err != nil {
    switch {
    case errors.Is(err, scyllamigrate.ErrNoChange):
        // No pending migrations
    case errors.Is(err, scyllamigrate.ErrMissingDown):
        // Down migration file not found
    case errors.Is(err, scyllamigrate.ErrMissingUp):
        // Up migration file not found
    default:
        // Check for migration execution errors
        var migErr *scyllamigrate.MigrationError
        if errors.As(err, &migErr) {
            log.Printf("Migration %d failed at statement %d: %v",
                migErr.Version, migErr.Statement, migErr.Err)
        }
    }
}
```

## Best Practices

### 1. Use Idempotent Statements

Always use `IF NOT EXISTS` and `IF EXISTS` clauses:

```cql
-- Good
CREATE TABLE IF NOT EXISTS users (...);
DROP TABLE IF EXISTS users;

-- Avoid
CREATE TABLE users (...);
DROP TABLE users;
```

### 2. Keep Migrations Small and Focused

Each migration should do one thing:

```
000001_create_users.up.cql      -- Create users table
000002_add_users_email_idx.up.cql  -- Add email index
000003_create_posts.up.cql      -- Create posts table
```

### 3. Always Provide Down Migrations

Even if you don't plan to rollback, having down migrations helps with:
- Development and testing
- Disaster recovery
- Schema verification

### 4. Test Migrations Before Production

```bash
# Apply migrations
scyllamigrate up --keyspace=myapp_test

# Verify state

# Rollback all
scyllamigrate down -n 100 --keyspace=myapp_test

# Apply again to verify idempotency
scyllamigrate up --keyspace=myapp_test
```

### 5. Use Environment Variables in Production

```bash
export SCYLLA_HOSTS=node1:9042,node2:9042,node3:9042
export SCYLLA_KEYSPACE=production
export SCYLLA_CONSISTENCY=local_quorum

scyllamigrate up
```

## ScyllaDB-Specific Considerations

### Schema Agreement

By default, scyllamigrate waits for schema agreement after executing DDL statements. This ensures all nodes in the cluster have the updated schema before proceeding. Disable this for faster migrations in development:

```go
scyllamigrate.WithSchemaAgreement(false)
```

### Consistency Levels

For production migrations, use appropriate consistency levels:

```go
scyllamigrate.WithConsistency(gocql.LocalQuorum)
```

### Keyspace Must Exist

The target keyspace must exist before running migrations. Create it manually or use a bootstrap migration:

```cql
-- Run this before using scyllamigrate
CREATE KEYSPACE IF NOT EXISTS myapp
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```

## License

MIT License - see [LICENSE](LICENSE) for details.
