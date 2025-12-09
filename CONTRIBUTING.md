# Contributing to scyllamigrate

Thank you for your interest in contributing to scyllamigrate! This document provides guidelines and instructions for contributing to the project.

## Getting Started

### Prerequisites

- Go 1.25 or later
- A ScyllaDB instance (for integration tests)
- `golangci-lint` (for code quality checks)

### Setting Up Your Development Environment

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/scyllamigrate.git
   cd scyllamigrate
   ```

3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/heartwilltell/scyllamigrate.git
   ```

4. Install dependencies:
   ```bash
   go mod download
   ```

5. Install golangci-lint (if not already installed):
   ```bash
   go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
   ```

## Development Workflow

### Making Changes

1. Create a new branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. Make your changes following the coding standards (see below)

3. Write or update tests for your changes

4. Ensure all tests pass and linting checks pass

5. Commit your changes with clear, descriptive commit messages

6. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

7. Create a Pull Request on GitHub

## Coding Standards

### Code Style

- Follow standard Go formatting conventions (`gofmt` / `go fmt`)
- Follow the project's linting rules (configured in `.golangci.yml`)
- Use meaningful variable and function names
- Add comments for exported functions, types, and packages
- Keep functions focused and small

### Running Linters

Before submitting a PR, ensure your code passes all linting checks:

```bash
golangci-lint run
```

The project uses a comprehensive set of linters including:
- `errcheck` - Checks for unchecked errors
- `gosec` - Security vulnerability scanner
- `staticcheck` - Advanced static analysis
- `revive` - Fast, configurable linter
- And many more (see `.golangci.yml` for full list)

### Testing

#### Unit Tests

Run all unit tests:

```bash
go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
```

Run tests for a specific package:

```bash
go test -v ./migrator
```

#### Integration Tests

Integration tests require a running ScyllaDB instance. They are controlled by environment variables (`SCYLLA_HOSTS` and `SCYLLA_KEYSPACE`) and will be automatically skipped if these are not set.

**Using Docker Compose (Recommended):**

```bash
# Start ScyllaDB using docker-compose
docker-compose up -d

# Wait for ScyllaDB to be ready (docker-compose healthcheck handles this)
# Then run integration tests with environment variables
SCYLLA_HOSTS=localhost:9042 SCYLLA_KEYSPACE=test_migrations go test -v ./...

# Stop ScyllaDB when done
docker-compose down
```

**Using Docker directly:**

```bash
# Start ScyllaDB in Docker
docker run -d -p 9042:9042 --name scylla scylladb/scylla:latest

# Wait for ScyllaDB to be ready
docker exec scylla cqlsh -e "SELECT now() FROM system.local"

# Create test keyspace
docker exec scylla cqlsh -e "CREATE KEYSPACE IF NOT EXISTS test_migrations WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Run integration tests
SCYLLA_HOSTS=localhost:9042 SCYLLA_KEYSPACE=test_migrations go test -v ./...

# Clean up
docker stop scylla && docker rm scylla
```

**Note:** Integration tests are automatically skipped if `SCYLLA_HOSTS` or `SCYLLA_KEYSPACE` environment variables are not set. This allows unit tests to run without requiring a ScyllaDB instance.

#### Test Coverage

The project aims to maintain high test coverage. Check coverage:

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Pull Request Guidelines

### Before Submitting

- [ ] All tests pass locally
- [ ] Code passes linting checks (`golangci-lint run`)
- [ ] Code is properly formatted (`go fmt ./...`)
- [ ] New features include tests
- [ ] Documentation is updated if needed
- [ ] Commit messages are clear and descriptive

### PR Description

When creating a Pull Request, please include:

- A clear description of what the PR does
- Reference to any related issues
- Any breaking changes
- Testing instructions if applicable

### Commit Messages

Follow these guidelines for commit messages:

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

Example:
```
Add support for custom migration sources

This change allows users to implement their own migration source
by implementing the Source interface. This enables loading
migrations from databases, remote APIs, or other custom sources.

Fixes #123
```

## Project Structure

```
scyllamigrate/
├── cmd/              # CLI tool implementation
├── brew/             # Homebrew formula
├── *.go              # Core library code
├── *_test.go         # Test files
├── .golangci.yml     # Linter configuration
├── go.mod            # Go module definition
└── README.md         # Project documentation
```

## Areas for Contribution

We welcome contributions in the following areas:

- **Bug fixes**: Fix issues reported in GitHub Issues
- **New features**: Propose and implement new functionality
- **Documentation**: Improve README, code comments, or add examples
- **Tests**: Increase test coverage or add missing test cases
- **Performance**: Optimize existing code
- **Code quality**: Refactor and improve code structure

## Reporting Issues

When reporting issues, please include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Environment details (Go version, OS, ScyllaDB version)
- Any relevant error messages or logs

## Code Review Process

1. All PRs require at least one approval before merging
2. Maintainers will review your code and may request changes
3. Address review comments promptly
4. Once approved, a maintainer will merge your PR

## Questions?

If you have questions about contributing, feel free to:

- Open an issue on GitHub
- Check existing issues and discussions

Thank you for contributing to scyllamigrate!

