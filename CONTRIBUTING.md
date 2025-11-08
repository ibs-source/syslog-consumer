# Contributing to Syslog Consumer

Thank you for your interest in contributing to the Syslog Consumer project! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Documentation](#documentation)

## Code of Conduct

### Our Standards

- **Be respectful**: Treat all contributors with respect and professionalism
- **Be collaborative**: Work together towards common goals
- **Be constructive**: Provide helpful feedback and suggestions
- **Be inclusive**: Welcome contributors of all experience levels

## Getting Started

### Prerequisites

- **Go 1.24+**: Install from [go.dev](https://go.dev)
- **Git**: For version control
- **Redis**: For integration testing
- **MQTT Broker**: For integration testing (Mosquitto recommended)

### Fork and Clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/syslog-consumer.git
cd syslog-consumer

# Add upstream remote
git remote add upstream https://github.com/ibs-source/syslog-consumer.git
```

### Build and Test

```bash
# Install dependencies
go mod download

# Build the project
go build -o bin/consumer ./cmd/consumer

# Run tests
go test ./...

# Run tests with coverage
go test -cover ./...
```

## Development Workflow

### 1. Create a Branch

```bash
# Update your fork
git fetch upstream
git checkout main
git merge upstream/main

# Create a feature branch
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write clean, idiomatic Go code
- Follow existing code patterns
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run tests with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 4. Format and Lint

```bash
# Format code
go fmt ./...

# Run static analysis
go vet ./...

# Run linter (if golangci-lint is installed)
golangci-lint run
```

## Coding Standards

### Go Style Guide

Follow the official [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) and [Effective Go](https://go.dev/doc/effective_go).

### Key Principles

1. **Simplicity**: Favor simple, clear code over clever solutions
2. **Readability**: Code is read more often than written
3. **Error Handling**: Always check and handle errors appropriately
4. **Documentation**: Document exported functions, types, and packages
5. **Testing**: Write tests for all new functionality

### Code Structure

```go
// Package comment describing the package purpose
package mypackage

import (
    // Standard library imports first
    "context"
    "fmt"
    
    // Third-party imports
    "github.com/external/package"
    
    // Local imports
    "github.com/ibs-source/syslog-consumer/internal/config"
)

// ExportedType describes what this type does.
// It provides detailed information about usage.
type ExportedType struct {
    field1 string // Unexported fields use camelCase
    field2 int
}

// NewExportedType creates a new ExportedType.
// Constructor functions should validate inputs.
func NewExportedType(field1 string, field2 int) (*ExportedType, error) {
    if field1 == "" {
        return nil, fmt.Errorf("field1 cannot be empty")
    }
    return &ExportedType{
        field1: field1,
        field2: field2,
    }, nil
}

// ExportedMethod performs an operation.
// Document parameters, return values, and behavior.
func (e *ExportedType) ExportedMethod(ctx context.Context) error {
    // Implementation
    return nil
}
```

### Naming Conventions

- **Packages**: Short, lowercase, no underscores (e.g., `config`, `redis`, `mqtt`)
- **Interfaces**: Descriptive names, often ending in `-er` (e.g., `Publisher`, `Reader`)
- **Variables**: Use camelCase for unexported, PascalCase for exported
- **Constants**: Use PascalCase (e.g., `DefaultTimeout`)
- **Functions**: Use descriptive names, PascalCase for exported

### Error Handling

```go
// Good: Check all errors
result, err := operation()
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}

// Good: Wrap errors with context
if err := validate(input); err != nil {
    return fmt.Errorf("validation failed for input %s: %w", input, err)
}

// Bad: Ignoring errors
result, _ := operation() // Never ignore errors
```

### Context Usage

```go
// Always respect context cancellation
func ProcessData(ctx context.Context, data []byte) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }
    
    // Processing logic
    return nil
}
```

## Testing Requirements

### Test Coverage

- **Minimum**: 80% code coverage for new code
- **Target**: 90%+ coverage for critical paths
- **Focus**: Test behavior, not implementation details

### Test Structure

```go
package mypackage

import (
    "context"
    "testing"
)

func TestFunctionName(t *testing.T) {
    // Arrange: Set up test data and dependencies
    ctx := context.Background()
    input := "test-input"
    
    // Act: Execute the function under test
    result, err := FunctionName(ctx, input)
    
    // Assert: Verify the results
    if err != nil {
        t.Fatalf("FunctionName() error = %v, want nil", err)
    }
    
    expected := "expected-result"
    if result != expected {
        t.Errorf("FunctionName() = %v, want %v", result, expected)
    }
}

func TestFunctionName_ErrorCase(t *testing.T) {
    ctx := context.Background()
    input := "" // Invalid input
    
    _, err := FunctionName(ctx, input)
    if err == nil {
        t.Fatal("FunctionName() error = nil, want error")
    }
}
```

### Table-Driven Tests

```go
func TestValidation(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        wantErr bool
    }{
        {
            name:    "valid input",
            input:   "valid",
            wantErr: false,
        },
        {
            name:    "empty input",
            input:   "",
            wantErr: true,
        },
        {
            name:    "too long input",
            input:   string(make([]byte, 1000)),
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := Validate(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Integration Tests

```go
// +build integration

func TestIntegration_RedisOperations(t *testing.T) {
    // Requires running Redis instance
    redisAddr := os.Getenv("REDIS_TEST_ADDR")
    if redisAddr == "" {
        t.Skip("REDIS_TEST_ADDR not set, skipping integration test")
    }
    
    // Test implementation
}
```

### Benchmarks

```go
func BenchmarkFunctionName(b *testing.B) {
    ctx := context.Background()
    input := "benchmark-input"
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = FunctionName(ctx, input)
    }
}
```

## Commit Guidelines

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, no logic change)
- **refactor**: Code refactoring (no feature change or bug fix)
- **perf**: Performance improvements
- **test**: Adding or updating tests
- **chore**: Maintenance tasks

### Examples

```bash
# Feature
feat(mqtt): add TLS certificate rotation support

Add automatic TLS certificate rotation when certificates are updated
on disk. Monitors certificate files for changes and reconnects with
new certificates.

Closes #123

# Bug fix
fix(redis): prevent memory leak in claim loop

Fixed memory leak caused by not releasing claimed message references.
Added proper cleanup in defer statement.

Fixes #456

# Documentation
docs(readme): update deployment examples

Add Docker Compose and Kubernetes deployment examples with
complete configuration.
```

### Commit Best Practices

- **Atomic commits**: One logical change per commit
- **Clear messages**: Describe what and why, not how
- **Reference issues**: Link to issue numbers when applicable
- **Sign commits**: Use GPG signing when possible

## Pull Request Process

### Before Submitting

1. **Update from main**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run all checks**:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   go test -cover ./...
   ```

3. **Update documentation**: Ensure README.md and ARCHITECTURE.md reflect your changes

4. **Add tests**: Include tests for new functionality

### Pull Request Template

```markdown
## Description

Brief description of the changes in this PR.

## Type of Change

- [ ] Bug fix (non-breaking change fixing an issue)
- [ ] New feature (non-breaking change adding functionality)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update

## Testing

Describe the testing you've done:

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed
- [ ] Test coverage maintained/improved

## Checklist

- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex code
- [ ] Documentation updated
- [ ] No new warnings generated
- [ ] Tests pass locally
- [ ] Dependent changes merged

## Related Issues

Closes #(issue number)
```

### Review Process

1. **Automated checks**: CI must pass
2. **Code review**: At least one maintainer approval required
3. **Documentation**: Must be updated if needed
4. **Tests**: Coverage must not decrease

## Documentation

### Code Documentation

```go
// Package redis provides Redis Streams client operations.
//
// The client supports both single-stream and multi-stream modes,
// with automatic consumer group management and recovery mechanisms.
package redis

// Client manages Redis stream operations.
//
// The client handles:
//   - Stream consumption using consumer groups
//   - Automatic claiming of idle messages
//   - Dead consumer cleanup
//   - Stream discovery in multi-stream mode
//
// Example usage:
//
//   cfg := &config.RedisConfig{
//       Address: "localhost:6379",
//       Stream:  "my-stream",
//   }
//   client, err := redis.NewClient(cfg, logger)
//   if err != nil {
//       log.Fatal(err)
//   }
//   defer client.Close()
type Client struct {
    // fields...
}
```

### Updating Documentation Files

When making changes that affect:

- **README.md**: Update usage examples, configuration, or features
- **ARCHITECTURE.md**: Update system design, flows, or component descriptions
- **CONTRIBUTING.md**: Update guidelines or processes (this file)

## Questions?

If you have questions about contributing:

1. Check existing documentation (README.md, ARCHITECTURE.md)
2. Search existing issues and discussions
3. Open a new issue with the question label
4. Contact maintainers

---

**Thank you for contributing to Syslog Consumer!**
