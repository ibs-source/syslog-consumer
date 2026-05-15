# Contributing Guide

Thank you for your interest in contributing to syslog-consumer. This guide provides the necessary information and standards for effective collaboration.

## Code of Conduct

This project maintains an open and welcoming environment. All contributors must:

- Use inclusive and professional language
- Respect differing viewpoints and experiences
- Accept constructive criticism gracefully
- Focus on community benefit
- Demonstrate empathy toward other members

Report concerns to security@ibs.srl.

## Prerequisites

### Required Tools

- **Go**: 1.25.10 or later (pinned via the `go` directive in `go.mod`; `GOTOOLCHAIN=auto` fetches it transparently)
- **Redis**: 6.0 or higher (for integration tests; miniredis is used for unit tests)
- **MQTT broker**: optional for end-to-end testing (Mosquitto or any MQTT 3.1.1/5.0 broker)
- **Docker and Docker Compose**: Optional but recommended
- **golangci-lint**: Install via `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest` (v2.x)
- **make**: For development automation

### Setup

```bash
# Fork and clone
git clone https://github.com/your-username/syslog-consumer.git
cd syslog-consumer

# Add upstream remote
git remote add upstream https://github.com/ibs-source/syslog-consumer.git

# Install dependencies
go mod tidy

# Verify setup
make test
```

## Development Workflow

### Branch Naming

Use descriptive branch names following these conventions:

- `feature/description` - New features or enhancements
- `fix/description` - Bug fixes
- `docs/description` - Documentation changes
- `perf/description` - Performance improvements
- `refactor/description` - Code refactoring

### Development Process

```bash
# Create feature branch
git checkout main
git pull upstream main
git checkout -b feature/your-feature

# Make changes and test
make vet && make lint && make test-race

# Commit with clear messages
git commit -m "Add feature: description"

# Push to fork
git push origin feature/your-feature

# Create Pull Request
```

## Code Standards

### Go Style

Follow official Go conventions:

- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://go.dev/doc/effective_go)

Apply formatting automatically:

```bash
gofmt -w .
goimports -w .
make lint
```

### Code Organization

- **Package Cohesion**: Single responsibility per package
- **Modularity**: Loose coupling via interfaces
- **Dependencies**: Avoid circular dependencies, use Go Modules
- **Structure**: Follow existing layout (`cmd/` for binaries, `internal/` for non-exported packages, `testdata/` for fixtures)

```
cmd/consumer/main.go        — Application entry point, signal handling, lifecycle
internal/config/            — Environment + flag configuration with validation
internal/hotpath/           — Pipeline orchestrator (fetch, publish, claim, cleanup, refresh)
internal/redis/             — Redis Streams client with multi-stream support
internal/mqtt/              — MQTT client, connection pool, ACK parsing
internal/compress/          — Zstd compression utilities + decoder freelist
internal/message/           — Message types (RedisMessage, AckMessage)
internal/health/            — HTTP health check server
internal/metrics/           — expvar counters exposed on /debug/vars
internal/log/               — Structured slog logger
```

### Design Principles

1. **Stateless design.** All state in Redis — no local caching.
2. **Self-contained messages.** Each message carries all metadata for processing.
3. **Fail-fast configuration.** Validate all settings at startup.
4. **Horizontal scalability.** Add instances without coordination.
5. **Lock-free hot path.** Go channels and atomic operations — no mutexes.

### Documentation

**Godoc Comments**
- Document all exported types, functions, methods, and constants
- Explain purpose, behavior, and usage
- Include runnable examples for public APIs

**Project Documentation**
- Update `README.md` for user-facing changes
- Update `ARCHITECTURE.md` for architectural modifications
- Add inline comments only for complex or non-obvious logic

### Error Handling

```go
// Always handle errors
result, err := function()
if err != nil {
    return fmt.Errorf("context: %w", err)
}

// Log with context
log.With("error", err).Error("operation failed")

// Avoid panic except in initialization
if critical {
    panic("unrecoverable error")
}
```

### Performance

The publish hot path (`buildPayload`, `severityName`, and adjacent helpers) must produce
**0 allocations per operation**. This is enforced by benchmarks with `-benchmem`.

Before submitting a change that touches the hot path, run:

```bash
go test -bench=. -benchmem ./internal/hotpath/
```

Every hot-path benchmark line must show `0 B/op` and `0 allocs/op`.

General performance guidance:

- **Memory**: Minimize allocations in hot paths
- **Concurrency**: Design for parallelism, avoid contention; prefer channels + atomics over mutexes
- **Batching**: Leverage `XREADGROUP COUNT` and pipelined MQTT publishes
- **Profiling**: Profile performance-critical changes (`go tool pprof`)
- **Benchmarking**: Add benchmarks for critical code paths

```bash
# Run benchmarks
go test -bench=. -benchmem ./...

# Specific package
go test -bench=. -benchmem ./internal/hotpath
```

## Linting and Static Analysis

### Configuration

The repository uses golangci-lint configured in `.golangci.yml`:

- **Tests**: Linting enabled for test files (`tests: true`)
- **Security**: gosec enabled at low severity/confidence; G115 cases in hot paths are guarded by saturating helpers rather than via global exclusions
- **Complexity**: cyclop, gocognit, gocyclo, funlen, lll enforced
- **Style**: revive, perfsprint, modernize — see `linters:` in `.golangci.yml` for the full set

### Running Linters

```bash
# Local execution
make lint

# Or directly
golangci-lint run
```

### NOLINT Usage

Use sparingly and only when necessary:

```go
//nolint:linter-name // Explanation: why this is safe
```

Requirements:
- Specific linter tag required
- Concise explanation required
- Prefer refactoring over disabling

### Test Guidelines

Extract helpers to satisfy complexity limits:

```go
// Instead of large test functions
func TestLargeFunction(t *testing.T) {
    helper1(t)
    helper2(t)
    helper3(t)
}

func helper1(t *testing.T) {
    // Focused test logic
}
```

## Testing

### Test Requirements

- **Unit Tests**: Test all new functions and methods
- **Integration Tests**: Test component interactions where applicable
- **Coverage**: Maintain or improve existing coverage
- **Test Cases**: Include positive and negative scenarios
- **Table-Driven Tests**: Use for multiple input/output combinations
- **Race Detection**: Run with `-race` flag

### Running Tests

```bash
# Unit tests (60s timeout)
make test

# With race detector (120s timeout)
make test-race

# Coverage report
make test-cover

# Static analysis
make vet
```

### Benchmarking

For performance-critical changes:

```bash
# Hot-path benchmarks (must remain 0 allocs/op)
go test -bench=. -benchmem ./internal/hotpath

# All benchmarks
go test -bench=. -benchmem ./...
```

Include benchmark results in pull requests for performance-related changes.

## Profile-Guided Optimization (PGO)

After significant hot-path changes, regenerate the PGO profile:

```bash
make pgo
make build-pgo
```

`make pgo` collects CPU profiles from hot-path, MQTT, and compression benchmarks and writes them to `default.pgo`. `make build-pgo` then rebuilds the binary using that profile.

## Pull Request Process

### Pre-submission Checklist

- [ ] All tests pass (`make test`)
- [ ] Race detector clean (`make test-race`)
- [ ] Code passes linting (`make lint` and `make vet`)
- [ ] Hot-path benchmarks still report 0 allocs/op (if hot path was touched)
- [ ] PGO profile regenerated (if hot path was touched)
- [ ] Documentation updated
- [ ] Rebased on latest main
- [ ] Clean commit history

```bash
# Rebase before submission
git fetch upstream
git rebase upstream/main
```

### Pull Request Template

**Title**: Clear, descriptive summary (e.g., `feat: Add MQTT QoS 2 support`)

**Description**:

```markdown
## Changes
- Technical implementation details
- Modified components
- New features or fixes

## Motivation
- Problem being solved
- Context and background
- Relevant issue links

## Impact
- Breaking changes (if any)
- Performance implications
- Behavioral changes

## Testing
- Test approach
- Coverage additions
- Benchmark results (if applicable)
```

### Review Process

1. **Initial Review**: Maintainer reviews for adherence to standards
2. **Feedback**: Address comments and push updates
3. **Approval**: Maintainer approval required for merge
4. **Merge**: Merged to main branch
5. **Cleanup**: Delete feature branch after merge

## Issue Reporting

### Bug Reports

Provide comprehensive information:

**Environment**
- Go version: `go version`
- Operating system and version
- Redis version
- MQTT broker version
- Deployment method (Docker, bare metal, etc.)

**Reproduction**
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages or logs
- Configuration settings (sanitized)

**Additional Context**
- Screenshots or recordings
- Related issues
- Possible fixes

### Feature Requests

**Description**
- Use case and motivation
- Proposed solution
- Alternative approaches considered
- Impact assessment

**Implementation**
- Technical approach
- Affected components
- Migration requirements

### Security Issues

**DO NOT** create public issues for security vulnerabilities.

Report privately to: security@ibs.srl

Response time: Within 48 hours

## Communication

### Getting Help

1. Review existing issues and pull requests
2. Check `README.md` and `ARCHITECTURE.md`
3. Create a discussion issue for questions
4. Ask specific, focused questions

### Best Practices

- Search existing issues before creating new ones
- Provide context and examples
- Be respectful and professional
- Stay on topic
- Follow up on your issues and PRs

## Development Commands

```bash
# Build
make build

# Build with PGO
make build-pgo

# Format code
gofmt -w .
goimports -w .

# Run linter
make lint
make vet

# Run tests
make test
make test-race
make test-cover

# Regenerate PGO profile
make pgo

# Build Docker image
make docker-build

# Clean artifacts
make clean
```

## Git Commit Messages

Follow these guidelines for commit messages:

- Use imperative mood ("Add feature" not "Added feature")
- First line: 50 characters or less
- Separate subject from body with blank line
- Body: Explain what and why, not how
- Reference issues and PRs

Example:

```
Add MQTT connection pooling

Implement connection pooling to improve throughput under high load.
The pool maintains minimum idle connections and grows dynamically
based on demand.

Fixes #123
```

## Contribution License

By contributing, you agree that your contributions will be licensed under the project's MIT License.

## Additional Resources

- [Project README](README.md)
- [Architecture Documentation](ARCHITECTURE.md)
- [Go Documentation](https://go.dev/doc/)
- [golangci-lint](https://golangci-lint.run/)
- [Redis Documentation](https://redis.io/docs/)
- [Eclipse Paho MQTT Go Client](https://github.com/eclipse/paho.mqtt.golang)

## Questions

For questions or clarifications:

1. Review existing documentation
2. Search closed issues
3. Create a discussion issue
4. Contact maintainers via GitHub

Thank you for contributing to this project.
