# Trellis

[![Go Reference](https://pkg.go.dev/badge/github.com/jacentio/trellis.svg)](https://pkg.go.dev/github.com/jacentio/trellis)
[![Go Report Card](https://goreportcard.com/badge/github.com/jacentio/trellis)](https://goreportcard.com/report/github.com/jacentio/trellis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, Lambda-optimized DynamoDB data access layer for Go with hierarchical entity support.

## Features

- **Parent Validation** - Atomic parent existence check on child creation
- **Orphan Protection** - Prevent deleting parents with active children
- **Cascading Deletes** - Async deletion via DynamoDB Streams + TTL
- **Unique Constraints** - Field uniqueness within parent scope
- **Optimistic Locking** - Version-based concurrency control
- **Configurable Sharding** - Scale from 1,000 to 256,000 writes/sec per parent
- **TTL-based Soft Deletes** - DynamoDB handles cleanup automatically

## Requirements

- Go 1.23+
- AWS SDK for Go v2
- DynamoDB tables configured with TTL

## Installation

```bash
go get github.com/jacentio/trellis
```

## Quick Start

### Define Entities

```go
// Root entity (no parent)
type Organization struct {
    ID   string
    Name string
}

func (o Organization) TableName() string  { return "organizations" }
func (o Organization) EntityRef() string  { return "organization#" + o.ID }
func (o Organization) EntityType() string { return "organization" }
func (o Organization) GetKey() store.PK {
    return store.PK{"id": &types.AttributeValueMemberS{Value: o.ID}}
}

// Child entity with parent
type Studio struct {
    ID             string
    OrganizationID string
    Name           string
}

func (s Studio) TableName() string  { return "studios" }
func (s Studio) EntityRef() string  { return "studio#" + s.ID }
func (s Studio) EntityType() string { return "studio" }
func (s Studio) GetKey() store.PK {
    return store.PK{"id": &types.AttributeValueMemberS{Value: s.ID}}
}

// Implement ParentChecker for parent validation
func (s Studio) ParentCheck() *store.ConditionCheck {
    return &store.ConditionCheck{
        TableName: "organizations",
        Key:       store.PK{"id": &types.AttributeValueMemberS{Value: s.OrganizationID}},
    }
}

func (s Studio) ParentRef() string {
    return "organization#" + s.OrganizationID
}
```

### Create Store

```go
cfg, _ := config.LoadDefaultConfig(context.Background())
client := dynamodb.NewFromConfig(cfg)

s := store.New(client, store.DefaultConfig())
```

### CRUD Operations

```go
// Create (with automatic parent validation)
org := Organization{ID: "org-1", Name: "Acme Corp"}
item := map[string]types.AttributeValue{
    "id":   &types.AttributeValueMemberS{Value: org.ID},
    "name": &types.AttributeValueMemberS{Value: org.Name},
}
err := s.Create(ctx, org, item)

// Get (automatically filters deleted entities)
result, err := s.Get(ctx, "organizations", org.GetKey())
if errors.Is(err, store.ErrNotFound) {
    // Entity doesn't exist or is deleted
}

// Update (with optimistic locking)
updateItem := map[string]types.AttributeValue{
    "name": &types.AttributeValueMemberS{Value: "New Name"},
}
err = s.Update(ctx, org, updateItem, result.Version)
if errors.Is(err, store.ErrConcurrentModification) {
    // Version mismatch - retry with fresh data
}

// Delete (with orphan protection)
err = s.Delete(ctx, org, store.DeleteOptions{OrphanProtect: true})
if errors.Is(err, store.ErrHasChildren) {
    // Cannot delete - has active children
}

// Delete (cascade to children)
err = s.Delete(ctx, org, store.DeleteOptions{Cascade: true})
```

### Query (with automatic TTL filtering)

```go
items, err := s.Query(ctx, store.QueryInput{
    TableName:              "studios",
    KeyConditionExpression: "organization_id = :org_id",
    ExpressionAttributeValues: map[string]types.AttributeValue{
        ":org_id": &types.AttributeValueMemberS{Value: "org-1"},
    },
})
```

### Unique Constraints

```go
type Title struct {
    ID       string
    StudioID string
    Name     string
    Slug     string
}

// Implement UniqueFielder for uniqueness constraints
func (t Title) UniqueFields() map[string]string {
    return map[string]string{
        "name": t.Name,  // Unique within studio
        "slug": t.Slug,  // Unique within studio
    }
}

// Create fails if name or slug already exists under same studio
err := s.Create(ctx, title, item)
if errors.Is(err, store.ErrDuplicateValue) {
    // Unique constraint violated
}
```

### Relationship Registry

Register parent-child relationships for cascade operations:

```go
registry := store.NewRegistry()

registry.Register(store.Relationship{
    ParentType:     "organization",
    ChildType:      "studio",
    ChildTableName: "studios",
    ParentKeyAttr:  "organization_id",
})

registry.Register(store.Relationship{
    ParentType:     "studio",
    ChildType:      "title",
    ChildTableName: "titles",
    ParentKeyAttr:  "studio_id",
})

children := registry.ChildrenOf("organization") // Returns studio relationship
```

## Errors

| Error | Description |
|-------|-------------|
| `ErrNotFound` | Entity doesn't exist or is deleted |
| `ErrParentNotFound` | Parent entity doesn't exist or is deleted |
| `ErrAlreadyExists` | Entity with same ID already exists |
| `ErrHasChildren` | Cannot delete entity with active children |
| `ErrConcurrentModification` | Optimistic lock failed (version mismatch) |
| `ErrDuplicateValue` | Unique constraint violated |

All errors can be checked with `errors.Is()`:

```go
if errors.Is(err, store.ErrNotFound) {
    // Handle not found
}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `RelationshipTable` | `trellis_relationships` | Table for parent-child relationships |
| `UniqueTable` | `trellis_unique_constraints` | Table for unique constraints |
| `NumShards` | `1` | Relationship table shards (1-256) |

### Scaling Guide

| NumShards | Writes/sec | Reads/sec | Use Case |
|-----------|------------|-----------|----------|
| 1 | 1,000 | 3,000 | Small datasets, < 1K children/parent |
| 16 | 16,000 | 48,000 | Large datasets |
| 256 | 256,000 | 768,000 | Extreme scale |

**When to increase NumShards:**
- Writes approaching 1,000/sec per parent
- Reads approaching 3,000/sec per parent
- More than ~10K children per parent

## DynamoDB Tables Required

### Entity Tables (one per entity type)

- PK: `id` (String)
- TTL attribute: `ttl`
- Stream: `NEW_AND_OLD_IMAGES` (for cascade deletes)

### Relationship Table

- PK: `pk` (String) - `{parent_ref}#{shard}`
- SK: `child_ref` (String)
- TTL attribute: `ttl`

### Unique Constraints Table

- PK: `pk` (String) - SHA256 hash
- SK: `sk` (String) - `CONSTRAINT`
- TTL attribute: `ttl`

## Cascade Deletes

Trellis uses TTL-based soft deletes with DynamoDB Streams for async cascade:

1. Delete sets `ttl = now()` on entity
2. Stream Lambda detects TTL change
3. Lambda propagates TTL to all children
4. DynamoDB automatically cleans up items (within 48 hours)

### Stream Handler

Use the `stream` package for your cascade delete Lambda:

```go
import (
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/jacentio/trellis/store"
    "github.com/jacentio/trellis/stream"
)

var handler *stream.Handler

func init() {
    cfg, _ := config.LoadDefaultConfig(context.Background())
    client := dynamodb.NewFromConfig(cfg)
    s := store.New(client, store.DefaultConfig())
    handler = stream.NewHandler(s, nil) // nil uses slog.Default()
}

func main() {
    lambda.Start(handler.HandleCascadeDelete)
}
```

## Testing

### Unit Tests

```bash
go test ./...
```

### E2E Integration Tests

E2E tests run against real DynamoDB tables. Configure your AWS credentials and run:

```bash
go test -tags=e2e -v ./e2e/...
```

> **Note:** E2E tests create temporary tables with unique names and clean up after completion.

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/jacentio/trellis.git
cd trellis

# Install git hooks (runs CI checks before each commit)
./scripts/setup-hooks.sh
```

### CI Script

Run all CI checks locally:

```bash
./scripts/ci.sh        # Full checks (with race detector)
./scripts/ci.sh --quick  # Quick checks (for pre-commit)
```

The CI script performs:
- Code formatting check (gofmt)
- Static analysis (go vet, staticcheck)
- Dependency verification (go mod tidy)
- Build verification
- Unit tests with race detector
- E2E test compilation check

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Clone and set up hooks: `./scripts/setup-hooks.sh`
3. Create your feature branch (`git checkout -b feature/amazing-feature`)
4. Make your changes
5. Run CI checks (`./scripts/ci.sh`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

All PRs must pass CI checks before merging.

## License

MIT License - see [LICENSE](LICENSE) for details.
