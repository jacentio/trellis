// Package store provides a DynamoDB data access layer with hierarchical entity support.
//
// Trellis is designed for applications that need to model hierarchical relationships
// in DynamoDB while maintaining referential integrity, unique constraints, and
// supporting cascading deletes.
//
// # Key Features
//
//   - Parent validation on child creation (atomic)
//   - Orphan protection (prevent deleting parents with children)
//   - Cascading deletes via DynamoDB Streams + TTL
//   - Unique field constraints within parent scope
//   - Optimistic locking with version field
//   - Configurable write sharding for high throughput
//
// # Entity Interfaces
//
// All entities must implement the [Entity] interface:
//
//	type Entity interface {
//	    TableName() string
//	    GetKey() PK
//	    EntityRef() string
//	    EntityType() string
//	}
//
// Child entities should also implement [ParentChecker]:
//
//	type ParentChecker interface {
//	    ParentCheck() *ConditionCheck
//	    ParentRef() string
//	}
//
// Entities with unique constraints implement [UniqueFielder]:
//
//	type UniqueFielder interface {
//	    UniqueFields() map[string]string
//	}
//
// # Configuration
//
// Use [DefaultConfig] for small datasets (NumShards=1, single queries).
// Increase NumShards for higher throughput:
//
//	cfg := store.DefaultConfig()
//	cfg.NumShards = 16 // 16,000 writes/sec per parent
//
// # Errors
//
// The package defines domain-specific errors:
//
//   - [ErrNotFound] - entity doesn't exist or is deleted
//   - [ErrParentNotFound] - parent validation failed
//   - [ErrAlreadyExists] - entity with ID already exists
//   - [ErrHasChildren] - cannot delete entity with children
//   - [ErrConcurrentModification] - optimistic lock failed
//   - [ErrDuplicateValue] - unique constraint violated
package store
