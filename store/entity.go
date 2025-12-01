// Package store provides a DynamoDB data access layer with hierarchical entity support.
package store

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// PK represents a DynamoDB primary key.
type PK map[string]types.AttributeValue

// Entity is the base interface for all storable types.
type Entity interface {
	// TableName returns the DynamoDB table name for this entity type.
	TableName() string

	// GetKey returns the primary key for this entity.
	GetKey() PK

	// EntityRef returns the type-qualified reference (e.g., "studio#uuid").
	EntityRef() string

	// EntityType returns the entity type name (e.g., "studio").
	EntityType() string
}

// ParentChecker is implemented by entities that have a parent.
type ParentChecker interface {
	// ParentCheck returns the condition check for parent validation.
	// Returns nil for root entities or when parent validation should be skipped.
	ParentCheck() *ConditionCheck

	// ParentRef returns the parent's entity reference (e.g., "organization#uuid").
	// Returns empty string for root entities.
	ParentRef() string
}

// ConditionCheck defines a parent existence check for transactions.
type ConditionCheck struct {
	TableName string
	Key       PK

	// ConditionExpr is an optional custom condition expression.
	// If empty, ParentExistsCondition() is used (checks existence and not deleted).
	ConditionExpr string
}

// UniqueFielder is implemented by entities with unique field constraints.
type UniqueFielder interface {
	// UniqueFields returns field name to value mappings for fields
	// that must be unique within the parent scope.
	UniqueFields() map[string]string
}

// Item represents a retrieved DynamoDB item with common fields.
type Item struct {
	// Raw is the raw DynamoDB item.
	Raw map[string]types.AttributeValue

	// Version is the optimistic lock version.
	Version int64

	// CreatedAt is the ISO 8601 creation timestamp.
	CreatedAt string

	// UpdatedAt is the ISO 8601 last update timestamp.
	UpdatedAt string

	// EntityRef is the type-qualified entity reference.
	EntityRef string

	// ParentRef is the parent's entity reference (empty for root entities).
	ParentRef string
}

// ChildRef represents a reference to a child entity in the relationship table.
type ChildRef struct {
	// Ref is the child's entity reference.
	Ref string

	// TableName is the DynamoDB table containing the child.
	TableName string

	// Key is the primary key to locate the child.
	Key PK

	// ShardPK is the relationship table partition key (for TTL updates).
	ShardPK string
}

// QueryInput defines parameters for querying entities.
type QueryInput struct {
	// TableName is the DynamoDB table to query.
	TableName string

	// IndexName is the optional GSI/LSI to query.
	IndexName string

	// KeyConditionExpression is the DynamoDB key condition.
	KeyConditionExpression string

	// FilterExpression is an optional filter (TTL filter is automatically merged).
	FilterExpression string

	// ExpressionAttributeNames maps expression attribute name placeholders.
	ExpressionAttributeNames map[string]string

	// ExpressionAttributeValues maps expression attribute value placeholders.
	ExpressionAttributeValues map[string]types.AttributeValue

	// Limit is the maximum number of items to return (0 = no limit).
	Limit int32

	// ScanIndexForward determines sort order (true = ascending, false = descending).
	ScanIndexForward *bool
}
