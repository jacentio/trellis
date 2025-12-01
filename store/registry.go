package store

// Relationship defines a parent-child relationship for cascade operations.
type Relationship struct {
	// ParentType is the parent entity type (e.g., "organization").
	ParentType string

	// ChildType is the child entity type (e.g., "studio").
	ChildType string

	// ChildTableName is the DynamoDB table name for the child (e.g., "studios").
	ChildTableName string

	// ParentKeyAttr is the attribute name in child that references parent (e.g., "organization_id").
	ParentKeyAttr string
}

// Registry holds all known entity relationships for cascade operations.
type Registry struct {
	relationships []Relationship
	byParent      map[string][]Relationship
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		relationships: []Relationship{},
		byParent:      make(map[string][]Relationship),
	}
}

// Register adds a relationship to the registry.
// This should be called during init() for each parent-child relationship.
func (r *Registry) Register(rel Relationship) {
	r.relationships = append(r.relationships, rel)
	r.byParent[rel.ParentType] = append(r.byParent[rel.ParentType], rel)
}

// ChildrenOf returns all child relationships for a given parent type.
func (r *Registry) ChildrenOf(parentType string) []Relationship {
	return r.byParent[parentType]
}

// AllRelationships returns all registered relationships.
func (r *Registry) AllRelationships() []Relationship {
	return r.relationships
}

// HasChildren returns true if the parent type has any registered child relationships.
func (r *Registry) HasChildren(parentType string) bool {
	return len(r.byParent[parentType]) > 0
}
