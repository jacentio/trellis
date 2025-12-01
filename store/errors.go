package store

import "errors"

var (
	// ErrParentNotFound is returned when the parent entity doesn't exist or is deleted.
	ErrParentNotFound = errors.New("trellis: parent entity not found")

	// ErrNotFound is returned when an entity doesn't exist or is deleted (has TTL <= now).
	ErrNotFound = errors.New("trellis: entity not found")

	// ErrAlreadyExists is returned when attempting to create an entity with an existing ID.
	ErrAlreadyExists = errors.New("trellis: entity already exists")

	// ErrHasChildren is returned when attempting to delete an entity with active children.
	ErrHasChildren = errors.New("trellis: entity has active children")

	// ErrConcurrentModification is returned when optimistic lock fails (version mismatch).
	ErrConcurrentModification = errors.New("trellis: entity was modified concurrently")

	// ErrDuplicateValue is returned when a unique constraint is violated.
	ErrDuplicateValue = errors.New("trellis: duplicate value for unique field")

	// ErrAlreadyDeleted is returned when attempting to delete an already-deleted entity.
	ErrAlreadyDeleted = errors.New("trellis: entity is already deleted")
)
