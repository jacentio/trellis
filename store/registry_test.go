package store_test

import (
	"testing"

	"github.com/jacentio/trellis/store"
)

func TestNewRegistry(t *testing.T) {
	r := store.NewRegistry()
	if r == nil {
		t.Fatal("expected non-nil Registry")
	}
}

func TestRegistry_Register(t *testing.T) {
	r := store.NewRegistry()

	rel := store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	}

	r.Register(rel)

	rels := r.AllRelationships()
	if len(rels) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].ParentType != "organization" {
		t.Errorf("expected ParentType 'organization', got %q", rels[0].ParentType)
	}
}

func TestRegistry_ChildrenOf(t *testing.T) {
	r := store.NewRegistry()

	// Register organization -> studio relationship
	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	})

	// Register studio -> title relationship
	r.Register(store.Relationship{
		ParentType:     "studio",
		ChildType:      "title",
		ChildTableName: "titles",
		ParentKeyAttr:  "studio_id",
	})

	// Organization has one child type (studio)
	orgChildren := r.ChildrenOf("organization")
	if len(orgChildren) != 1 {
		t.Errorf("expected 1 child for organization, got %d", len(orgChildren))
	}
	if orgChildren[0].ChildType != "studio" {
		t.Errorf("expected child type 'studio', got %q", orgChildren[0].ChildType)
	}

	// Studio has one child type (title)
	studioChildren := r.ChildrenOf("studio")
	if len(studioChildren) != 1 {
		t.Errorf("expected 1 child for studio, got %d", len(studioChildren))
	}
	if studioChildren[0].ChildType != "title" {
		t.Errorf("expected child type 'title', got %q", studioChildren[0].ChildType)
	}

	// Title has no children
	titleChildren := r.ChildrenOf("title")
	if len(titleChildren) != 0 {
		t.Errorf("expected 0 children for title, got %d", len(titleChildren))
	}
}

func TestRegistry_HasChildren(t *testing.T) {
	r := store.NewRegistry()

	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	})

	if !r.HasChildren("organization") {
		t.Error("expected organization to have children")
	}

	if r.HasChildren("studio") {
		t.Error("expected studio to not have children")
	}
}

func TestRegistry_MultipleChildTypes(t *testing.T) {
	r := store.NewRegistry()

	// Organization has multiple child types
	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	})
	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "user",
		ChildTableName: "users",
		ParentKeyAttr:  "organization_id",
	})

	children := r.ChildrenOf("organization")
	if len(children) != 2 {
		t.Errorf("expected 2 children for organization, got %d", len(children))
	}
}

// --- Registry Edge Cases ---

func TestRegistry_Empty(t *testing.T) {
	r := store.NewRegistry()

	// Empty registry should have no relationships
	rels := r.AllRelationships()
	if len(rels) != 0 {
		t.Errorf("expected 0 relationships, got %d", len(rels))
	}
}

func TestRegistry_ChildrenOf_Nonexistent(t *testing.T) {
	r := store.NewRegistry()

	// Query children for non-existent parent type
	// Note: nil slice is acceptable - len(nil) == 0 and range works on nil slices
	children := r.ChildrenOf("nonexistent")
	if len(children) != 0 {
		t.Errorf("expected 0 children for nonexistent parent, got %d", len(children))
	}
}

func TestRegistry_HasChildren_Nonexistent(t *testing.T) {
	r := store.NewRegistry()

	// Check children for non-existent parent type
	if r.HasChildren("nonexistent") {
		t.Error("expected false for nonexistent parent")
	}
}

func TestRegistry_HasChildren_EmptyString(t *testing.T) {
	r := store.NewRegistry()

	// Check children for empty string parent type
	if r.HasChildren("") {
		t.Error("expected false for empty string parent")
	}
}

func TestRegistry_ChildrenOf_EmptyString(t *testing.T) {
	r := store.NewRegistry()

	// Query children for empty string parent type
	children := r.ChildrenOf("")
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty string parent, got %d", len(children))
	}
}

func TestRegistry_Register_EmptyRelationship(t *testing.T) {
	r := store.NewRegistry()

	// Register empty relationship (edge case)
	r.Register(store.Relationship{})

	rels := r.AllRelationships()
	if len(rels) != 1 {
		t.Errorf("expected 1 relationship, got %d", len(rels))
	}

	// Empty parent type should be queryable
	children := r.ChildrenOf("")
	if len(children) != 1 {
		t.Errorf("expected 1 child for empty parent, got %d", len(children))
	}
}

func TestRegistry_Register_DuplicateRelationship(t *testing.T) {
	r := store.NewRegistry()

	rel := store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	}

	// Register same relationship twice
	r.Register(rel)
	r.Register(rel)

	// Both should be stored (no deduplication)
	rels := r.AllRelationships()
	if len(rels) != 2 {
		t.Errorf("expected 2 relationships (duplicates allowed), got %d", len(rels))
	}

	children := r.ChildrenOf("organization")
	if len(children) != 2 {
		t.Errorf("expected 2 children for organization (duplicates), got %d", len(children))
	}
}

func TestRegistry_DeepHierarchy(t *testing.T) {
	r := store.NewRegistry()

	// Create deep hierarchy: org -> studio -> title -> environment
	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	})
	r.Register(store.Relationship{
		ParentType:     "studio",
		ChildType:      "title",
		ChildTableName: "titles",
		ParentKeyAttr:  "studio_id",
	})
	r.Register(store.Relationship{
		ParentType:     "title",
		ChildType:      "environment",
		ChildTableName: "environments",
		ParentKeyAttr:  "title_id",
	})

	// Verify each level
	if len(r.ChildrenOf("organization")) != 1 {
		t.Error("expected 1 child for organization")
	}
	if len(r.ChildrenOf("studio")) != 1 {
		t.Error("expected 1 child for studio")
	}
	if len(r.ChildrenOf("title")) != 1 {
		t.Error("expected 1 child for title")
	}
	if len(r.ChildrenOf("environment")) != 0 {
		t.Error("expected 0 children for environment (leaf)")
	}

	// Total relationships
	if len(r.AllRelationships()) != 3 {
		t.Errorf("expected 3 relationships, got %d", len(r.AllRelationships()))
	}
}

func TestRegistry_AllRelationships_Order(t *testing.T) {
	r := store.NewRegistry()

	// Register in specific order
	r.Register(store.Relationship{ParentType: "a", ChildType: "b"})
	r.Register(store.Relationship{ParentType: "c", ChildType: "d"})
	r.Register(store.Relationship{ParentType: "e", ChildType: "f"})

	rels := r.AllRelationships()
	if len(rels) != 3 {
		t.Fatalf("expected 3 relationships, got %d", len(rels))
	}

	// Should maintain insertion order
	if rels[0].ParentType != "a" {
		t.Errorf("expected first relationship parent 'a', got %q", rels[0].ParentType)
	}
	if rels[1].ParentType != "c" {
		t.Errorf("expected second relationship parent 'c', got %q", rels[1].ParentType)
	}
	if rels[2].ParentType != "e" {
		t.Errorf("expected third relationship parent 'e', got %q", rels[2].ParentType)
	}
}

func TestRegistry_ChildrenOf_ReturnsCorrectChild(t *testing.T) {
	r := store.NewRegistry()

	r.Register(store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	})

	children := r.ChildrenOf("organization")
	if len(children) != 1 {
		t.Fatal("expected 1 child")
	}

	child := children[0]
	if child.ChildType != "studio" {
		t.Errorf("expected ChildType 'studio', got %q", child.ChildType)
	}
	if child.ChildTableName != "studios" {
		t.Errorf("expected ChildTableName 'studios', got %q", child.ChildTableName)
	}
	if child.ParentKeyAttr != "organization_id" {
		t.Errorf("expected ParentKeyAttr 'organization_id', got %q", child.ParentKeyAttr)
	}
}

func TestRegistry_HasChildren_AfterRegister(t *testing.T) {
	r := store.NewRegistry()

	// Initially no children
	if r.HasChildren("organization") {
		t.Error("expected no children before register")
	}

	// Register
	r.Register(store.Relationship{
		ParentType: "organization",
		ChildType:  "studio",
	})

	// Now has children
	if !r.HasChildren("organization") {
		t.Error("expected children after register")
	}
}

func TestRegistry_SpecialCharactersInTypes(t *testing.T) {
	r := store.NewRegistry()

	// Types with special characters
	r.Register(store.Relationship{
		ParentType:     "parent#with#hash",
		ChildType:      "child:with:colon",
		ChildTableName: "table.with.dots",
		ParentKeyAttr:  "attr-with-dash",
	})

	children := r.ChildrenOf("parent#with#hash")
	if len(children) != 1 {
		t.Error("expected 1 child for parent with special chars")
	}
	if children[0].ChildType != "child:with:colon" {
		t.Errorf("expected child type with colon, got %q", children[0].ChildType)
	}
}

func TestRegistry_UnicodeTypes(t *testing.T) {
	r := store.NewRegistry()

	// Types with unicode
	r.Register(store.Relationship{
		ParentType: "親タイプ",
		ChildType:  "子タイプ",
	})

	if !r.HasChildren("親タイプ") {
		t.Error("expected children for unicode parent type")
	}

	children := r.ChildrenOf("親タイプ")
	if len(children) != 1 || children[0].ChildType != "子タイプ" {
		t.Error("expected unicode child type")
	}
}
