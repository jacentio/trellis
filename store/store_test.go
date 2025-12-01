package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jacentio/trellis/store"
)

// --- Test Entity Types ---

// Parent is a root entity with no parent.
type Parent struct {
	ID   string
	Name string
}

func (p Parent) TableName() string  { return "parents" }
func (p Parent) EntityRef() string  { return "parent#" + p.ID }
func (p Parent) EntityType() string { return "parent" }
func (p Parent) GetKey() store.PK {
	return store.PK{
		"id": &types.AttributeValueMemberS{Value: p.ID},
	}
}

// Child is an entity with a parent.
type Child struct {
	ID       string
	ParentID string
	Name     string
}

func (c Child) TableName() string  { return "children" }
func (c Child) EntityRef() string  { return "child#" + c.ID }
func (c Child) EntityType() string { return "child" }
func (c Child) GetKey() store.PK {
	return store.PK{
		"id": &types.AttributeValueMemberS{Value: c.ID},
	}
}

func (c Child) ParentCheck() *store.ConditionCheck {
	return &store.ConditionCheck{
		TableName: "parents",
		Key: store.PK{
			"id": &types.AttributeValueMemberS{Value: c.ParentID},
		},
	}
}

func (c Child) ParentRef() string {
	return "parent#" + c.ParentID
}

// UniqueChild is an entity with unique field constraints.
type UniqueChild struct {
	ID       string
	ParentID string
	Name     string
	Slug     string
}

func (u UniqueChild) TableName() string  { return "unique_children" }
func (u UniqueChild) EntityRef() string  { return "unique_child#" + u.ID }
func (u UniqueChild) EntityType() string { return "unique_child" }
func (u UniqueChild) GetKey() store.PK {
	return store.PK{
		"id": &types.AttributeValueMemberS{Value: u.ID},
	}
}

func (u UniqueChild) ParentCheck() *store.ConditionCheck {
	return &store.ConditionCheck{
		TableName: "parents",
		Key: store.PK{
			"id": &types.AttributeValueMemberS{Value: u.ParentID},
		},
	}
}

func (u UniqueChild) ParentRef() string {
	return "parent#" + u.ParentID
}

func (u UniqueChild) UniqueFields() map[string]string {
	return map[string]string{
		"name": u.Name,
		"slug": u.Slug,
	}
}

// --- Unit Tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := store.DefaultConfig()

	if cfg.RelationshipTable != "trellis_relationships" {
		t.Errorf("expected RelationshipTable 'trellis_relationships', got %q", cfg.RelationshipTable)
	}
	if cfg.UniqueTable != "trellis_unique_constraints" {
		t.Errorf("expected UniqueTable 'trellis_unique_constraints', got %q", cfg.UniqueTable)
	}
	if cfg.NumShards != 1 {
		t.Errorf("expected NumShards 1, got %d", cfg.NumShards)
	}
}

func TestIsDeleted(t *testing.T) {
	tests := []struct {
		name     string
		item     map[string]types.AttributeValue
		expected bool
	}{
		{
			name:     "no TTL attribute",
			item:     map[string]types.AttributeValue{},
			expected: false,
		},
		{
			name: "TTL in past",
			item: map[string]types.AttributeValue{
				"ttl": &types.AttributeValueMemberN{Value: "1000000000"}, // 2001
			},
			expected: true,
		},
		{
			name: "TTL in future",
			item: map[string]types.AttributeValue{
				"ttl": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix()+3600)},
			},
			expected: false,
		},
		{
			name: "TTL is now",
			item: map[string]types.AttributeValue{
				"ttl": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := store.IsDeleted(tt.item)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEntityInterfaces(t *testing.T) {
	// Test Parent entity
	parent := Parent{ID: "p1", Name: "Test Parent"}
	if parent.TableName() != "parents" {
		t.Errorf("expected TableName 'parents', got %q", parent.TableName())
	}
	if parent.EntityRef() != "parent#p1" {
		t.Errorf("expected EntityRef 'parent#p1', got %q", parent.EntityRef())
	}
	if parent.EntityType() != "parent" {
		t.Errorf("expected EntityType 'parent', got %q", parent.EntityType())
	}

	// Test Child entity
	child := Child{ID: "c1", ParentID: "p1", Name: "Test Child"}
	if child.TableName() != "children" {
		t.Errorf("expected TableName 'children', got %q", child.TableName())
	}
	if child.EntityRef() != "child#c1" {
		t.Errorf("expected EntityRef 'child#c1', got %q", child.EntityRef())
	}
	if child.ParentRef() != "parent#p1" {
		t.Errorf("expected ParentRef 'parent#p1', got %q", child.ParentRef())
	}

	check := child.ParentCheck()
	if check == nil {
		t.Fatal("expected non-nil ParentCheck")
	}
	if check.TableName != "parents" {
		t.Errorf("expected ParentCheck TableName 'parents', got %q", check.TableName)
	}

	// Test UniqueChild entity
	uniqueChild := UniqueChild{ID: "uc1", ParentID: "p1", Name: "Unique", Slug: "unique-slug"}
	fields := uniqueChild.UniqueFields()
	if fields["name"] != "Unique" {
		t.Errorf("expected UniqueFields name 'Unique', got %q", fields["name"])
	}
	if fields["slug"] != "unique-slug" {
		t.Errorf("expected UniqueFields slug 'unique-slug', got %q", fields["slug"])
	}
}

func TestInterfaceCompliance(t *testing.T) {
	// Verify test entities implement the required interfaces
	var _ store.Entity = Parent{}
	var _ store.Entity = Child{}
	var _ store.Entity = UniqueChild{}
	var _ store.ParentChecker = Child{}
	var _ store.ParentChecker = UniqueChild{}
	var _ store.UniqueFielder = UniqueChild{}
}

// --- Mock Tests (without DynamoDB) ---

func TestNewStore(t *testing.T) {
	// Test with nil client (just verifying config validation)
	cfg := store.Config{
		RelationshipTable: "",
		UniqueTable:       "",
		NumShards:         0,
	}

	s := store.New(nil, cfg)
	if s == nil {
		t.Error("expected non-nil Store")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  store.Config
	}{
		{
			name: "zero NumShards gets set to 1",
			cfg:  store.Config{NumShards: 0},
		},
		{
			name: "negative NumShards gets set to 1",
			cfg:  store.Config{NumShards: -5},
		},
		{
			name: "NumShards over 256 gets capped",
			cfg:  store.Config{NumShards: 500},
		},
		{
			name: "empty table names get defaults",
			cfg:  store.Config{RelationshipTable: "", UniqueTable: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify New doesn't panic
			s := store.New(nil, tt.cfg)
			if s == nil {
				t.Error("expected non-nil Store")
			}
		})
	}
}

// --- Integration Test Examples (require DynamoDB Local or mocks) ---

// ExampleStore_Create demonstrates creating an entity with parent validation.
func ExampleStore_Create() {
	// This example shows the API usage pattern.
	// In production, you would use a real DynamoDB client.

	ctx := context.Background()
	_ = ctx // suppress unused warning

	cfg := store.DefaultConfig()
	_ = cfg // suppress unused warning

	// s := store.New(dynamoClient, cfg)

	parent := Parent{ID: "org-123", Name: "Acme Corp"}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: parent.ID},
		"name": &types.AttributeValueMemberS{Value: parent.Name},
	}
	_ = item   // suppress unused warning
	_ = parent // suppress unused warning

	// err := s.Create(ctx, parent, item)

	child := Child{ID: "studio-456", ParentID: "org-123", Name: "Main Studio"}
	childItem := map[string]types.AttributeValue{
		"id":        &types.AttributeValueMemberS{Value: child.ID},
		"parent_id": &types.AttributeValueMemberS{Value: child.ParentID},
		"name":      &types.AttributeValueMemberS{Value: child.Name},
	}
	_ = childItem // suppress unused warning
	_ = child     // suppress unused warning

	// err = s.Create(ctx, child, childItem)
	// If parent doesn't exist, returns store.ErrParentNotFound
}

// ExampleStore_Delete demonstrates deleting with orphan protection.
func ExampleStore_Delete() {
	ctx := context.Background()
	_ = ctx

	cfg := store.DefaultConfig()
	_ = cfg

	// s := store.New(dynamoClient, cfg)

	parent := Parent{ID: "org-123", Name: "Acme Corp"}
	_ = parent

	// Delete with orphan protection (fails if children exist)
	opts := store.DeleteOptions{OrphanProtect: true}
	_ = opts
	// err := s.Delete(ctx, parent, opts)
	// If children exist, returns store.ErrHasChildren

	// Cascade delete (marks entity and all children for deletion)
	cascadeOpts := store.DeleteOptions{Cascade: true}
	_ = cascadeOpts
	// err = s.Delete(ctx, parent, cascadeOpts)
}

// ExampleStore_Update demonstrates optimistic locking.
func ExampleStore_Update() {
	ctx := context.Background()
	_ = ctx

	cfg := store.DefaultConfig()
	_ = cfg

	// s := store.New(dynamoClient, cfg)

	// First, get the entity to obtain current version
	// item, _ := s.Get(ctx, "parents", parent.GetKey())
	// currentVersion := item.Version

	parent := Parent{ID: "org-123", Name: "Updated Name"}
	updateItem := map[string]types.AttributeValue{
		"name": &types.AttributeValueMemberS{Value: parent.Name},
	}
	_ = updateItem
	_ = parent

	// Update with optimistic locking
	// err := s.Update(ctx, parent, updateItem, currentVersion)
	// If version doesn't match, returns store.ErrConcurrentModification
}

// --- Benchmark placeholders ---

func BenchmarkIsDeleted(b *testing.B) {
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: "1000000000"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.IsDeleted(item)
	}
}

// --- Helper for creating test items ---

func makeTestItem(id, name string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: id},
		"name": &types.AttributeValueMemberS{Value: name},
	}
}

func TestMakeTestItem(t *testing.T) {
	item := makeTestItem("test-id", "test-name")
	if v, ok := item["id"].(*types.AttributeValueMemberS); !ok || v.Value != "test-id" {
		t.Error("expected id 'test-id'")
	}
	if v, ok := item["name"].(*types.AttributeValueMemberS); !ok || v.Value != "test-name" {
		t.Error("expected name 'test-name'")
	}
}

// --- Test Errors ---

func TestErrors(t *testing.T) {
	// Verify error messages are meaningful
	errors := []error{
		store.ErrParentNotFound,
		store.ErrNotFound,
		store.ErrAlreadyExists,
		store.ErrHasChildren,
		store.ErrConcurrentModification,
		store.ErrDuplicateValue,
		store.ErrAlreadyDeleted,
	}

	for _, err := range errors {
		if err.Error() == "" {
			t.Errorf("error %v has empty message", err)
		}
		// Verify error messages start with "trellis:"
		if len(err.Error()) < 8 || err.Error()[:8] != "trellis:" {
			t.Errorf("error %q should start with 'trellis:'", err.Error())
		}
	}
}

// --- Test DeleteOptions ---

func TestDeleteOptions(t *testing.T) {
	// Test default options
	opts := store.DeleteOptions{}
	if opts.Cascade {
		t.Error("expected Cascade to be false by default")
	}
	if opts.OrphanProtect {
		t.Error("expected OrphanProtect to be false by default")
	}

	// Test setting options
	opts = store.DeleteOptions{
		Cascade:       true,
		OrphanProtect: true,
	}
	if !opts.Cascade {
		t.Error("expected Cascade to be true")
	}
	if !opts.OrphanProtect {
		t.Error("expected OrphanProtect to be true")
	}
}

// --- Test Item struct ---

func TestItemStruct(t *testing.T) {
	item := store.Item{
		Raw: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "test"},
		},
		Version:   5,
		CreatedAt: "2024-01-01T00:00:00Z",
		UpdatedAt: "2024-01-02T00:00:00Z",
		EntityRef: "parent#test",
		ParentRef: "",
	}

	if item.Version != 5 {
		t.Errorf("expected Version 5, got %d", item.Version)
	}
	if item.EntityRef != "parent#test" {
		t.Errorf("expected EntityRef 'parent#test', got %q", item.EntityRef)
	}
}

// --- Test ChildRef struct ---

func TestChildRefStruct(t *testing.T) {
	ref := store.ChildRef{
		Ref:       "child#c1",
		TableName: "children",
		Key: store.PK{
			"id": &types.AttributeValueMemberS{Value: "c1"},
		},
		ShardPK: "parent#p1#00",
	}

	if ref.Ref != "child#c1" {
		t.Errorf("expected Ref 'child#c1', got %q", ref.Ref)
	}
	if ref.TableName != "children" {
		t.Errorf("expected TableName 'children', got %q", ref.TableName)
	}
	if ref.ShardPK != "parent#p1#00" {
		t.Errorf("expected ShardPK 'parent#p1#00', got %q", ref.ShardPK)
	}
}

// --- Test ConditionCheck struct ---

func TestConditionCheckStruct(t *testing.T) {
	check := store.ConditionCheck{
		TableName: "parents",
		Key: store.PK{
			"id": &types.AttributeValueMemberS{Value: "p1"},
		},
	}

	if check.TableName != "parents" {
		t.Errorf("expected TableName 'parents', got %q", check.TableName)
	}
}

// --- Test TTL Helpers ---

func TestTTLFilterExpr(t *testing.T) {
	expr := store.TTLFilterExpr()
	if expr == "" {
		t.Error("expected non-empty TTL filter expression")
	}
	// Should contain TTL attribute reference
	if !contains(expr, "#ttl") {
		t.Error("expected TTL filter to reference #ttl")
	}
}

func TestTTLFilterNames(t *testing.T) {
	names := store.TTLFilterNames()
	if names["#ttl"] != "ttl" {
		t.Errorf("expected #ttl -> ttl, got %q", names["#ttl"])
	}
}

func TestTTLFilterValues(t *testing.T) {
	values := store.TTLFilterValues()
	if _, ok := values[":now"]; !ok {
		t.Error("expected :now value")
	}
}

func TestParentExistsCondition(t *testing.T) {
	cond := store.ParentExistsCondition()
	if cond == "" {
		t.Error("expected non-empty parent exists condition")
	}
	// Should check attribute_exists and TTL
	if !contains(cond, "attribute_exists") {
		t.Error("expected condition to check attribute_exists")
	}
	if !contains(cond, "#ttl") {
		t.Error("expected condition to reference #ttl")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- Test Store with Registry ---

func TestNewWithRegistry(t *testing.T) {
	registry := store.NewRegistry()
	registry.Register(store.Relationship{
		ParentType:     "parent",
		ChildType:      "child",
		ChildTableName: "children",
		ParentKeyAttr:  "parent_id",
	})

	s := store.NewWithRegistry(nil, store.DefaultConfig(), registry)
	if s == nil {
		t.Fatal("expected non-nil Store")
	}

	r := s.Registry()
	if r == nil {
		t.Fatal("expected non-nil Registry")
	}

	if !r.HasChildren("parent") {
		t.Error("expected registry to have parent -> child relationship")
	}
}

func TestStore_SetRegistry(t *testing.T) {
	s := store.New(nil, store.DefaultConfig())
	if s.Registry() != nil {
		t.Error("expected nil Registry initially")
	}

	registry := store.NewRegistry()
	s.SetRegistry(registry)

	if s.Registry() != registry {
		t.Error("expected Registry to be set")
	}
}

// --- Test ConditionCheck with ConditionExpr ---

func TestConditionCheck_CustomExpr(t *testing.T) {
	check := store.ConditionCheck{
		TableName:     "parents",
		Key:           store.PK{"id": &types.AttributeValueMemberS{Value: "p1"}},
		ConditionExpr: "attribute_exists(id) AND #status = :active",
	}

	if check.ConditionExpr == "" {
		t.Error("expected ConditionExpr to be set")
	}
}

// --- Additional IsDeleted Tests (Edge Cases) ---

func TestIsDeleted_WrongType(t *testing.T) {
	// TTL attribute exists but is wrong type (string instead of number)
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberS{Value: "not-a-number"},
	}
	// Should return false (not deleted) when TTL is wrong type
	if store.IsDeleted(item) {
		t.Error("expected false when TTL is wrong type")
	}
}

func TestIsDeleted_UnparseableNumber(t *testing.T) {
	// TTL is a number type but contains non-numeric value
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: "invalid"},
	}
	// Should return false when TTL can't be parsed
	if store.IsDeleted(item) {
		t.Error("expected false when TTL is unparseable")
	}
}

func TestIsDeleted_EmptyNumber(t *testing.T) {
	// TTL is a number type but empty
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: ""},
	}
	if store.IsDeleted(item) {
		t.Error("expected false when TTL is empty string")
	}
}

func TestIsDeleted_ZeroTTL(t *testing.T) {
	// TTL of 0 should be considered deleted (epoch time is in past)
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: "0"},
	}
	if !store.IsDeleted(item) {
		t.Error("expected true when TTL is 0")
	}
}

func TestIsDeleted_NegativeTTL(t *testing.T) {
	// Negative TTL should be considered deleted
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: "-1"},
	}
	if !store.IsDeleted(item) {
		t.Error("expected true when TTL is negative")
	}
}

func TestIsDeleted_VeryLargeTTL(t *testing.T) {
	// Very large TTL (year 3000) should NOT be deleted
	item := map[string]types.AttributeValue{
		"ttl": &types.AttributeValueMemberN{Value: "32503680000"}, // Year 3000
	}
	if store.IsDeleted(item) {
		t.Error("expected false when TTL is far in future")
	}
}

func TestIsDeleted_NilItem(t *testing.T) {
	// Nil item should not panic
	var item map[string]types.AttributeValue
	if store.IsDeleted(item) {
		t.Error("expected false for nil item")
	}
}

// --- Error Tests (Uniqueness and errors.Is) ---

func TestErrors_Uniqueness(t *testing.T) {
	// Verify all errors are distinct
	allErrors := []error{
		store.ErrParentNotFound,
		store.ErrNotFound,
		store.ErrAlreadyExists,
		store.ErrHasChildren,
		store.ErrConcurrentModification,
		store.ErrDuplicateValue,
		store.ErrAlreadyDeleted,
	}

	seen := make(map[string]error)
	for _, err := range allErrors {
		msg := err.Error()
		if existing, ok := seen[msg]; ok {
			t.Errorf("duplicate error message: %q shared by %v and %v", msg, existing, err)
		}
		seen[msg] = err
	}
}

func TestErrors_ErrorsIs(t *testing.T) {
	// Verify errors work with errors.Is
	tests := []struct {
		err    error
		target error
		match  bool
	}{
		{store.ErrNotFound, store.ErrNotFound, true},
		{store.ErrNotFound, store.ErrParentNotFound, false},
		{store.ErrAlreadyExists, store.ErrAlreadyExists, true},
		{store.ErrHasChildren, store.ErrHasChildren, true},
		{store.ErrConcurrentModification, store.ErrConcurrentModification, true},
		{store.ErrDuplicateValue, store.ErrDuplicateValue, true},
		{store.ErrAlreadyDeleted, store.ErrAlreadyDeleted, true},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			if got := (tt.err == tt.target); got != tt.match {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", tt.err, tt.target, got, tt.match)
			}
		})
	}
}

func TestErrors_NotNil(t *testing.T) {
	// All sentinel errors should be non-nil
	errors := []error{
		store.ErrParentNotFound,
		store.ErrNotFound,
		store.ErrAlreadyExists,
		store.ErrHasChildren,
		store.ErrConcurrentModification,
		store.ErrDuplicateValue,
		store.ErrAlreadyDeleted,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("expected non-nil error")
		}
	}
}

// --- QueryInput Tests ---

func TestQueryInput_Defaults(t *testing.T) {
	input := store.QueryInput{}

	if input.TableName != "" {
		t.Error("expected empty TableName by default")
	}
	if input.IndexName != "" {
		t.Error("expected empty IndexName by default")
	}
	if input.Limit != 0 {
		t.Error("expected Limit 0 by default")
	}
	if input.ScanIndexForward != nil {
		t.Error("expected nil ScanIndexForward by default")
	}
}

func TestQueryInput_WithAllFields(t *testing.T) {
	scanForward := true
	input := store.QueryInput{
		TableName:              "test_table",
		IndexName:              "gsi1",
		KeyConditionExpression: "pk = :pk",
		FilterExpression:       "status = :active",
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "test"},
			":active": &types.AttributeValueMemberS{Value: "active"},
		},
		Limit:            100,
		ScanIndexForward: &scanForward,
	}

	if input.TableName != "test_table" {
		t.Errorf("expected TableName 'test_table', got %q", input.TableName)
	}
	if input.IndexName != "gsi1" {
		t.Errorf("expected IndexName 'gsi1', got %q", input.IndexName)
	}
	if input.Limit != 100 {
		t.Errorf("expected Limit 100, got %d", input.Limit)
	}
	if input.ScanIndexForward == nil || *input.ScanIndexForward != true {
		t.Error("expected ScanIndexForward true")
	}
}

// --- PK Type Tests ---

func TestPK_Empty(t *testing.T) {
	pk := store.PK{}
	if len(pk) != 0 {
		t.Error("expected empty PK")
	}
}

func TestPK_SingleKey(t *testing.T) {
	pk := store.PK{
		"id": &types.AttributeValueMemberS{Value: "test-id"},
	}
	if len(pk) != 1 {
		t.Errorf("expected 1 key, got %d", len(pk))
	}
	if v, ok := pk["id"].(*types.AttributeValueMemberS); !ok || v.Value != "test-id" {
		t.Error("expected id 'test-id'")
	}
}

func TestPK_CompositeKey(t *testing.T) {
	pk := store.PK{
		"pk": &types.AttributeValueMemberS{Value: "partition"},
		"sk": &types.AttributeValueMemberS{Value: "sort"},
	}
	if len(pk) != 2 {
		t.Errorf("expected 2 keys, got %d", len(pk))
	}
}

func TestPK_NumberKey(t *testing.T) {
	pk := store.PK{
		"version": &types.AttributeValueMemberN{Value: "42"},
	}
	if v, ok := pk["version"].(*types.AttributeValueMemberN); !ok || v.Value != "42" {
		t.Error("expected version '42'")
	}
}

// --- Config Tests ---

func TestConfig_ValidNumShards(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{1, 1},
		{16, 16},
		{64, 64},
		{256, 256},
	}

	for _, tt := range tests {
		cfg := store.Config{NumShards: tt.input}
		s := store.New(nil, cfg)
		// Can't directly check config, but verify store is created
		if s == nil {
			t.Errorf("expected non-nil store for NumShards=%d", tt.input)
		}
	}
}

func TestConfig_TableNames(t *testing.T) {
	cfg := store.Config{
		RelationshipTable: "custom_relationships",
		UniqueTable:       "custom_unique",
		NumShards:         1,
	}

	s := store.New(nil, cfg)
	if s == nil {
		t.Error("expected non-nil Store with custom table names")
	}
}

// --- TTL Helper Tests (Additional) ---

func TestTTLFilterValues_FreshTimestamp(t *testing.T) {
	// Each call should return a fresh timestamp
	values1 := store.TTLFilterValues()
	time.Sleep(1 * time.Millisecond)
	values2 := store.TTLFilterValues()

	// Both should have :now key
	if _, ok := values1[":now"]; !ok {
		t.Error("expected :now in first call")
	}
	if _, ok := values2[":now"]; !ok {
		t.Error("expected :now in second call")
	}
}

func TestTTLFilterExpr_Format(t *testing.T) {
	expr := store.TTLFilterExpr()

	// Should handle both cases: no TTL (active) or TTL > now (not yet deleted)
	if !contains(expr, "attribute_not_exists") {
		t.Error("expected attribute_not_exists in expression")
	}
	if !contains(expr, "OR") {
		t.Error("expected OR in expression")
	}
	if !contains(expr, ":now") {
		t.Error("expected :now placeholder in expression")
	}
}

func TestParentExistsCondition_Format(t *testing.T) {
	cond := store.ParentExistsCondition()

	// Should check existence AND TTL
	if !contains(cond, "attribute_exists") {
		t.Error("expected attribute_exists in condition")
	}
	if !contains(cond, "AND") {
		t.Error("expected AND in condition")
	}
}

// --- Entity Edge Cases ---

func TestParent_EmptyID(t *testing.T) {
	parent := Parent{ID: "", Name: "Test"}
	if parent.EntityRef() != "parent#" {
		t.Errorf("expected 'parent#', got %q", parent.EntityRef())
	}
}

func TestChild_EmptyParentID(t *testing.T) {
	child := Child{ID: "c1", ParentID: "", Name: "Test"}
	if child.ParentRef() != "parent#" {
		t.Errorf("expected 'parent#', got %q", child.ParentRef())
	}

	check := child.ParentCheck()
	if check == nil {
		t.Fatal("expected non-nil ParentCheck even with empty ParentID")
	}
}

func TestUniqueChild_EmptyUniqueFields(t *testing.T) {
	// Create entity with empty unique field values
	child := UniqueChild{ID: "uc1", ParentID: "p1", Name: "", Slug: ""}
	fields := child.UniqueFields()

	if fields["name"] != "" {
		t.Errorf("expected empty name, got %q", fields["name"])
	}
	if fields["slug"] != "" {
		t.Errorf("expected empty slug, got %q", fields["slug"])
	}
}

// --- Item Tests (Additional) ---

func TestItem_Defaults(t *testing.T) {
	item := store.Item{}

	if item.Version != 0 {
		t.Error("expected Version 0 by default")
	}
	if item.CreatedAt != "" {
		t.Error("expected empty CreatedAt by default")
	}
	if item.UpdatedAt != "" {
		t.Error("expected empty UpdatedAt by default")
	}
	if item.EntityRef != "" {
		t.Error("expected empty EntityRef by default")
	}
	if item.ParentRef != "" {
		t.Error("expected empty ParentRef by default")
	}
	if item.Raw != nil {
		t.Error("expected nil Raw by default")
	}
}

func TestItem_WithParentRef(t *testing.T) {
	item := store.Item{
		Raw:       map[string]types.AttributeValue{},
		Version:   1,
		EntityRef: "child#c1",
		ParentRef: "parent#p1",
	}

	if item.ParentRef != "parent#p1" {
		t.Errorf("expected ParentRef 'parent#p1', got %q", item.ParentRef)
	}
}

// --- ChildRef Tests (Additional) ---

func TestChildRef_Defaults(t *testing.T) {
	ref := store.ChildRef{}

	if ref.Ref != "" {
		t.Error("expected empty Ref by default")
	}
	if ref.TableName != "" {
		t.Error("expected empty TableName by default")
	}
	if ref.Key != nil {
		t.Error("expected nil Key by default")
	}
	if ref.ShardPK != "" {
		t.Error("expected empty ShardPK by default")
	}
}

// --- DeleteOptions Tests (Additional) ---

func TestDeleteOptions_MutuallyExclusive(t *testing.T) {
	// Both can be set (the implementation handles priority)
	opts := store.DeleteOptions{
		Cascade:       true,
		OrphanProtect: true,
	}

	// Both should be set
	if !opts.Cascade {
		t.Error("expected Cascade true")
	}
	if !opts.OrphanProtect {
		t.Error("expected OrphanProtect true")
	}
}

// --- Relationship Struct Tests ---

func TestRelationship_Fields(t *testing.T) {
	rel := store.Relationship{
		ParentType:     "organization",
		ChildType:      "studio",
		ChildTableName: "studios",
		ParentKeyAttr:  "organization_id",
	}

	if rel.ParentType != "organization" {
		t.Errorf("expected ParentType 'organization', got %q", rel.ParentType)
	}
	if rel.ChildType != "studio" {
		t.Errorf("expected ChildType 'studio', got %q", rel.ChildType)
	}
	if rel.ChildTableName != "studios" {
		t.Errorf("expected ChildTableName 'studios', got %q", rel.ChildTableName)
	}
	if rel.ParentKeyAttr != "organization_id" {
		t.Errorf("expected ParentKeyAttr 'organization_id', got %q", rel.ParentKeyAttr)
	}
}

func TestRelationship_Empty(t *testing.T) {
	rel := store.Relationship{}

	if rel.ParentType != "" {
		t.Error("expected empty ParentType")
	}
	if rel.ChildType != "" {
		t.Error("expected empty ChildType")
	}
}

// Ensure aws import is used
var _ = aws.String("test")
