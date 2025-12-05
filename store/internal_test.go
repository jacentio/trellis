package store

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- joinStrings Tests ---

func TestJoinStrings_Empty(t *testing.T) {
	result := joinStrings([]string{}, ", ")
	if result != "" {
		t.Errorf("expected empty string for empty slice, got %q", result)
	}
}

func TestJoinStrings_Single(t *testing.T) {
	result := joinStrings([]string{"one"}, ", ")
	if result != "one" {
		t.Errorf("expected 'one', got %q", result)
	}
}

func TestJoinStrings_Multiple(t *testing.T) {
	result := joinStrings([]string{"a", "b", "c"}, ", ")
	if result != "a, b, c" {
		t.Errorf("expected 'a, b, c', got %q", result)
	}
}

func TestJoinStrings_EmptySeparator(t *testing.T) {
	result := joinStrings([]string{"a", "b", "c"}, "")
	if result != "abc" {
		t.Errorf("expected 'abc', got %q", result)
	}
}

func TestJoinStrings_DifferentSeparators(t *testing.T) {
	tests := []struct {
		name     string
		strs     []string
		sep      string
		expected string
	}{
		{"comma", []string{"a", "b"}, ",", "a,b"},
		{"space", []string{"a", "b"}, " ", "a b"},
		{"newline", []string{"a", "b"}, "\n", "a\nb"},
		{"multi-char", []string{"a", "b"}, " AND ", "a AND b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := joinStrings(tt.strs, tt.sep)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestJoinStrings_EmptyStrings(t *testing.T) {
	result := joinStrings([]string{"", "", ""}, ", ")
	if result != ", , " {
		t.Errorf("expected ', , ', got %q", result)
	}
}

func TestJoinStrings_UpdateExpression(t *testing.T) {
	// Test a realistic use case: building SET clauses
	clauses := []string{"#attr0 = :val0", "#attr1 = :val1", "#updated_at = :updated_at"}
	result := joinStrings(clauses, ", ")
	expected := "#attr0 = :val0, #attr1 = :val1, #updated_at = :updated_at"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// --- unmarshalItem Tests ---

func TestUnmarshalItem_Full(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"id":         &types.AttributeValueMemberS{Value: "test-id"},
		"version":    &types.AttributeValueMemberN{Value: "5"},
		"created_at": &types.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"},
		"updated_at": &types.AttributeValueMemberS{Value: "2024-01-02T00:00:00Z"},
		"entity_ref": &types.AttributeValueMemberS{Value: "parent#test-id"},
		"parent_ref": &types.AttributeValueMemberS{Value: "grandparent#gp-id"},
	}

	item := s.unmarshalItem(raw)

	if item.Version != 5 {
		t.Errorf("expected Version 5, got %d", item.Version)
	}
	if item.CreatedAt != "2024-01-01T00:00:00Z" {
		t.Errorf("expected CreatedAt '2024-01-01T00:00:00Z', got %q", item.CreatedAt)
	}
	if item.UpdatedAt != "2024-01-02T00:00:00Z" {
		t.Errorf("expected UpdatedAt '2024-01-02T00:00:00Z', got %q", item.UpdatedAt)
	}
	if item.EntityRef != "parent#test-id" {
		t.Errorf("expected EntityRef 'parent#test-id', got %q", item.EntityRef)
	}
	if item.ParentRef != "grandparent#gp-id" {
		t.Errorf("expected ParentRef 'grandparent#gp-id', got %q", item.ParentRef)
	}
	if item.Raw == nil {
		t.Error("expected Raw to be set")
	}
}

func TestUnmarshalItem_Minimal(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: "test-id"},
	}

	item := s.unmarshalItem(raw)

	if item.Version != 0 {
		t.Errorf("expected Version 0 for missing version, got %d", item.Version)
	}
	if item.CreatedAt != "" {
		t.Errorf("expected empty CreatedAt, got %q", item.CreatedAt)
	}
	if item.UpdatedAt != "" {
		t.Errorf("expected empty UpdatedAt, got %q", item.UpdatedAt)
	}
	if item.EntityRef != "" {
		t.Errorf("expected empty EntityRef, got %q", item.EntityRef)
	}
	if item.ParentRef != "" {
		t.Errorf("expected empty ParentRef, got %q", item.ParentRef)
	}
}

func TestUnmarshalItem_InvalidVersionType(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"version": &types.AttributeValueMemberS{Value: "not-a-number"}, // Wrong type
	}

	item := s.unmarshalItem(raw)

	// Should default to 0 when version is wrong type
	if item.Version != 0 {
		t.Errorf("expected Version 0 for wrong type, got %d", item.Version)
	}
}

func TestUnmarshalItem_UnparseableVersion(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"version": &types.AttributeValueMemberN{Value: "invalid"}, // Not a valid number
	}

	item := s.unmarshalItem(raw)

	// Should default to 0 when version can't be parsed
	if item.Version != 0 {
		t.Errorf("expected Version 0 for unparseable, got %d", item.Version)
	}
}

func TestUnmarshalItem_LargeVersion(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"version": &types.AttributeValueMemberN{Value: "9223372036854775807"}, // max int64
	}

	item := s.unmarshalItem(raw)

	if item.Version != 9223372036854775807 {
		t.Errorf("expected max int64, got %d", item.Version)
	}
}

func TestUnmarshalItem_RootEntity(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"id":         &types.AttributeValueMemberS{Value: "org-123"},
		"entity_ref": &types.AttributeValueMemberS{Value: "organization#org-123"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
	}

	item := s.unmarshalItem(raw)

	if item.ParentRef != "" {
		t.Errorf("expected empty ParentRef for root entity, got %q", item.ParentRef)
	}
	if item.EntityRef != "organization#org-123" {
		t.Errorf("expected EntityRef 'organization#org-123', got %q", item.EntityRef)
	}
}

func TestUnmarshalItem_PreservesRaw(t *testing.T) {
	s := &Store{}
	raw := map[string]types.AttributeValue{
		"id":           &types.AttributeValueMemberS{Value: "test"},
		"custom_field": &types.AttributeValueMemberS{Value: "custom-value"},
	}

	item := s.unmarshalItem(raw)

	if item.Raw == nil {
		t.Fatal("expected Raw to be set")
	}
	if v, ok := item.Raw["custom_field"].(*types.AttributeValueMemberS); !ok || v.Value != "custom-value" {
		t.Error("expected custom_field to be preserved in Raw")
	}
}

// --- unmarshalChildRef Tests ---

func TestUnmarshalChildRef_Full(t *testing.T) {
	s := &Store{}
	item := map[string]types.AttributeValue{
		"child_ref":   &types.AttributeValueMemberS{Value: "child#c123"},
		"child_table": &types.AttributeValueMemberS{Value: "children"},
		"child_key": &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "c123"},
			},
		},
	}
	shardPK := "parent#p123#00"

	ref := s.unmarshalChildRef(item, shardPK)

	if ref.Ref != "child#c123" {
		t.Errorf("expected Ref 'child#c123', got %q", ref.Ref)
	}
	if ref.TableName != "children" {
		t.Errorf("expected TableName 'children', got %q", ref.TableName)
	}
	if ref.ShardPK != shardPK {
		t.Errorf("expected ShardPK %q, got %q", shardPK, ref.ShardPK)
	}
	if ref.Key == nil {
		t.Fatal("expected Key to be set")
	}
	if v, ok := ref.Key["id"].(*types.AttributeValueMemberS); !ok || v.Value != "c123" {
		t.Error("expected Key[id] to be 'c123'")
	}
}

func TestUnmarshalChildRef_Minimal(t *testing.T) {
	s := &Store{}
	item := map[string]types.AttributeValue{}
	shardPK := "parent#p123#00"

	ref := s.unmarshalChildRef(item, shardPK)

	if ref.Ref != "" {
		t.Errorf("expected empty Ref, got %q", ref.Ref)
	}
	if ref.TableName != "" {
		t.Errorf("expected empty TableName, got %q", ref.TableName)
	}
	if ref.ShardPK != shardPK {
		t.Errorf("expected ShardPK %q, got %q", shardPK, ref.ShardPK)
	}
	if ref.Key != nil {
		t.Error("expected nil Key")
	}
}

func TestUnmarshalChildRef_CompositeKey(t *testing.T) {
	s := &Store{}
	item := map[string]types.AttributeValue{
		"child_ref":   &types.AttributeValueMemberS{Value: "child#c123"},
		"child_table": &types.AttributeValueMemberS{Value: "children"},
		"child_key": &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "partition"},
				"sk": &types.AttributeValueMemberS{Value: "sort"},
			},
		},
	}

	ref := s.unmarshalChildRef(item, "parent#p1#00")

	if len(ref.Key) != 2 {
		t.Errorf("expected 2 keys, got %d", len(ref.Key))
	}
}

func TestUnmarshalChildRef_WrongKeyType(t *testing.T) {
	s := &Store{}
	item := map[string]types.AttributeValue{
		"child_ref":   &types.AttributeValueMemberS{Value: "child#c123"},
		"child_table": &types.AttributeValueMemberS{Value: "children"},
		"child_key":   &types.AttributeValueMemberS{Value: "not-a-map"}, // Wrong type
	}

	ref := s.unmarshalChildRef(item, "parent#p1#00")

	// Should have nil Key when type is wrong
	if ref.Key != nil {
		t.Error("expected nil Key for wrong type")
	}
}

// --- mapCreateTransactionError Tests ---

func TestMapCreateTransactionError_NilError(t *testing.T) {
	s := &Store{}
	err := s.mapCreateTransactionError(nil, 0, 1)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestMapCreateTransactionError_NonTransactionError(t *testing.T) {
	s := &Store{}
	originalErr := errors.New("some other error")
	err := s.mapCreateTransactionError(originalErr, 0, 1)
	if err != originalErr {
		t.Errorf("expected original error, got %v", err)
	}
}

func TestMapCreateTransactionError_ParentCheckFailure(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: &code}, // Index 0 - parent check
			{},            // Index 1 - entity put
		},
	}

	err := s.mapCreateTransactionError(txErr, 0, 1)
	if !errors.Is(err, ErrParentNotFound) {
		t.Errorf("expected ErrParentNotFound, got %v", err)
	}
}

func TestMapCreateTransactionError_EntityPutFailure(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{},            // Index 0 - parent check (no failure)
			{Code: &code}, // Index 1 - entity put
		},
	}

	err := s.mapCreateTransactionError(txErr, 0, 1)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestMapCreateTransactionError_UniqueConstraintFailure(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{},            // Index 0 - parent check
			{},            // Index 1 - entity put
			{Code: &code}, // Index 2 - unique constraint
		},
	}

	err := s.mapCreateTransactionError(txErr, 0, 1)
	if !errors.Is(err, ErrDuplicateValue) {
		t.Errorf("expected ErrDuplicateValue, got %v", err)
	}
}

func TestMapCreateTransactionError_NoParentCheck(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: &code}, // Index 0 - entity put (parentCheckIndex is -1)
		},
	}

	// parentCheckIndex = -1 means no parent check
	err := s.mapCreateTransactionError(txErr, -1, 0)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestMapCreateTransactionError_OtherCancellationCode(t *testing.T) {
	s := &Store{}
	code := "TransactionConflict" // Different code
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: &code},
		},
	}

	err := s.mapCreateTransactionError(txErr, 0, 1)
	// Should return original error when code is not ConditionalCheckFailed
	if err == nil || errors.Is(err, ErrParentNotFound) || errors.Is(err, ErrAlreadyExists) {
		t.Error("expected original transaction error")
	}
}

func TestMapCreateTransactionError_NilCode(t *testing.T) {
	s := &Store{}
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: nil}, // Nil code
		},
	}

	err := s.mapCreateTransactionError(txErr, 0, 1)
	// Should return original error when code is nil
	if err != txErr {
		t.Errorf("expected original error for nil code, got %v", err)
	}
}

// --- mapUpdateTransactionError Tests ---

func TestMapUpdateTransactionError_NilError(t *testing.T) {
	s := &Store{}
	err := s.mapUpdateTransactionError(nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestMapUpdateTransactionError_NonTransactionError(t *testing.T) {
	s := &Store{}
	originalErr := errors.New("some other error")
	err := s.mapUpdateTransactionError(originalErr)
	if err != originalErr {
		t.Errorf("expected original error, got %v", err)
	}
}

func TestMapUpdateTransactionError_ConditionalCheckFailed(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: &code},
		},
	}

	err := s.mapUpdateTransactionError(txErr)
	if !errors.Is(err, ErrDuplicateValue) {
		t.Errorf("expected ErrDuplicateValue, got %v", err)
	}
}

func TestMapUpdateTransactionError_OtherCode(t *testing.T) {
	s := &Store{}
	code := "TransactionConflict"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: &code},
		},
	}

	err := s.mapUpdateTransactionError(txErr)
	// Should return original error for other codes
	if err == nil || errors.Is(err, ErrDuplicateValue) {
		t.Error("expected original error")
	}
}

func TestMapUpdateTransactionError_MultipleReasons(t *testing.T) {
	s := &Store{}
	code := "ConditionalCheckFailed"
	txErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: nil},
			{Code: &code}, // Second reason is the failure
			{Code: nil},
		},
	}

	err := s.mapUpdateTransactionError(txErr)
	if !errors.Is(err, ErrDuplicateValue) {
		t.Errorf("expected ErrDuplicateValue, got %v", err)
	}
}

// --- Config.validate Tests ---

func TestConfigValidate_Defaults(t *testing.T) {
	cfg := Config{}
	cfg.validate()

	if cfg.RelationshipTable != "trellis_relationships" {
		t.Errorf("expected default RelationshipTable, got %q", cfg.RelationshipTable)
	}
	if cfg.UniqueTable != "trellis_unique_constraints" {
		t.Errorf("expected default UniqueTable, got %q", cfg.UniqueTable)
	}
	if cfg.NumShards != 1 {
		t.Errorf("expected NumShards 1, got %d", cfg.NumShards)
	}
}

func TestConfigValidate_NumShardsZero(t *testing.T) {
	cfg := Config{NumShards: 0}
	cfg.validate()

	if cfg.NumShards != 1 {
		t.Errorf("expected NumShards 1 for 0, got %d", cfg.NumShards)
	}
}

func TestConfigValidate_NumShardsNegative(t *testing.T) {
	cfg := Config{NumShards: -10}
	cfg.validate()

	if cfg.NumShards != 1 {
		t.Errorf("expected NumShards 1 for -10, got %d", cfg.NumShards)
	}
}

func TestConfigValidate_NumShardsOverMax(t *testing.T) {
	cfg := Config{NumShards: 500}
	cfg.validate()

	if cfg.NumShards != 256 {
		t.Errorf("expected NumShards 256 for 500, got %d", cfg.NumShards)
	}
}

func TestConfigValidate_NumShardsAtMax(t *testing.T) {
	cfg := Config{NumShards: 256}
	cfg.validate()

	if cfg.NumShards != 256 {
		t.Errorf("expected NumShards 256, got %d", cfg.NumShards)
	}
}

func TestConfigValidate_PreservesCustomTableNames(t *testing.T) {
	cfg := Config{
		RelationshipTable: "custom_rels",
		UniqueTable:       "custom_unique",
		NumShards:         16,
	}
	cfg.validate()

	if cfg.RelationshipTable != "custom_rels" {
		t.Errorf("expected custom RelationshipTable, got %q", cfg.RelationshipTable)
	}
	if cfg.UniqueTable != "custom_unique" {
		t.Errorf("expected custom UniqueTable, got %q", cfg.UniqueTable)
	}
}

// --- Store relationshipPK Tests ---

func TestStore_RelationshipPK(t *testing.T) {
	s := &Store{
		config: Config{NumShards: 16},
	}

	pk := s.relationshipPK("parent#p1", "child#c1")

	// Should start with parent ref
	if len(pk) < len("parent#p1#") {
		t.Errorf("expected pk to start with 'parent#p1#', got %q", pk)
	}
	if pk[:len("parent#p1#")] != "parent#p1#" {
		t.Errorf("expected pk to start with 'parent#p1#', got %q", pk)
	}
}

func TestStore_RelationshipPK_SingleShard(t *testing.T) {
	s := &Store{
		config: Config{NumShards: 1},
	}

	pk := s.relationshipPK("parent#p1", "child#c1")

	if pk != "parent#p1#00" {
		t.Errorf("expected 'parent#p1#00', got %q", pk)
	}
}

// Ensure aws import is used
var _ = aws.String("test")
