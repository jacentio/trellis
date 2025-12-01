package shard

import (
	"strings"
	"testing"
)

func TestRelationshipPK_SingleShard(t *testing.T) {
	// With numShards=1, all records should go to shard "00"
	tests := []struct {
		parentRef string
		childRef  string
		expected  string
	}{
		{"parent#p1", "child#c1", "parent#p1#00"},
		{"parent#p1", "child#c2", "parent#p1#00"},
		{"parent#p2", "child#c1", "parent#p2#00"},
		{"org#abc", "studio#xyz", "org#abc#00"},
	}

	for _, tt := range tests {
		result := RelationshipPK(tt.parentRef, tt.childRef, 1)
		if result != tt.expected {
			t.Errorf("RelationshipPK(%q, %q, 1) = %q, want %q",
				tt.parentRef, tt.childRef, result, tt.expected)
		}
	}
}

func TestRelationshipPK_ZeroShards(t *testing.T) {
	// Zero or negative shards should be treated as 1
	result := RelationshipPK("parent#p1", "child#c1", 0)
	if result != "parent#p1#00" {
		t.Errorf("expected 'parent#p1#00', got %q", result)
	}

	result = RelationshipPK("parent#p1", "child#c1", -1)
	if result != "parent#p1#00" {
		t.Errorf("expected 'parent#p1#00', got %q", result)
	}
}

func TestRelationshipPK_MultipleShards(t *testing.T) {
	// With numShards=256, different child refs should produce different shards
	parentRef := "parent#p1"
	numShards := 256

	// Generate PKs for multiple children
	shardCounts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		childRef := "child#" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		pk := RelationshipPK(parentRef, childRef, numShards)

		// Verify format: parentRef#XX (where XX is hex)
		if !strings.HasPrefix(pk, parentRef+"#") {
			t.Errorf("expected prefix %q#, got %q", parentRef, pk)
		}

		// Extract and count shard
		shard := pk[len(parentRef)+1:]
		shardCounts[shard]++
	}

	// Should have distribution across multiple shards (not all in one)
	if len(shardCounts) < 10 {
		t.Errorf("expected distribution across multiple shards, got only %d unique shards", len(shardCounts))
	}
}

func TestRelationshipPK_Deterministic(t *testing.T) {
	// Same inputs should always produce same output
	parentRef := "parent#p1"
	childRef := "child#c1"
	numShards := 256

	first := RelationshipPK(parentRef, childRef, numShards)
	for i := 0; i < 100; i++ {
		result := RelationshipPK(parentRef, childRef, numShards)
		if result != first {
			t.Errorf("expected deterministic result %q, got %q on iteration %d", first, result, i)
		}
	}
}

func TestRelationshipPK_HexFormat(t *testing.T) {
	// Shard should be 2-character hex (00-ff)
	result := RelationshipPK("parent#p1", "child#test", 256)
	parts := strings.Split(result, "#")
	if len(parts) < 3 {
		t.Fatalf("expected at least 3 parts, got %d: %q", len(parts), result)
	}

	shard := parts[len(parts)-1]
	if len(shard) != 2 {
		t.Errorf("expected 2-character shard, got %q", shard)
	}

	// Verify it's valid hex
	for _, c := range shard {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("expected hex character, got %c", c)
		}
	}
}

func TestUniqueConstraintPK(t *testing.T) {
	tests := []struct {
		parentRef  string
		entityType string
		field      string
		value      string
	}{
		{"parent#p1", "child", "name", "Test"},
		{"studio#s1", "title", "slug", "my-title"},
		{"org#o1", "studio", "name", "Acme Studios"},
	}

	for _, tt := range tests {
		result := UniqueConstraintPK(tt.parentRef, tt.entityType, tt.field, tt.value)

		// Should be 32 characters (128-bit hash as hex)
		if len(result) != 32 {
			t.Errorf("UniqueConstraintPK(%q, %q, %q, %q) = %q (len=%d), want 32 chars",
				tt.parentRef, tt.entityType, tt.field, tt.value, result, len(result))
		}

		// Should be valid hex
		for _, c := range result {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Errorf("expected hex character, got %c in %q", c, result)
			}
		}
	}
}

func TestUniqueConstraintPK_Deterministic(t *testing.T) {
	first := UniqueConstraintPK("parent#p1", "child", "name", "Test")
	for i := 0; i < 100; i++ {
		result := UniqueConstraintPK("parent#p1", "child", "name", "Test")
		if result != first {
			t.Errorf("expected deterministic result %q, got %q on iteration %d", first, result, i)
		}
	}
}

func TestUniqueConstraintPK_Uniqueness(t *testing.T) {
	// Different inputs should produce different hashes
	pks := make(map[string]string)

	inputs := []struct {
		parentRef  string
		entityType string
		field      string
		value      string
	}{
		{"parent#p1", "child", "name", "Test1"},
		{"parent#p1", "child", "name", "Test2"},
		{"parent#p1", "child", "slug", "Test1"},
		{"parent#p2", "child", "name", "Test1"},
		{"parent#p1", "other", "name", "Test1"},
	}

	for _, input := range inputs {
		pk := UniqueConstraintPK(input.parentRef, input.entityType, input.field, input.value)
		key := input.parentRef + "|" + input.entityType + "|" + input.field + "|" + input.value

		if existing, ok := pks[pk]; ok {
			t.Errorf("collision: %q and %q both produce %q", existing, key, pk)
		}
		pks[pk] = key
	}
}

func BenchmarkRelationshipPK_SingleShard(b *testing.B) {
	parentRef := "parent#550e8400-e29b-41d4-a716-446655440000"
	childRef := "child#6ba7b810-9dad-11d1-80b4-00c04fd430c8"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RelationshipPK(parentRef, childRef, 1)
	}
}

func BenchmarkRelationshipPK_256Shards(b *testing.B) {
	parentRef := "parent#550e8400-e29b-41d4-a716-446655440000"
	childRef := "child#6ba7b810-9dad-11d1-80b4-00c04fd430c8"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RelationshipPK(parentRef, childRef, 256)
	}
}

func BenchmarkUniqueConstraintPK(b *testing.B) {
	parentRef := "studio#550e8400-e29b-41d4-a716-446655440000"
	entityType := "title"
	field := "name"
	value := "Avatar: The Way of Water"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UniqueConstraintPK(parentRef, entityType, field, value)
	}
}

// --- RelationshipPK Edge Cases ---

func TestRelationshipPK_EmptyParentRef(t *testing.T) {
	result := RelationshipPK("", "child#c1", 1)
	if result != "#00" {
		t.Errorf("expected '#00', got %q", result)
	}
}

func TestRelationshipPK_EmptyChildRef(t *testing.T) {
	result := RelationshipPK("parent#p1", "", 1)
	if result != "parent#p1#00" {
		t.Errorf("expected 'parent#p1#00', got %q", result)
	}
}

func TestRelationshipPK_BothEmpty(t *testing.T) {
	result := RelationshipPK("", "", 1)
	if result != "#00" {
		t.Errorf("expected '#00', got %q", result)
	}
}

func TestRelationshipPK_VeryLargeShardCount(t *testing.T) {
	// Even though max supported is 256, verify behavior with larger values
	result := RelationshipPK("parent#p1", "child#c1", 1000)
	// Should still produce valid hex suffix
	if !strings.HasPrefix(result, "parent#p1#") {
		t.Errorf("expected prefix 'parent#p1#', got %q", result)
	}
}

func TestRelationshipPK_PowerOf2Shards(t *testing.T) {
	// Test common power-of-2 shard counts
	shardCounts := []int{2, 4, 8, 16, 32, 64, 128, 256}
	for _, numShards := range shardCounts {
		result := RelationshipPK("parent#p1", "child#c1", numShards)
		if !strings.HasPrefix(result, "parent#p1#") {
			t.Errorf("NumShards=%d: expected prefix 'parent#p1#', got %q", numShards, result)
		}
	}
}

func TestRelationshipPK_SpecialCharacters(t *testing.T) {
	// Parent and child refs with special characters
	result := RelationshipPK("parent#special/chars:here", "child#more/special:chars", 256)
	if !strings.HasPrefix(result, "parent#special/chars:here#") {
		t.Errorf("expected prefix with special chars, got %q", result)
	}
}

func TestRelationshipPK_Unicode(t *testing.T) {
	// Unicode in refs
	result := RelationshipPK("親#p1", "子#c1", 256)
	if !strings.HasPrefix(result, "親#p1#") {
		t.Errorf("expected unicode prefix, got %q", result)
	}
}

func TestRelationshipPK_LongRefs(t *testing.T) {
	// Very long refs
	longParent := "parent#" + strings.Repeat("a", 1000)
	longChild := "child#" + strings.Repeat("b", 1000)
	result := RelationshipPK(longParent, longChild, 256)
	if !strings.HasPrefix(result, longParent+"#") {
		t.Error("expected long parent prefix")
	}
}

func TestRelationshipPK_ShardDistribution_16Shards(t *testing.T) {
	// Verify distribution with 16 shards
	parentRef := "parent#p1"
	numShards := 16
	shardCounts := make(map[string]int)

	for i := 0; i < 1000; i++ {
		childRef := "child#" + strings.Repeat("x", i%50) + string(rune('a'+i%26))
		pk := RelationshipPK(parentRef, childRef, numShards)
		shard := pk[len(parentRef)+1:]
		shardCounts[shard]++
	}

	// Should have reasonable distribution
	if len(shardCounts) < 8 {
		t.Errorf("expected distribution across multiple shards with 16 shards, got only %d unique shards", len(shardCounts))
	}
}

func TestRelationshipPK_SameChildDifferentParent(t *testing.T) {
	// Same child under different parents should go to different PKs
	child := "child#c1"
	pk1 := RelationshipPK("parent#p1", child, 256)
	pk2 := RelationshipPK("parent#p2", child, 256)

	// PKs should be different (different parent prefix)
	if pk1 == pk2 {
		t.Error("expected different PKs for different parents")
	}
}

// --- UniqueConstraintPK Edge Cases ---

func TestUniqueConstraintPK_EmptyInputs(t *testing.T) {
	// All empty inputs
	result := UniqueConstraintPK("", "", "", "")
	if len(result) != 32 {
		t.Errorf("expected 32 char hash even for empty inputs, got %d", len(result))
	}
}

func TestUniqueConstraintPK_EmptyValue(t *testing.T) {
	result := UniqueConstraintPK("parent#p1", "child", "name", "")
	if len(result) != 32 {
		t.Errorf("expected 32 char hash for empty value, got %d", len(result))
	}
}

func TestUniqueConstraintPK_WhitespaceValue(t *testing.T) {
	// Whitespace should be treated differently from empty
	result1 := UniqueConstraintPK("parent#p1", "child", "name", "")
	result2 := UniqueConstraintPK("parent#p1", "child", "name", " ")
	result3 := UniqueConstraintPK("parent#p1", "child", "name", "  ")

	if result1 == result2 {
		t.Error("empty and single space should produce different hashes")
	}
	if result2 == result3 {
		t.Error("single space and double space should produce different hashes")
	}
}

func TestUniqueConstraintPK_CaseSensitive(t *testing.T) {
	// Case should matter
	result1 := UniqueConstraintPK("parent#p1", "child", "name", "Test")
	result2 := UniqueConstraintPK("parent#p1", "child", "name", "test")
	result3 := UniqueConstraintPK("parent#p1", "child", "name", "TEST")

	if result1 == result2 {
		t.Error("expected different hashes for different cases")
	}
	if result2 == result3 {
		t.Error("expected different hashes for different cases")
	}
}

func TestUniqueConstraintPK_Unicode(t *testing.T) {
	result := UniqueConstraintPK("親#p1", "子タイプ", "名前", "日本語テスト")
	if len(result) != 32 {
		t.Errorf("expected 32 char hash for unicode, got %d", len(result))
	}

	// Verify valid hex
	for _, c := range result {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("expected hex character, got %c in %q", c, result)
		}
	}
}

func TestUniqueConstraintPK_LongValue(t *testing.T) {
	// Very long value
	longValue := strings.Repeat("a", 10000)
	result := UniqueConstraintPK("parent#p1", "child", "name", longValue)
	if len(result) != 32 {
		t.Errorf("expected 32 char hash for long value, got %d", len(result))
	}
}

func TestUniqueConstraintPK_SpecialCharacters(t *testing.T) {
	// Special characters in all fields
	result := UniqueConstraintPK(
		"parent#special/chars:here",
		"entity:type/with:special",
		"field.name",
		"value with spaces and !@#$%^&*()",
	)
	if len(result) != 32 {
		t.Errorf("expected 32 char hash for special chars, got %d", len(result))
	}
}

func TestUniqueConstraintPK_Collisions(t *testing.T) {
	// Generate many PKs and check for collisions
	pks := make(map[string]string)
	collisions := 0

	for i := 0; i < 10000; i++ {
		parentRef := "parent#p" + string(rune('0'+i%10))
		entityType := "type" + string(rune('a'+i%26))
		field := "field" + string(rune('0'+i%10))
		value := "value" + string(rune('a'+i%26)) + string(rune('0'+i%10))

		pk := UniqueConstraintPK(parentRef, entityType, field, value)
		key := parentRef + "|" + entityType + "|" + field + "|" + value

		if existing, ok := pks[pk]; ok && existing != key {
			collisions++
		}
		pks[pk] = key
	}

	if collisions > 0 {
		t.Errorf("found %d collisions in 10000 unique constraint PKs", collisions)
	}
}

func TestUniqueConstraintPK_OrderMatters(t *testing.T) {
	// Order of fields should matter
	result1 := UniqueConstraintPK("a", "b", "c", "d")
	result2 := UniqueConstraintPK("b", "a", "c", "d")
	result3 := UniqueConstraintPK("a", "b", "d", "c")

	if result1 == result2 {
		t.Error("swapping parent and entity type should produce different hash")
	}
	if result1 == result3 {
		t.Error("swapping field and value should produce different hash")
	}
}

func TestUniqueConstraintPK_SimilarInputs(t *testing.T) {
	// Inputs that could produce similar concatenations
	result1 := UniqueConstraintPK("ab", "cd", "ef", "gh")
	result2 := UniqueConstraintPK("a", "bcd", "ef", "gh")
	result3 := UniqueConstraintPK("ab", "c", "def", "gh")

	if result1 == result2 {
		t.Error("similar concatenations should produce different hashes (1 vs 2)")
	}
	if result1 == result3 {
		t.Error("similar concatenations should produce different hashes (1 vs 3)")
	}
}

func TestUniqueConstraintPK_Newlines(t *testing.T) {
	// Value with newlines
	result1 := UniqueConstraintPK("parent#p1", "child", "name", "line1\nline2")
	result2 := UniqueConstraintPK("parent#p1", "child", "name", "line1\r\nline2")
	result3 := UniqueConstraintPK("parent#p1", "child", "name", "line1line2")

	// All should be different
	if result1 == result2 {
		t.Error("different newline styles should produce different hashes")
	}
	if result1 == result3 {
		t.Error("with and without newlines should produce different hashes")
	}
}

func TestUniqueConstraintPK_NullBytes(t *testing.T) {
	// Value with null bytes
	result := UniqueConstraintPK("parent#p1", "child", "name", "before\x00after")
	if len(result) != 32 {
		t.Errorf("expected 32 char hash with null bytes, got %d", len(result))
	}
}
