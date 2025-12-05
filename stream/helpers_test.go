package stream

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

// --- getStringAttr Tests ---

func TestGetStringAttr_ExistingString(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"name": events.NewStringAttribute("test-value"),
	}

	result := getStringAttr(image, "name")
	if result != "test-value" {
		t.Errorf("expected 'test-value', got %q", result)
	}
}

func TestGetStringAttr_MissingKey(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"other": events.NewStringAttribute("value"),
	}

	result := getStringAttr(image, "name")
	if result != "" {
		t.Errorf("expected empty string for missing key, got %q", result)
	}
}

func TestGetStringAttr_EmptyImage(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{}

	result := getStringAttr(image, "name")
	if result != "" {
		t.Errorf("expected empty string for empty image, got %q", result)
	}
}

func TestGetStringAttr_NilImage(t *testing.T) {
	var image map[string]events.DynamoDBAttributeValue

	result := getStringAttr(image, "name")
	if result != "" {
		t.Errorf("expected empty string for nil image, got %q", result)
	}
}

func TestGetStringAttr_EmptyStringValue(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"name": events.NewStringAttribute(""),
	}

	result := getStringAttr(image, "name")
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestGetStringAttr_UnicodeValue(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"name": events.NewStringAttribute("日本語テスト"),
	}

	result := getStringAttr(image, "name")
	if result != "日本語テスト" {
		t.Errorf("expected '日本語テスト', got %q", result)
	}
}

func TestGetStringAttr_SpecialCharacters(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"name": events.NewStringAttribute("value#with:special/chars"),
	}

	result := getStringAttr(image, "name")
	if result != "value#with:special/chars" {
		t.Errorf("expected 'value#with:special/chars', got %q", result)
	}
}

// --- getNumberAttr Tests ---

func TestGetNumberAttr_ValidNumber(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"ttl": events.NewNumberAttribute("1234567890"),
	}

	result := getNumberAttr(image, "ttl")
	if result != 1234567890 {
		t.Errorf("expected 1234567890, got %d", result)
	}
}

func TestGetNumberAttr_Zero(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"count": events.NewNumberAttribute("0"),
	}

	result := getNumberAttr(image, "count")
	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestGetNumberAttr_NegativeNumber(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"offset": events.NewNumberAttribute("-100"),
	}

	result := getNumberAttr(image, "offset")
	if result != -100 {
		t.Errorf("expected -100, got %d", result)
	}
}

func TestGetNumberAttr_MissingKey(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"other": events.NewNumberAttribute("42"),
	}

	result := getNumberAttr(image, "ttl")
	if result != 0 {
		t.Errorf("expected 0 for missing key, got %d", result)
	}
}

func TestGetNumberAttr_EmptyImage(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{}

	result := getNumberAttr(image, "ttl")
	if result != 0 {
		t.Errorf("expected 0 for empty image, got %d", result)
	}
}

func TestGetNumberAttr_NilImage(t *testing.T) {
	var image map[string]events.DynamoDBAttributeValue

	result := getNumberAttr(image, "ttl")
	if result != 0 {
		t.Errorf("expected 0 for nil image, got %d", result)
	}
}

func TestGetNumberAttr_StringAttribute(t *testing.T) {
	// When attribute exists but is wrong type (string instead of number)
	image := map[string]events.DynamoDBAttributeValue{
		"ttl": events.NewStringAttribute("not-a-number"),
	}

	result := getNumberAttr(image, "ttl")
	if result != 0 {
		t.Errorf("expected 0 for string attribute, got %d", result)
	}
}

func TestGetNumberAttr_LargeNumber(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"big": events.NewNumberAttribute("9223372036854775807"), // max int64
	}

	result := getNumberAttr(image, "big")
	if result != 9223372036854775807 {
		t.Errorf("expected 9223372036854775807, got %d", result)
	}
}

func TestGetNumberAttr_MinInt64(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"min": events.NewNumberAttribute("-9223372036854775808"), // min int64
	}

	result := getNumberAttr(image, "min")
	if result != -9223372036854775808 {
		t.Errorf("expected -9223372036854775808, got %d", result)
	}
}

// --- getStringListAttr Tests ---

func TestGetStringListAttr_ValidList(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"_unique_pks": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("pk1"),
			events.NewStringAttribute("pk2"),
			events.NewStringAttribute("pk3"),
		}),
	}

	result := getStringListAttr(image, "_unique_pks")
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0] != "pk1" || result[1] != "pk2" || result[2] != "pk3" {
		t.Errorf("expected [pk1, pk2, pk3], got %v", result)
	}
}

func TestGetStringListAttr_EmptyList(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"_unique_pks": events.NewListAttribute([]events.DynamoDBAttributeValue{}),
	}

	result := getStringListAttr(image, "_unique_pks")
	if len(result) != 0 {
		t.Errorf("expected nil or empty slice for empty list, got %v", result)
	}
}

func TestGetStringListAttr_MissingKey(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"other": events.NewStringAttribute("value"),
	}

	result := getStringListAttr(image, "_unique_pks")
	if result != nil {
		t.Errorf("expected nil for missing key, got %v", result)
	}
}

func TestGetStringListAttr_EmptyImage(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{}

	result := getStringListAttr(image, "_unique_pks")
	if result != nil {
		t.Errorf("expected nil for empty image, got %v", result)
	}
}

func TestGetStringListAttr_NilImage(t *testing.T) {
	var image map[string]events.DynamoDBAttributeValue

	result := getStringListAttr(image, "_unique_pks")
	if result != nil {
		t.Errorf("expected nil for nil image, got %v", result)
	}
}

func TestGetStringListAttr_NonListAttribute(t *testing.T) {
	// When attribute exists but is wrong type (string instead of list)
	image := map[string]events.DynamoDBAttributeValue{
		"_unique_pks": events.NewStringAttribute("not-a-list"),
	}

	result := getStringListAttr(image, "_unique_pks")
	if result != nil {
		t.Errorf("expected nil for non-list attribute, got %v", result)
	}
}

func TestGetStringListAttr_ListWithMixedTypes(t *testing.T) {
	// List with mixed types - only strings should be extracted
	image := map[string]events.DynamoDBAttributeValue{
		"mixed": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("str1"),
			events.NewNumberAttribute("123"),
			events.NewStringAttribute("str2"),
		}),
	}

	result := getStringListAttr(image, "mixed")
	// Should only contain strings
	if len(result) != 2 {
		t.Errorf("expected 2 string items, got %d: %v", len(result), result)
	}
	if result[0] != "str1" || result[1] != "str2" {
		t.Errorf("expected [str1, str2], got %v", result)
	}
}

func TestGetStringListAttr_SingleElement(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"_unique_pks": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("only-one"),
		}),
	}

	result := getStringListAttr(image, "_unique_pks")
	if len(result) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result))
	}
	if result[0] != "only-one" {
		t.Errorf("expected 'only-one', got %q", result[0])
	}
}

func TestGetStringListAttr_UnicodeValues(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"names": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("日本語"),
			events.NewStringAttribute("한국어"),
			events.NewStringAttribute("中文"),
		}),
	}

	result := getStringListAttr(image, "names")
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0] != "日本語" || result[1] != "한국어" || result[2] != "中文" {
		t.Errorf("expected unicode values, got %v", result)
	}
}

func TestGetStringListAttr_EmptyStringsInList(t *testing.T) {
	image := map[string]events.DynamoDBAttributeValue{
		"items": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute(""),
			events.NewStringAttribute("value"),
			events.NewStringAttribute(""),
		}),
	}

	result := getStringListAttr(image, "items")
	if len(result) != 3 {
		t.Fatalf("expected 3 items including empty strings, got %d", len(result))
	}
	if result[0] != "" || result[1] != "value" || result[2] != "" {
		t.Errorf("expected ['', 'value', ''], got %v", result)
	}
}

// --- ProcessRecord Logic Tests ---

func TestProcessRecord_SkipsNonModifyEvents(t *testing.T) {
	tests := []struct {
		name      string
		eventName string
	}{
		{"INSERT", "INSERT"},
		{"REMOVE", "REMOVE"},
		{"Unknown", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(nil, nil)
			record := &events.DynamoDBEventRecord{
				EventName: tt.eventName,
			}

			// Should not error - just skip non-MODIFY events
			err := h.processRecord(context.Background(), record)
			if err != nil {
				t.Errorf("expected no error for %s event, got %v", tt.eventName, err)
			}
		})
	}
}

func TestProcessRecord_SkipsModifyWithoutNewTTL(t *testing.T) {
	h := NewHandler(nil, nil)

	// MODIFY event where TTL is not newly set (was already present)
	record := &events.DynamoDBEventRecord{
		EventName: "MODIFY",
		Change: events.DynamoDBStreamRecord{
			OldImage: map[string]events.DynamoDBAttributeValue{
				"id":  events.NewStringAttribute("test"),
				"ttl": events.NewNumberAttribute("1000"), // TTL already existed
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"id":  events.NewStringAttribute("test"),
				"ttl": events.NewNumberAttribute("2000"), // TTL changed
			},
		},
	}

	err := h.processRecord(context.Background(), record)
	if err != nil {
		t.Errorf("expected no error when TTL already existed, got %v", err)
	}
}

func TestProcessRecord_SkipsModifyWithZeroNewTTL(t *testing.T) {
	h := NewHandler(nil, nil)

	// MODIFY event where new TTL is 0 (effectively no TTL)
	record := &events.DynamoDBEventRecord{
		EventName: "MODIFY",
		Change: events.DynamoDBStreamRecord{
			OldImage: map[string]events.DynamoDBAttributeValue{
				"id": events.NewStringAttribute("test"),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"id":  events.NewStringAttribute("test"),
				"ttl": events.NewNumberAttribute("0"), // TTL of 0 should be skipped
			},
		},
	}

	err := h.processRecord(context.Background(), record)
	if err != nil {
		t.Errorf("expected no error when newTTL is 0, got %v", err)
	}
}

// --- Benchmark Tests ---

func BenchmarkGetStringAttr(b *testing.B) {
	image := map[string]events.DynamoDBAttributeValue{
		"entity_ref": events.NewStringAttribute("parent#12345678-1234-1234-1234-123456789012"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getStringAttr(image, "entity_ref")
	}
}

func BenchmarkGetNumberAttr(b *testing.B) {
	image := map[string]events.DynamoDBAttributeValue{
		"ttl": events.NewNumberAttribute("1704067200"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getNumberAttr(image, "ttl")
	}
}

func BenchmarkGetStringListAttr(b *testing.B) {
	image := map[string]events.DynamoDBAttributeValue{
		"_unique_pks": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("pk1"),
			events.NewStringAttribute("pk2"),
			events.NewStringAttribute("pk3"),
		}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getStringListAttr(image, "_unique_pks")
	}
}
