package stream_test

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jacentio/trellis/store"
	"github.com/jacentio/trellis/stream"
)

func TestNewHandler(t *testing.T) {
	// Test with nil store and logger (should not panic)
	h := stream.NewHandler(nil, nil)
	if h == nil {
		t.Fatal("expected non-nil Handler")
	}
}

func TestConvertStreamKey(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"id": events.NewStringAttribute("test-id"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if pk == nil {
		t.Fatal("expected non-nil PK")
	}

	if v, ok := pk["id"].(*types.AttributeValueMemberS); !ok || v.Value != "test-id" {
		t.Error("expected id to be 'test-id'")
	}
}

func TestConvertStreamKey_Number(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"version": events.NewNumberAttribute("42"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["version"].(*types.AttributeValueMemberN); !ok || v.Value != "42" {
		t.Error("expected version to be '42'")
	}
}

func TestConvertStreamKey_CompositeKey(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"pk": events.NewStringAttribute("parent#123"),
		"sk": events.NewStringAttribute("child#456"),
	}

	pk := stream.ConvertStreamKey(streamKey)

	if v, ok := pk["pk"].(*types.AttributeValueMemberS); !ok || v.Value != "parent#123" {
		t.Error("expected pk to be 'parent#123'")
	}
	if v, ok := pk["sk"].(*types.AttributeValueMemberS); !ok || v.Value != "child#456" {
		t.Error("expected sk to be 'child#456'")
	}
}

// Ensure store.PK is compatible
var _ store.PK = stream.ConvertStreamKey(nil)

// --- ConvertStreamKey Edge Cases ---

func TestConvertStreamKey_Empty(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{}

	pk := stream.ConvertStreamKey(streamKey)
	if pk == nil {
		t.Fatal("expected non-nil PK for empty input")
	}
	if len(pk) != 0 {
		t.Errorf("expected empty PK, got %d keys", len(pk))
	}
}

func TestConvertStreamKey_Nil(t *testing.T) {
	pk := stream.ConvertStreamKey(nil)
	if pk == nil {
		t.Fatal("expected non-nil PK for nil input")
	}
	if len(pk) != 0 {
		t.Errorf("expected empty PK, got %d keys", len(pk))
	}
}

func TestConvertStreamKey_Binary(t *testing.T) {
	binaryData := []byte{0x01, 0x02, 0x03, 0x04}
	streamKey := map[string]events.DynamoDBAttributeValue{
		"data": events.NewBinaryAttribute(binaryData),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["data"].(*types.AttributeValueMemberB); !ok {
		t.Error("expected binary attribute")
	} else if len(v.Value) != len(binaryData) {
		t.Errorf("expected binary length %d, got %d", len(binaryData), len(v.Value))
	}
}

func TestConvertStreamKey_MixedTypes(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"id":      events.NewStringAttribute("test-id"),
		"version": events.NewNumberAttribute("42"),
		"data":    events.NewBinaryAttribute([]byte{0x01}),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if len(pk) != 3 {
		t.Errorf("expected 3 keys, got %d", len(pk))
	}

	if v, ok := pk["id"].(*types.AttributeValueMemberS); !ok || v.Value != "test-id" {
		t.Error("expected string id")
	}
	if v, ok := pk["version"].(*types.AttributeValueMemberN); !ok || v.Value != "42" {
		t.Error("expected number version")
	}
	if _, ok := pk["data"].(*types.AttributeValueMemberB); !ok {
		t.Error("expected binary data")
	}
}

func TestConvertStreamKey_EmptyString(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"id": events.NewStringAttribute(""),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["id"].(*types.AttributeValueMemberS); !ok || v.Value != "" {
		t.Error("expected empty string id")
	}
}

func TestConvertStreamKey_ZeroNumber(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"count": events.NewNumberAttribute("0"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["count"].(*types.AttributeValueMemberN); !ok || v.Value != "0" {
		t.Error("expected zero count")
	}
}

func TestConvertStreamKey_NegativeNumber(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"offset": events.NewNumberAttribute("-100"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["offset"].(*types.AttributeValueMemberN); !ok || v.Value != "-100" {
		t.Error("expected negative offset")
	}
}

func TestConvertStreamKey_LargeNumber(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"big": events.NewNumberAttribute("9999999999999999999"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["big"].(*types.AttributeValueMemberN); !ok || v.Value != "9999999999999999999" {
		t.Error("expected large number")
	}
}

func TestConvertStreamKey_DecimalNumber(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"price": events.NewNumberAttribute("19.99"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["price"].(*types.AttributeValueMemberN); !ok || v.Value != "19.99" {
		t.Error("expected decimal price")
	}
}

func TestConvertStreamKey_SpecialCharactersInKey(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"special#key": events.NewStringAttribute("value"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["special#key"].(*types.AttributeValueMemberS); !ok || v.Value != "value" {
		t.Error("expected value for special#key")
	}
}

func TestConvertStreamKey_UnicodeString(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"name": events.NewStringAttribute("日本語テスト"),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["name"].(*types.AttributeValueMemberS); !ok || v.Value != "日本語テスト" {
		t.Error("expected unicode string")
	}
}

func TestConvertStreamKey_EmptyBinary(t *testing.T) {
	streamKey := map[string]events.DynamoDBAttributeValue{
		"data": events.NewBinaryAttribute([]byte{}),
	}

	pk := stream.ConvertStreamKey(streamKey)
	if v, ok := pk["data"].(*types.AttributeValueMemberB); !ok {
		t.Error("expected binary attribute")
	} else if len(v.Value) != 0 {
		t.Errorf("expected empty binary, got %d bytes", len(v.Value))
	}
}

// --- NewHandler Tests ---

func TestNewHandler_WithNilStore(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	if h == nil {
		t.Fatal("expected non-nil Handler with nil store")
	}
}

func TestNewHandler_WithStore(t *testing.T) {
	s := store.New(nil, store.DefaultConfig())
	h := stream.NewHandler(s, nil)
	if h == nil {
		t.Fatal("expected non-nil Handler with store")
	}
}

// --- Handler HandleCascadeDelete Tests ---

func TestHandler_HandleCascadeDelete_EmptyEvent(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{},
	}

	// Empty event should not error
	err := h.HandleCascadeDelete(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for empty event, got %v", err)
	}
}

func TestHandler_HandleCascadeDelete_InsertEvent(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"id": events.NewStringAttribute("test"),
					},
				},
			},
		},
	}

	// INSERT events should be skipped (no error)
	err := h.HandleCascadeDelete(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for INSERT event, got %v", err)
	}
}

func TestHandler_HandleCascadeDelete_RemoveEvent(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "REMOVE",
				Change: events.DynamoDBStreamRecord{
					OldImage: map[string]events.DynamoDBAttributeValue{
						"id": events.NewStringAttribute("test"),
					},
				},
			},
		},
	}

	// REMOVE events should be skipped (no error)
	err := h.HandleCascadeDelete(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for REMOVE event, got %v", err)
	}
}

func TestHandler_HandleCascadeDelete_ModifyWithoutTTL(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					OldImage: map[string]events.DynamoDBAttributeValue{
						"id":   events.NewStringAttribute("test"),
						"name": events.NewStringAttribute("old"),
					},
					NewImage: map[string]events.DynamoDBAttributeValue{
						"id":   events.NewStringAttribute("test"),
						"name": events.NewStringAttribute("new"),
					},
				},
			},
		},
	}

	// MODIFY without TTL change should be skipped
	err := h.HandleCascadeDelete(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for MODIFY without TTL, got %v", err)
	}
}

func TestHandler_HandleCascadeDelete_ModifyWithExistingTTL(t *testing.T) {
	h := stream.NewHandler(nil, nil)
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					OldImage: map[string]events.DynamoDBAttributeValue{
						"id":  events.NewStringAttribute("test"),
						"ttl": events.NewNumberAttribute("1000"),
					},
					NewImage: map[string]events.DynamoDBAttributeValue{
						"id":  events.NewStringAttribute("test"),
						"ttl": events.NewNumberAttribute("2000"),
					},
				},
			},
		},
	}

	// MODIFY where TTL already existed should be skipped
	err := h.HandleCascadeDelete(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for MODIFY with existing TTL, got %v", err)
	}
}
