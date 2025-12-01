// Package stream provides DynamoDB Streams handlers for cascade operations.
package stream

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jacentio/trellis/store"
)

// Handler processes DynamoDB stream events for cascade deletes.
type Handler struct {
	store  *store.Store
	logger *slog.Logger
}

// NewHandler creates a new stream handler.
func NewHandler(s *store.Store, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		store:  s,
		logger: logger,
	}
}

// HandleCascadeDelete processes DynamoDB stream events to propagate TTL to children.
// This function is designed to be used as an AWS Lambda handler.
func (h *Handler) HandleCascadeDelete(ctx context.Context, event events.DynamoDBEvent) error {
	for _, record := range event.Records {
		if err := h.processRecord(ctx, record); err != nil {
			h.logger.Error("failed to process record",
				"eventID", record.EventID,
				"error", err,
			)
			return err // Will retry, eventually DLQ
		}
	}
	return nil
}

// processRecord processes a single DynamoDB stream record.
func (h *Handler) processRecord(ctx context.Context, record events.DynamoDBEventRecord) error {
	// Only process MODIFY events where TTL was added
	if record.EventName != "MODIFY" {
		return nil
	}

	oldTTL := getNumberAttr(record.Change.OldImage, "ttl")
	newTTL := getNumberAttr(record.Change.NewImage, "ttl")

	// Only process when TTL is newly set (was absent/0, now present)
	if oldTTL != 0 || newTTL == 0 {
		return nil
	}

	entityRef := getStringAttr(record.Change.NewImage, "entity_ref")
	parentRef := getStringAttr(record.Change.NewImage, "parent_ref")
	uniquePKs := getStringListAttr(record.Change.NewImage, "_unique_pks")

	h.logger.Info("processing cascade delete",
		"entityRef", entityRef,
		"parentRef", parentRef,
		"ttl", newTTL,
	)

	// 1. Query all children (including already-deleted ones - idempotent)
	children, err := h.store.QueryAllChildren(ctx, entityRef)
	if err != nil {
		return fmt.Errorf("query children: %w", err)
	}

	h.logger.Info("found children to cascade",
		"entityRef", entityRef,
		"childCount", len(children),
	)

	// 2. Set same TTL on all children (triggers their cascade via stream)
	for _, child := range children {
		if err := h.store.SetTTLByKey(ctx, child.TableName, child.Key, newTTL); err != nil {
			h.logger.Warn("failed to set TTL on child",
				"child", child.Ref,
				"error", err,
			)
			// Continue - idempotent, will retry
		}
	}

	// 3. Set TTL on this entity's relationship record (as a child)
	//    Uses parent_ref from stream record - no lookup needed!
	if parentRef != "" {
		if err := h.store.SetRelationshipTTL(ctx, entityRef, parentRef, newTTL); err != nil {
			h.logger.Warn("failed to set relationship TTL",
				"entity", entityRef,
				"parent", parentRef,
				"error", err,
			)
		}
	}

	// 4. Set TTL on unique constraint records
	for _, constraintPK := range uniquePKs {
		if err := h.store.SetUniqueConstraintTTL(ctx, constraintPK, newTTL); err != nil {
			h.logger.Warn("failed to set unique constraint TTL",
				"pk", constraintPK,
				"error", err,
			)
		}
	}

	h.logger.Info("cascade delete completed",
		"entityRef", entityRef,
		"childrenProcessed", len(children),
		"uniqueConstraints", len(uniquePKs),
	)

	return nil
}

// getStringAttr extracts a string attribute from a DynamoDB stream image.
func getStringAttr(image map[string]events.DynamoDBAttributeValue, key string) string {
	if v, ok := image[key]; ok {
		return v.String()
	}
	return ""
}

// getNumberAttr extracts a number attribute from a DynamoDB stream image.
func getNumberAttr(image map[string]events.DynamoDBAttributeValue, key string) int64 {
	if v, ok := image[key]; ok {
		if v.DataType() == events.DataTypeNumber {
			n, _ := strconv.ParseInt(v.Number(), 10, 64)
			return n
		}
	}
	return 0
}

// getStringListAttr extracts a string list attribute from a DynamoDB stream image.
func getStringListAttr(image map[string]events.DynamoDBAttributeValue, key string) []string {
	if v, ok := image[key]; ok {
		if v.DataType() == events.DataTypeList {
			var result []string
			for _, item := range v.List() {
				if item.DataType() == events.DataTypeString {
					result = append(result, item.String())
				}
			}
			return result
		}
	}
	return nil
}

// ConvertStreamKey converts a DynamoDB stream key to a store.PK.
// Use this when you need to convert keys from stream records to store operations.
func ConvertStreamKey(streamKey map[string]events.DynamoDBAttributeValue) store.PK {
	result := make(store.PK)
	for k, v := range streamKey {
		switch v.DataType() {
		case events.DataTypeString:
			result[k] = &types.AttributeValueMemberS{Value: v.String()}
		case events.DataTypeNumber:
			result[k] = &types.AttributeValueMemberN{Value: v.Number()}
		case events.DataTypeBinary:
			result[k] = &types.AttributeValueMemberB{Value: v.Binary()}
		}
	}
	return result
}
