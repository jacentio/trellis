package store

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// IsDeleted checks if an item has an expired TTL (is marked for deletion).
func IsDeleted(item map[string]types.AttributeValue) bool {
	ttlAttr, exists := item["ttl"]
	if !exists {
		return false // No TTL = active
	}
	ttlNum, ok := ttlAttr.(*types.AttributeValueMemberN)
	if !ok {
		return false
	}
	ttl, err := strconv.ParseInt(ttlNum.Value, 10, 64)
	if err != nil {
		return false
	}
	return ttl <= time.Now().Unix()
}

// TTLFilterExpr returns the filter expression to exclude deleted items.
// Use this when building custom queries that need TTL filtering.
func TTLFilterExpr() string {
	return "attribute_not_exists(#ttl) OR #ttl > :now"
}

// TTLFilterNames returns expression attribute names for TTL filter.
// Use with TTLFilterExpr() when building custom queries.
func TTLFilterNames() map[string]string {
	return map[string]string{"#ttl": "ttl"}
}

// TTLFilterValues returns expression attribute values for TTL filter.
// Use with TTLFilterExpr() when building custom queries.
func TTLFilterValues() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		":now": &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Unix(), 10),
		},
	}
}

// ParentExistsCondition returns the condition expression for parent validation.
// Ensures parent exists AND is not deleted (no TTL or TTL in future).
func ParentExistsCondition() string {
	return "attribute_exists(id) AND (attribute_not_exists(#ttl) OR #ttl > :now)"
}

// mergeExprNames merges multiple expression attribute name maps.
func mergeExprNames(maps ...map[string]string) map[string]string {
	result := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// mergeExprValues merges multiple expression attribute value maps.
func mergeExprValues(maps ...map[string]types.AttributeValue) map[string]types.AttributeValue {
	result := make(map[string]types.AttributeValue)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}
