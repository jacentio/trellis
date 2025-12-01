// Package shard provides shard key generation for distributed DynamoDB tables.
package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
)

// RelationshipPK computes the sharded partition key for a relationship record.
// With numShards=1, all records go to shard "00".
// With numShards>1, records are distributed across shards based on childRef hash.
func RelationshipPK(parentRef, childRef string, numShards int) string {
	if numShards <= 1 {
		return fmt.Sprintf("%s#00", parentRef)
	}
	h := fnv.New32a()
	h.Write([]byte(childRef))
	shard := h.Sum32() % uint32(numShards)
	return fmt.Sprintf("%s#%02x", parentRef, shard)
}

// UniqueConstraintPK computes a hash-distributed partition key for a unique constraint.
// This ensures each constraint goes to a different partition, eliminating hot partition risk.
func UniqueConstraintPK(parentRef, entityType, field, value string) string {
	data := fmt.Sprintf("%s#%s#%s#%s", parentRef, entityType, field, value)
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:16]) // 128-bit hash as hex
}
