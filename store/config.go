package store

// Config holds configuration for the Store.
type Config struct {
	// RelationshipTable is the name of the relationship table.
	// Default: "trellis_relationships"
	RelationshipTable string

	// UniqueTable is the name of the unique constraints table.
	// Default: "trellis_unique_constraints"
	UniqueTable string

	// NumShards is the number of shards for the relationship table.
	// Higher values increase write throughput but require more parallel queries.
	// Default: 1 (no sharding, single query)
	// Max: 256
	//
	// Per-shard limits:
	//   - Writes: 1,000/sec
	//   - Reads: 3,000/sec
	//
	// Examples:
	//   - NumShards=1:   1,000 writes/sec,   3,000 reads/sec per parent
	//   - NumShards=16:  16,000 writes/sec,  48,000 reads/sec per parent
	//   - NumShards=256: 256,000 writes/sec, 768,000 reads/sec per parent
	NumShards int
}

// DefaultConfig returns sensible defaults for small datasets.
func DefaultConfig() Config {
	return Config{
		RelationshipTable: "trellis_relationships",
		UniqueTable:       "trellis_unique_constraints",
		NumShards:         1,
	}
}

// validate ensures config values are within acceptable bounds.
func (c *Config) validate() {
	if c.RelationshipTable == "" {
		c.RelationshipTable = "trellis_relationships"
	}
	if c.UniqueTable == "" {
		c.UniqueTable = "trellis_unique_constraints"
	}
	if c.NumShards < 1 {
		c.NumShards = 1
	}
	if c.NumShards > 256 {
		c.NumShards = 256
	}
}
