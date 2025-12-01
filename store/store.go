package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jacentio/trellis/internal/shard"
)

// Store provides DynamoDB operations with hierarchical entity support.
type Store struct {
	client   *dynamodb.Client
	config   Config
	registry *Registry
}

// New creates a new Store instance.
func New(client *dynamodb.Client, config Config) *Store {
	config.validate()
	return &Store{
		client: client,
		config: config,
	}
}

// NewWithRegistry creates a new Store instance with a relationship registry.
func NewWithRegistry(client *dynamodb.Client, config Config, registry *Registry) *Store {
	config.validate()
	return &Store{
		client:   client,
		config:   config,
		registry: registry,
	}
}

// SetRegistry sets the relationship registry for cascade operations.
func (s *Store) SetRegistry(registry *Registry) {
	s.registry = registry
}

// Registry returns the relationship registry, or nil if not set.
func (s *Store) Registry() *Registry {
	return s.registry
}

// relationshipPK computes the sharded partition key for a relationship record.
func (s *Store) relationshipPK(parentRef, childRef string) string {
	return shard.RelationshipPK(parentRef, childRef, s.config.NumShards)
}

// Create creates a new entity with parent validation and unique constraints.
func (s *Store) Create(ctx context.Context, entity Entity, item map[string]types.AttributeValue) error {
	items := []types.TransactWriteItem{}
	now := time.Now()
	nowUnix := now.Unix()
	nowISO := now.UTC().Format(time.RFC3339)

	// Track item indices for error mapping
	parentCheckIndex := -1
	entityPutIndex := -1

	// 1. Add parent condition check if entity has a parent
	if checker, ok := entity.(ParentChecker); ok {
		if check := checker.ParentCheck(); check != nil {
			parentCheckIndex = len(items)
			// Use custom condition expression if provided, otherwise use default
			condExpr := check.ConditionExpr
			if condExpr == "" {
				condExpr = ParentExistsCondition()
			}
			items = append(items, types.TransactWriteItem{
				ConditionCheck: &types.ConditionCheck{
					TableName:           aws.String(check.TableName),
					Key:                 check.Key,
					ConditionExpression: aws.String(condExpr),
					ExpressionAttributeNames: map[string]string{
						"#ttl": "ttl",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":now": &types.AttributeValueMemberN{
							Value: strconv.FormatInt(nowUnix, 10),
						},
					},
				},
			})
		}
	}

	// 2. Set ORM-managed fields
	item["entity_ref"] = &types.AttributeValueMemberS{Value: entity.EntityRef()}
	item["version"] = &types.AttributeValueMemberN{Value: "1"}
	item["created_at"] = &types.AttributeValueMemberS{Value: nowISO}
	item["updated_at"] = &types.AttributeValueMemberS{Value: nowISO}

	// Set parent_ref if entity has parent
	var parentRef string
	if checker, ok := entity.(ParentChecker); ok {
		parentRef = checker.ParentRef()
		if parentRef != "" {
			item["parent_ref"] = &types.AttributeValueMemberS{Value: parentRef}
		}
	}

	// 3. Handle unique constraints
	var uniquePKs []string
	if uf, ok := entity.(UniqueFielder); ok && parentRef != "" {
		entityType := entity.EntityType()
		for field, value := range uf.UniqueFields() {
			constraintPK := shard.UniqueConstraintPK(parentRef, entityType, field, value)
			uniquePKs = append(uniquePKs, constraintPK)

			items = append(items, types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String(s.config.UniqueTable),
					Item: map[string]types.AttributeValue{
						"pk":          &types.AttributeValueMemberS{Value: constraintPK},
						"sk":          &types.AttributeValueMemberS{Value: "CONSTRAINT"},
						"parent_ref":  &types.AttributeValueMemberS{Value: parentRef},
						"entity_type": &types.AttributeValueMemberS{Value: entityType},
						"field_name":  &types.AttributeValueMemberS{Value: field},
						"field_value": &types.AttributeValueMemberS{Value: value},
						"entity_ref":  &types.AttributeValueMemberS{Value: entity.EntityRef()},
					},
					ConditionExpression: aws.String("attribute_not_exists(pk)"),
				},
			})
		}
	}

	// Store unique PKs on entity for cascade delete cleanup
	if len(uniquePKs) > 0 {
		uniquePKsAttr, _ := attributevalue.MarshalList(uniquePKs)
		item["_unique_pks"] = &types.AttributeValueMemberL{Value: uniquePKsAttr}
	}

	// 4. Add the entity put
	entityPutIndex = len(items)
	items = append(items, types.TransactWriteItem{
		Put: &types.Put{
			TableName:           aws.String(entity.TableName()),
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(id)"),
		},
	})

	// 5. Add relationship record if entity has a parent
	if parentRef != "" {
		childRef := entity.EntityRef()
		shardPK := s.relationshipPK(parentRef, childRef)

		keyAttr, err := attributevalue.MarshalMap(entity.GetKey())
		if err != nil {
			return fmt.Errorf("marshal key: %w", err)
		}

		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.config.RelationshipTable),
				Item: map[string]types.AttributeValue{
					"pk":          &types.AttributeValueMemberS{Value: shardPK},
					"child_ref":   &types.AttributeValueMemberS{Value: childRef},
					"parent_ref":  &types.AttributeValueMemberS{Value: parentRef},
					"child_table": &types.AttributeValueMemberS{Value: entity.TableName()},
					"child_key":   &types.AttributeValueMemberM{Value: keyAttr},
				},
			},
		})
	}

	// 6. Execute transaction
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})

	return s.mapCreateTransactionError(err, parentCheckIndex, entityPutIndex)
}

// Get retrieves an entity by key, returning ErrNotFound if deleted or missing.
func (s *Store) Get(ctx context.Context, table string, key PK) (*Item, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key:       key,
	})
	if err != nil {
		return nil, err
	}
	if result.Item == nil {
		return nil, ErrNotFound
	}

	// Check if entity is deleted (has expired TTL)
	if IsDeleted(result.Item) {
		return nil, ErrNotFound
	}

	return s.unmarshalItem(result.Item), nil
}

// Query queries entities with automatic TTL filtering.
func (s *Store) Query(ctx context.Context, input QueryInput) ([]*Item, error) {
	// Merge TTL filter with any existing filter
	filterExpr := TTLFilterExpr()
	if input.FilterExpression != "" {
		filterExpr = fmt.Sprintf("(%s) AND (%s)", input.FilterExpression, filterExpr)
	}

	// Merge expression attribute names
	exprNames := map[string]string{"#ttl": "ttl"}
	for k, v := range input.ExpressionAttributeNames {
		exprNames[k] = v
	}

	// Merge expression attribute values
	exprValues := map[string]types.AttributeValue{
		":now": &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Unix(), 10),
		},
	}
	for k, v := range input.ExpressionAttributeValues {
		exprValues[k] = v
	}

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(input.TableName),
		KeyConditionExpression:    aws.String(input.KeyConditionExpression),
		FilterExpression:          aws.String(filterExpr),
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprValues,
	}

	if input.IndexName != "" {
		queryInput.IndexName = aws.String(input.IndexName)
	}
	if input.Limit > 0 {
		queryInput.Limit = aws.Int32(input.Limit)
	}
	if input.ScanIndexForward != nil {
		queryInput.ScanIndexForward = input.ScanIndexForward
	}

	// Paginate through all results
	var items []*Item
	paginator := dynamodb.NewQueryPaginator(s.client, queryInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, raw := range page.Items {
			items = append(items, s.unmarshalItem(raw))
		}
	}

	return items, nil
}

// Update updates an entity with optimistic locking.
// If the entity implements UniqueFielder and unique fields change,
// old constraints are deleted and new ones created transactionally.
func (s *Store) Update(ctx context.Context, entity Entity, item map[string]types.AttributeValue, expectedVersion int64) error {
	// Check if entity has unique fields that might need updating
	uf, hasUniqueFields := entity.(UniqueFielder)
	pc, hasParent := entity.(ParentChecker)

	// If entity has unique fields and a parent, check if any unique fields changed
	if hasUniqueFields && hasParent && pc.ParentRef() != "" {
		return s.updateWithUniqueConstraints(ctx, entity, item, expectedVersion, uf, pc)
	}

	// Fast path: simple update without unique constraint handling
	return s.updateSimple(ctx, entity, item, expectedVersion)
}

// updateSimple performs a basic update without unique constraint handling.
func (s *Store) updateSimple(ctx context.Context, entity Entity, item map[string]types.AttributeValue, expectedVersion int64) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// Build SET expression from item attributes
	var setClauses []string
	exprNames := map[string]string{
		"#updated_at": "updated_at",
		"#version":    "version",
		"#ttl":        "ttl",
	}
	exprValues := map[string]types.AttributeValue{
		":updated_at":       &types.AttributeValueMemberS{Value: now},
		":one":              &types.AttributeValueMemberN{Value: "1"},
		":expected_version": &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion, 10)},
	}

	// Add user-provided attributes
	i := 0
	for k, v := range item {
		// Skip managed fields
		if k == "id" || k == "entity_ref" || k == "parent_ref" || k == "version" ||
			k == "created_at" || k == "updated_at" || k == "ttl" || k == "_unique_pks" {
			continue
		}
		nameKey := fmt.Sprintf("#attr%d", i)
		valueKey := fmt.Sprintf(":val%d", i)
		exprNames[nameKey] = k
		exprValues[valueKey] = v
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", nameKey, valueKey))
		i++
	}

	// Add managed field updates
	setClauses = append(setClauses, "#updated_at = :updated_at", "#version = #version + :one")

	updateExpr := "SET " + joinStrings(setClauses, ", ")

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(entity.TableName()),
		Key:                       entity.GetKey(),
		UpdateExpression:          aws.String(updateExpr),
		ConditionExpression:       aws.String("#version = :expected_version AND attribute_not_exists(#ttl)"),
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprValues,
	})

	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return ErrConcurrentModification
		}
		return err
	}
	return nil
}

// updateWithUniqueConstraints handles updates where unique fields may have changed.
func (s *Store) updateWithUniqueConstraints(ctx context.Context, entity Entity, item map[string]types.AttributeValue, expectedVersion int64, uf UniqueFielder, pc ParentChecker) error {
	// Fetch current entity to get old unique field values
	current, err := s.Get(ctx, entity.TableName(), entity.GetKey())
	if err != nil {
		return err
	}

	parentRef := pc.ParentRef()
	entityType := entity.EntityType()
	newUniques := uf.UniqueFields()

	// Extract old unique values from current item
	oldUniques := make(map[string]string)
	for field := range newUniques {
		if v, ok := current.Raw[field].(*types.AttributeValueMemberS); ok {
			oldUniques[field] = v.Value
		}
	}

	// Check if any unique fields changed
	var changedFields []string
	for field, newValue := range newUniques {
		if oldValue, ok := oldUniques[field]; !ok || oldValue != newValue {
			changedFields = append(changedFields, field)
		}
	}

	// If no unique fields changed, use simple update
	if len(changedFields) == 0 {
		return s.updateSimple(ctx, entity, item, expectedVersion)
	}

	// Build transaction for changed unique fields
	now := time.Now().UTC().Format(time.RFC3339)
	items := []types.TransactWriteItem{}

	// Compute all new unique PKs (including unchanged ones for _unique_pks update)
	var newUniquePKs []string
	for field, newValue := range newUniques {
		newPK := shard.UniqueConstraintPK(parentRef, entityType, field, newValue)
		newUniquePKs = append(newUniquePKs, newPK)
	}

	// For each changed field: delete old constraint, create new constraint
	for _, field := range changedFields {
		oldValue := oldUniques[field]
		newValue := newUniques[field]

		// Delete old uniqueness record
		if oldValue != "" {
			oldPK := shard.UniqueConstraintPK(parentRef, entityType, field, oldValue)
			items = append(items, types.TransactWriteItem{
				Delete: &types.Delete{
					TableName: aws.String(s.config.UniqueTable),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: oldPK},
						"sk": &types.AttributeValueMemberS{Value: "CONSTRAINT"},
					},
				},
			})
		}

		// Create new uniqueness record
		newPK := shard.UniqueConstraintPK(parentRef, entityType, field, newValue)
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.config.UniqueTable),
				Item: map[string]types.AttributeValue{
					"pk":          &types.AttributeValueMemberS{Value: newPK},
					"sk":          &types.AttributeValueMemberS{Value: "CONSTRAINT"},
					"parent_ref":  &types.AttributeValueMemberS{Value: parentRef},
					"entity_type": &types.AttributeValueMemberS{Value: entityType},
					"field_name":  &types.AttributeValueMemberS{Value: field},
					"field_value": &types.AttributeValueMemberS{Value: newValue},
					"entity_ref":  &types.AttributeValueMemberS{Value: entity.EntityRef()},
				},
				// Fails if another entity already has this unique value
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
			},
		})
	}

	// Build the entity update
	var setClauses []string
	exprNames := map[string]string{
		"#updated_at": "updated_at",
		"#version":    "version",
		"#ttl":        "ttl",
		"#unique_pks": "_unique_pks",
	}
	exprValues := map[string]types.AttributeValue{
		":updated_at":       &types.AttributeValueMemberS{Value: now},
		":one":              &types.AttributeValueMemberN{Value: "1"},
		":expected_version": &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion, 10)},
	}

	// Add user-provided attributes
	i := 0
	for k, v := range item {
		// Skip managed fields
		if k == "id" || k == "entity_ref" || k == "parent_ref" || k == "version" ||
			k == "created_at" || k == "updated_at" || k == "ttl" || k == "_unique_pks" {
			continue
		}
		nameKey := fmt.Sprintf("#attr%d", i)
		valueKey := fmt.Sprintf(":val%d", i)
		exprNames[nameKey] = k
		exprValues[valueKey] = v
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", nameKey, valueKey))
		i++
	}

	// Add managed field updates
	setClauses = append(setClauses, "#updated_at = :updated_at", "#version = #version + :one")

	// Update _unique_pks with new PKs
	uniquePKsAttr, _ := attributevalue.MarshalList(newUniquePKs)
	exprValues[":unique_pks"] = &types.AttributeValueMemberL{Value: uniquePKsAttr}
	setClauses = append(setClauses, "#unique_pks = :unique_pks")

	updateExpr := "SET " + joinStrings(setClauses, ", ")

	items = append(items, types.TransactWriteItem{
		Update: &types.Update{
			TableName:                 aws.String(entity.TableName()),
			Key:                       entity.GetKey(),
			UpdateExpression:          aws.String(updateExpr),
			ConditionExpression:       aws.String("#version = :expected_version AND attribute_not_exists(#ttl)"),
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: exprValues,
		},
	})

	// Execute transaction
	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})

	return s.mapUpdateTransactionError(err)
}

// DeleteOptions configures delete behavior.
type DeleteOptions struct {
	// Cascade enables cascading delete of children via TTL.
	Cascade bool

	// OrphanProtect fails the delete if active children exist.
	OrphanProtect bool
}

// Delete deletes an entity by setting its TTL.
func (s *Store) Delete(ctx context.Context, entity Entity, opts DeleteOptions) error {
	if opts.OrphanProtect && !opts.Cascade {
		hasChildren, err := s.HasActiveChildren(ctx, entity.EntityRef())
		if err != nil {
			return err
		}
		if hasChildren {
			return ErrHasChildren
		}
	}

	return s.SetTTL(ctx, entity)
}

// SetTTL marks an entity for deletion by setting its TTL to now.
// This also increments the version to fail concurrent updates.
func (s *Store) SetTTL(ctx context.Context, entity Entity) error {
	now := time.Now()

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(entity.TableName()),
		Key:                 entity.GetKey(),
		UpdateExpression:    aws.String("SET #ttl = :now, #version = #version + :one"),
		ConditionExpression: aws.String("attribute_not_exists(#ttl)"),
		ExpressionAttributeNames: map[string]string{
			"#ttl":     "ttl",
			"#version": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(now.Unix(), 10),
			},
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
	})

	// Ignore condition failure - already has TTL (already deleted)
	var condErr *types.ConditionalCheckFailedException
	if errors.As(err, &condErr) {
		return nil
	}
	return err
}

// HasActiveChildren checks if an entity has any active (non-deleted) children.
func (s *Store) HasActiveChildren(ctx context.Context, entityRef string) (bool, error) {
	now := time.Now().Unix()
	numShards := s.config.NumShards
	if numShards < 1 {
		numShards = 1
	}

	// Fast path for single shard (default)
	if numShards == 1 {
		return s.hasActiveChildrenSingleShard(ctx, entityRef, now)
	}

	// Multi-shard fan-out with early cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	found := make(chan bool, 1)
	errs := make(chan error, numShards)
	var wg sync.WaitGroup

	for shardNum := 0; shardNum < numShards; shardNum++ {
		wg.Add(1)
		go func(shardNum int) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
			}

			shardPK := fmt.Sprintf("%s#%02x", entityRef, shardNum)
			result, err := s.client.Query(ctx, &dynamodb.QueryInput{
				TableName:              aws.String(s.config.RelationshipTable),
				KeyConditionExpression: aws.String("pk = :pk"),
				FilterExpression:       aws.String(TTLFilterExpr()),
				ExpressionAttributeNames: map[string]string{
					"#ttl": "ttl",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: shardPK},
					":now": &types.AttributeValueMemberN{
						Value: strconv.FormatInt(now, 10),
					},
				},
				Limit: aws.Int32(1),
			})
			if err != nil {
				errs <- err
				return
			}
			if len(result.Items) > 0 {
				select {
				case found <- true:
					cancel()
				default:
				}
			}
		}(shardNum)
	}

	go func() {
		wg.Wait()
		close(found)
		close(errs)
	}()

	select {
	case <-found:
		return true, nil
	case err := <-errs:
		if err != nil {
			return false, err
		}
	}

	for err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return false, err
		}
	}

	return false, nil
}

func (s *Store) hasActiveChildrenSingleShard(ctx context.Context, entityRef string, now int64) (bool, error) {
	shardPK := fmt.Sprintf("%s#00", entityRef)
	result, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String(s.config.RelationshipTable),
		KeyConditionExpression:   aws.String("pk = :pk"),
		FilterExpression:         aws.String(TTLFilterExpr()),
		ExpressionAttributeNames: map[string]string{"#ttl": "ttl"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  &types.AttributeValueMemberS{Value: shardPK},
			":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	return len(result.Items) > 0, nil
}

// QueryAllChildren returns all children of an entity (including deleted ones).
// This is used by cascade delete to propagate TTL to all children.
func (s *Store) QueryAllChildren(ctx context.Context, parentRef string) ([]ChildRef, error) {
	numShards := s.config.NumShards
	if numShards < 1 {
		numShards = 1
	}

	// Fast path for single shard (default)
	if numShards == 1 {
		return s.queryChildrenSingleShard(ctx, parentRef)
	}

	// Multi-shard fan-out
	var mu sync.Mutex
	var allChildren []ChildRef
	var wg sync.WaitGroup
	errs := make(chan error, numShards)

	for shardNum := 0; shardNum < numShards; shardNum++ {
		wg.Add(1)
		go func(shardNum int) {
			defer wg.Done()

			shardPK := fmt.Sprintf("%s#%02x", parentRef, shardNum)
			var shardChildren []ChildRef

			paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
				TableName:              aws.String(s.config.RelationshipTable),
				KeyConditionExpression: aws.String("pk = :pk"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: shardPK},
				},
			})

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					errs <- fmt.Errorf("shard %02x: %w", shardNum, err)
					return
				}
				for _, item := range page.Items {
					shardChildren = append(shardChildren, s.unmarshalChildRef(item, shardPK))
				}
			}

			mu.Lock()
			allChildren = append(allChildren, shardChildren...)
			mu.Unlock()
		}(shardNum)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return allChildren, nil
}

func (s *Store) queryChildrenSingleShard(ctx context.Context, parentRef string) ([]ChildRef, error) {
	var children []ChildRef
	shardPK := fmt.Sprintf("%s#00", parentRef)

	paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
		TableName:              aws.String(s.config.RelationshipTable),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: shardPK},
		},
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			children = append(children, s.unmarshalChildRef(item, shardPK))
		}
	}

	return children, nil
}

// SetTTLByKey sets TTL on an entity by table and key.
// Used by cascade delete to propagate TTL to children.
func (s *Store) SetTTLByKey(ctx context.Context, table string, key PK, ttl int64) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(table),
		Key:                 key,
		UpdateExpression:    aws.String("SET #ttl = :ttl, #version = #version + :one"),
		ConditionExpression: aws.String("attribute_not_exists(#ttl)"),
		ExpressionAttributeNames: map[string]string{
			"#ttl":     "ttl",
			"#version": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ttl": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(ttl, 10),
			},
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
	})

	// Ignore condition failure - already has TTL
	var condErr *types.ConditionalCheckFailedException
	if errors.As(err, &condErr) {
		return nil
	}
	return err
}

// SetRelationshipTTL sets TTL on a relationship record.
func (s *Store) SetRelationshipTTL(ctx context.Context, childRef, parentRef string, ttl int64) error {
	shardPK := s.relationshipPK(parentRef, childRef)

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.config.RelationshipTable),
		Key: map[string]types.AttributeValue{
			"pk":        &types.AttributeValueMemberS{Value: shardPK},
			"child_ref": &types.AttributeValueMemberS{Value: childRef},
		},
		UpdateExpression:    aws.String("SET #ttl = :ttl"),
		ConditionExpression: aws.String("attribute_not_exists(#ttl)"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ttl": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(ttl, 10),
			},
		},
	})

	// Ignore condition failure - already has TTL
	var condErr *types.ConditionalCheckFailedException
	if errors.As(err, &condErr) {
		return nil
	}
	return err
}

// SetUniqueConstraintTTL sets TTL on a unique constraint record.
func (s *Store) SetUniqueConstraintTTL(ctx context.Context, pk string, ttl int64) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.config.UniqueTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: "CONSTRAINT"},
		},
		UpdateExpression:    aws.String("SET #ttl = :ttl"),
		ConditionExpression: aws.String("attribute_not_exists(#ttl)"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ttl": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(ttl, 10),
			},
		},
	})

	// Ignore condition failure - already has TTL
	var condErr *types.ConditionalCheckFailedException
	if errors.As(err, &condErr) {
		return nil
	}
	return err
}

// mapCreateTransactionError maps DynamoDB transaction errors for Create operations.
// parentCheckIndex is the index of the parent check item (-1 if none).
// entityPutIndex is the index of the entity put item.
func (s *Store) mapCreateTransactionError(err error, parentCheckIndex, entityPutIndex int) error {
	if err == nil {
		return nil
	}

	var txErr *types.TransactionCanceledException
	if errors.As(err, &txErr) {
		for i, reason := range txErr.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				if i == parentCheckIndex {
					return ErrParentNotFound
				}
				if i == entityPutIndex {
					return ErrAlreadyExists
				}
				// Must be a unique constraint
				return ErrDuplicateValue
			}
		}
	}

	return err
}

// mapUpdateTransactionError maps DynamoDB transaction errors for Update operations.
func (s *Store) mapUpdateTransactionError(err error) error {
	if err == nil {
		return nil
	}

	var txErr *types.TransactionCanceledException
	if errors.As(err, &txErr) {
		for _, reason := range txErr.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				// For updates, this is always a unique constraint violation
				return ErrDuplicateValue
			}
		}
	}

	return err
}

// unmarshalItem converts a DynamoDB item to an Item struct.
func (s *Store) unmarshalItem(raw map[string]types.AttributeValue) *Item {
	item := &Item{Raw: raw}

	if v, ok := raw["version"].(*types.AttributeValueMemberN); ok {
		item.Version, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := raw["created_at"].(*types.AttributeValueMemberS); ok {
		item.CreatedAt = v.Value
	}
	if v, ok := raw["updated_at"].(*types.AttributeValueMemberS); ok {
		item.UpdatedAt = v.Value
	}
	if v, ok := raw["entity_ref"].(*types.AttributeValueMemberS); ok {
		item.EntityRef = v.Value
	}
	if v, ok := raw["parent_ref"].(*types.AttributeValueMemberS); ok {
		item.ParentRef = v.Value
	}

	return item
}

// unmarshalChildRef converts a relationship item to a ChildRef.
func (s *Store) unmarshalChildRef(item map[string]types.AttributeValue, shardPK string) ChildRef {
	ref := ChildRef{ShardPK: shardPK}

	if v, ok := item["child_ref"].(*types.AttributeValueMemberS); ok {
		ref.Ref = v.Value
	}
	if v, ok := item["child_table"].(*types.AttributeValueMemberS); ok {
		ref.TableName = v.Value
	}
	if v, ok := item["child_key"].(*types.AttributeValueMemberM); ok {
		ref.Key = v.Value
	}

	return ref
}

// joinStrings joins strings with a separator (avoiding strings package import).
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for _, s := range strs[1:] {
		result += sep + s
	}
	return result
}
