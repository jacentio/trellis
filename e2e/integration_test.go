//go:build e2e

// Package e2e contains end-to-end integration tests using real DynamoDB tables.
// Run with: go test -tags=e2e -v ./e2e/...
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/jacentio/trellis/store"
)

// Test configuration
const (
	awsProfile = "jacent-alpha-cp"

	// Table names - unique per test run to avoid conflicts
	tablePrefix = "trellis-e2e-test"
)

var (
	testID             string
	organizationsTable string
	studiosTable       string
	titlesTable        string
	relationshipTable  string
	uniqueTable        string

	ddbClient *dynamodb.Client
	testStore *store.Store
)

// --- Test Entities ---

// Organization is a root entity
type Organization struct {
	ID   string
	Name string
}

func (o Organization) TableName() string  { return organizationsTable }
func (o Organization) EntityType() string { return "organization" }
func (o Organization) EntityRef() string  { return "organization#" + o.ID }
func (o Organization) GetKey() store.PK {
	return store.PK{"id": &types.AttributeValueMemberS{Value: o.ID}}
}

// Studio is a child of Organization with unique name constraint
type Studio struct {
	ID             string
	OrganizationID string
	Name           string
	Slug           string
}

func (s Studio) TableName() string  { return studiosTable }
func (s Studio) EntityType() string { return "studio" }
func (s Studio) EntityRef() string  { return "studio#" + s.ID }
func (s Studio) GetKey() store.PK {
	return store.PK{"id": &types.AttributeValueMemberS{Value: s.ID}}
}

func (s Studio) ParentRef() string { return "organization#" + s.OrganizationID }
func (s Studio) ParentCheck() *store.ConditionCheck {
	return &store.ConditionCheck{
		TableName: organizationsTable,
		Key:       store.PK{"id": &types.AttributeValueMemberS{Value: s.OrganizationID}},
	}
}

func (s Studio) UniqueFields() map[string]string {
	return map[string]string{
		"name": s.Name,
		"slug": s.Slug,
	}
}

// Title is a child of Studio
type Title struct {
	ID       string
	StudioID string
	Name     string
}

func (t Title) TableName() string  { return titlesTable }
func (t Title) EntityType() string { return "title" }
func (t Title) EntityRef() string  { return "title#" + t.ID }
func (t Title) GetKey() store.PK {
	return store.PK{"id": &types.AttributeValueMemberS{Value: t.ID}}
}

func (t Title) ParentRef() string { return "studio#" + t.StudioID }
func (t Title) ParentCheck() *store.ConditionCheck {
	return &store.ConditionCheck{
		TableName: studiosTable,
		Key:       store.PK{"id": &types.AttributeValueMemberS{Value: t.StudioID}},
	}
}

// --- Test Setup & Teardown ---

func TestMain(m *testing.M) {
	// Generate unique test ID
	testID = uuid.New().String()[:8]
	organizationsTable = fmt.Sprintf("%s-%s-organizations", tablePrefix, testID)
	studiosTable = fmt.Sprintf("%s-%s-studios", tablePrefix, testID)
	titlesTable = fmt.Sprintf("%s-%s-titles", tablePrefix, testID)
	relationshipTable = fmt.Sprintf("%s-%s-relationships", tablePrefix, testID)
	uniqueTable = fmt.Sprintf("%s-%s-unique", tablePrefix, testID)

	fmt.Printf("Test ID: %s\n", testID)
	fmt.Printf("Tables:\n")
	fmt.Printf("  - Organizations: %s\n", organizationsTable)
	fmt.Printf("  - Studios: %s\n", studiosTable)
	fmt.Printf("  - Titles: %s\n", titlesTable)
	fmt.Printf("  - Relationships: %s\n", relationshipTable)
	fmt.Printf("  - Unique: %s\n", uniqueTable)

	// Initialize AWS client (uses region from profile config)
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(awsProfile),
	)
	if err != nil {
		fmt.Printf("Failed to load AWS config: %v\n", err)
		os.Exit(1)
	}

	ddbClient = dynamodb.NewFromConfig(cfg)

	// Create tables
	if err := createTables(ctx); err != nil {
		fmt.Printf("Failed to create tables: %v\n", err)
		os.Exit(1)
	}

	// Initialize store
	testStore = store.New(ddbClient, store.Config{
		RelationshipTable: relationshipTable,
		UniqueTable:       uniqueTable,
		NumShards:         1,
	})

	// Run tests
	code := m.Run()

	// Cleanup tables
	if err := deleteTables(ctx); err != nil {
		fmt.Printf("Failed to delete tables: %v\n", err)
	}

	os.Exit(code)
}

func createTables(ctx context.Context) error {
	fmt.Println("Creating test tables...")

	// Entity tables (organizations, studios, titles)
	entityTables := []string{organizationsTable, studiosTable, titlesTable}
	for _, tableName := range entityTables {
		_, err := ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		if err != nil {
			return fmt.Errorf("create table %s: %w", tableName, err)
		}
	}

	// Relationship table (pk, child_ref)
	_, err := ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(relationshipTable),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("child_ref"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("child_ref"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create relationship table: %w", err)
	}

	// Unique constraints table (pk, sk)
	_, err = ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(uniqueTable),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create unique table: %w", err)
	}

	// Wait for all tables to be active
	allTables := []string{organizationsTable, studiosTable, titlesTable, relationshipTable, uniqueTable}
	for _, tableName := range allTables {
		waiter := dynamodb.NewTableExistsWaiter(ddbClient)
		if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		}, 2*time.Minute); err != nil {
			return fmt.Errorf("wait for table %s: %w", tableName, err)
		}
	}

	fmt.Println("All tables created and active")
	return nil
}

func deleteTables(ctx context.Context) error {
	fmt.Println("Deleting test tables...")

	tables := []string{organizationsTable, studiosTable, titlesTable, relationshipTable, uniqueTable}
	for _, tableName := range tables {
		_, err := ddbClient.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			fmt.Printf("Warning: failed to delete table %s: %v\n", tableName, err)
		}
	}

	fmt.Println("Tables deleted")
	return nil
}

// --- CRUD Tests ---

func TestCreate_RootEntity(t *testing.T) {
	ctx := context.Background()

	org := Organization{
		ID:   uuid.New().String(),
		Name: "Test Organization",
	}

	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}

	err := testStore.Create(ctx, org, item)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify it was created
	result, err := testStore.Get(ctx, org.TableName(), org.GetKey())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Version != 1 {
		t.Errorf("expected version 1, got %d", result.Version)
	}
	if result.EntityRef != org.EntityRef() {
		t.Errorf("expected entity_ref %q, got %q", org.EntityRef(), result.EntityRef)
	}

	// Verify created_at and updated_at are set
	if result.CreatedAt == "" {
		t.Error("expected created_at to be set")
	}
	if result.UpdatedAt == "" {
		t.Error("expected updated_at to be set")
	}
}

func TestCreate_ChildEntity_WithParentValidation(t *testing.T) {
	ctx := context.Background()

	// Create parent organization first
	org := Organization{
		ID:   uuid.New().String(),
		Name: "Parent Org",
	}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	// Create child studio
	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Test Studio",
		Slug:           "test-studio",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}

	err := testStore.Create(ctx, studio, studioItem)
	if err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	// Verify studio was created with parent_ref
	result, err := testStore.Get(ctx, studio.TableName(), studio.GetKey())
	if err != nil {
		t.Fatalf("Get studio failed: %v", err)
	}

	if result.ParentRef != studio.ParentRef() {
		t.Errorf("expected parent_ref %q, got %q", studio.ParentRef(), result.ParentRef)
	}
}

func TestCreate_ChildEntity_ParentNotFound(t *testing.T) {
	ctx := context.Background()

	// Try to create studio with non-existent parent
	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: "nonexistent-org-id",
		Name:           "Orphan Studio",
		Slug:           "orphan-studio",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}

	err := testStore.Create(ctx, studio, studioItem)
	if err != store.ErrParentNotFound {
		t.Errorf("expected ErrParentNotFound, got %v", err)
	}
}

func TestCreate_DuplicateEntity(t *testing.T) {
	ctx := context.Background()

	org := Organization{
		ID:   uuid.New().String(),
		Name: "Duplicate Test Org",
	}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}

	// First create should succeed
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("First create failed: %v", err)
	}

	// Second create should fail
	err := testStore.Create(ctx, org, item)
	if err != store.ErrAlreadyExists {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestGet_NotFound(t *testing.T) {
	ctx := context.Background()

	_, err := testStore.Get(ctx, organizationsTable, store.PK{
		"id": &types.AttributeValueMemberS{Value: "nonexistent-id"},
	})

	if err != store.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Update Tests ---

func TestUpdate_Success(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{
		ID:   uuid.New().String(),
		Name: "Update Test Org",
	}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update org
	updateItem := map[string]types.AttributeValue{
		"name": &types.AttributeValueMemberS{Value: "Updated Name"},
	}
	if err := testStore.Update(ctx, org, updateItem, 1); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	result, err := testStore.Get(ctx, org.TableName(), org.GetKey())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result.Version != 2 {
		t.Errorf("expected version 2, got %d", result.Version)
	}

	name := result.Raw["name"].(*types.AttributeValueMemberS).Value
	if name != "Updated Name" {
		t.Errorf("expected name 'Updated Name', got %q", name)
	}
}

func TestUpdate_OptimisticLockFailure(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{
		ID:   uuid.New().String(),
		Name: "Lock Test Org",
	}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// First update succeeds
	if err := testStore.Update(ctx, org, item, 1); err != nil {
		t.Fatalf("First update failed: %v", err)
	}

	// Second update with stale version fails
	err := testStore.Update(ctx, org, item, 1)
	if err != store.ErrConcurrentModification {
		t.Errorf("expected ErrConcurrentModification, got %v", err)
	}
}

// --- Unique Constraint Tests ---

func TestUniqueConstraint_Enforced(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{
		ID:   uuid.New().String(),
		Name: "Unique Test Org",
	}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	// Create first studio
	studio1 := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Unique Name",
		Slug:           "unique-slug",
	}
	studio1Item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio1.ID},
		"name": &types.AttributeValueMemberS{Value: studio1.Name},
		"slug": &types.AttributeValueMemberS{Value: studio1.Slug},
	}
	if err := testStore.Create(ctx, studio1, studio1Item); err != nil {
		t.Fatalf("Create studio1 failed: %v", err)
	}

	// Try to create second studio with same name (should fail)
	studio2 := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Unique Name", // Duplicate!
		Slug:           "different-slug",
	}
	studio2Item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio2.ID},
		"name": &types.AttributeValueMemberS{Value: studio2.Name},
		"slug": &types.AttributeValueMemberS{Value: studio2.Slug},
	}

	err := testStore.Create(ctx, studio2, studio2Item)
	if err != store.ErrDuplicateValue {
		t.Errorf("expected ErrDuplicateValue, got %v", err)
	}
}

func TestUniqueConstraint_DifferentParents_AllowsSameName(t *testing.T) {
	ctx := context.Background()

	// Create two orgs
	org1 := Organization{ID: uuid.New().String(), Name: "Org 1"}
	org2 := Organization{ID: uuid.New().String(), Name: "Org 2"}

	for _, org := range []Organization{org1, org2} {
		item := map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: org.ID},
			"name": &types.AttributeValueMemberS{Value: org.Name},
		}
		if err := testStore.Create(ctx, org, item); err != nil {
			t.Fatalf("Create org failed: %v", err)
		}
	}

	// Create studios with same name under different orgs (should work)
	for i, orgID := range []string{org1.ID, org2.ID} {
		studio := Studio{
			ID:             uuid.New().String(),
			OrganizationID: orgID,
			Name:           "Same Name",
			Slug:           fmt.Sprintf("same-slug-%d", i),
		}
		item := map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: studio.ID},
			"name": &types.AttributeValueMemberS{Value: studio.Name},
			"slug": &types.AttributeValueMemberS{Value: studio.Slug},
		}
		if err := testStore.Create(ctx, studio, item); err != nil {
			t.Errorf("Create studio under org %d failed: %v", i+1, err)
		}
	}
}

func TestUniqueConstraint_UpdateChangesUniqueField(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{ID: uuid.New().String(), Name: "Update Unique Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	// Create studio
	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Original Name",
		Slug:           "original-slug",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Create(ctx, studio, studioItem); err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	// Update unique field
	studio.Name = "Updated Name"
	updateItem := map[string]types.AttributeValue{
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Update(ctx, studio, updateItem, 1); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify old name is now available
	studio2 := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Original Name", // Now free
		Slug:           "another-slug",
	}
	studio2Item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio2.ID},
		"name": &types.AttributeValueMemberS{Value: studio2.Name},
		"slug": &types.AttributeValueMemberS{Value: studio2.Slug},
	}
	if err := testStore.Create(ctx, studio2, studio2Item); err != nil {
		t.Errorf("Create with old name failed: %v", err)
	}
}

// --- Delete Tests ---

func TestDelete_SoftDelete_SetsTTL(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{ID: uuid.New().String(), Name: "Delete Test Org"}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Delete (soft)
	if err := testStore.Delete(ctx, org, store.DeleteOptions{}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get should return not found (item has TTL)
	_, err := testStore.Get(ctx, org.TableName(), org.GetKey())
	if err != store.ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Direct DynamoDB get should show TTL is set
	result, err := ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(org.TableName()),
		Key:       org.GetKey(),
	})
	if err != nil {
		t.Fatalf("Direct get failed: %v", err)
	}

	if _, ok := result.Item["ttl"]; !ok {
		t.Error("expected ttl to be set on deleted item")
	}
}

func TestDelete_OrphanProtect_FailsWithChildren(t *testing.T) {
	ctx := context.Background()

	// Create org with child
	org := Organization{ID: uuid.New().String(), Name: "Orphan Protect Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Child Studio",
		Slug:           "child-studio",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Create(ctx, studio, studioItem); err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	// Try to delete org with orphan protection (should fail)
	err := testStore.Delete(ctx, org, store.DeleteOptions{OrphanProtect: true})
	if err != store.ErrHasChildren {
		t.Errorf("expected ErrHasChildren, got %v", err)
	}
}

func TestDelete_OrphanProtect_SucceedsWithoutChildren(t *testing.T) {
	ctx := context.Background()

	// Create org without children
	org := Organization{ID: uuid.New().String(), Name: "Childless Org"}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Delete with orphan protection (should succeed)
	if err := testStore.Delete(ctx, org, store.DeleteOptions{OrphanProtect: true}); err != nil {
		t.Errorf("expected delete to succeed, got %v", err)
	}
}

// --- Relationship Tests ---

func TestRelationship_RecordCreated(t *testing.T) {
	ctx := context.Background()

	// Create org and studio
	org := Organization{ID: uuid.New().String(), Name: "Relationship Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Relationship Studio",
		Slug:           "relationship-studio",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Create(ctx, studio, studioItem); err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	// Query children
	children, err := testStore.QueryAllChildren(ctx, org.EntityRef())
	if err != nil {
		t.Fatalf("QueryAllChildren failed: %v", err)
	}

	if len(children) != 1 {
		t.Errorf("expected 1 child, got %d", len(children))
	}
	if len(children) > 0 && children[0].Ref != studio.EntityRef() {
		t.Errorf("expected child ref %q, got %q", studio.EntityRef(), children[0].Ref)
	}
}

func TestHasActiveChildren(t *testing.T) {
	ctx := context.Background()

	// Create org
	org := Organization{ID: uuid.New().String(), Name: "Has Children Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	// Initially no children
	hasChildren, err := testStore.HasActiveChildren(ctx, org.EntityRef())
	if err != nil {
		t.Fatalf("HasActiveChildren failed: %v", err)
	}
	if hasChildren {
		t.Error("expected no children initially")
	}

	// Add child
	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Child For Check",
		Slug:           "child-for-check",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Create(ctx, studio, studioItem); err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	// Now has children
	hasChildren, err = testStore.HasActiveChildren(ctx, org.EntityRef())
	if err != nil {
		t.Fatalf("HasActiveChildren failed: %v", err)
	}
	if !hasChildren {
		t.Error("expected to have children after adding studio")
	}

	// Delete child
	if err := testStore.Delete(ctx, studio, store.DeleteOptions{}); err != nil {
		t.Fatalf("Delete studio failed: %v", err)
	}

	// Simulate cascade handler setting TTL on relationship record
	// (In production, this is done by the Lambda handler via DynamoDB Streams)
	if err := testStore.SetRelationshipTTL(ctx, studio.EntityRef(), org.EntityRef(), time.Now().Unix()); err != nil {
		t.Fatalf("SetRelationshipTTL failed: %v", err)
	}

	// No active children after delete (TTL set on both entity and relationship)
	hasChildren, err = testStore.HasActiveChildren(ctx, org.EntityRef())
	if err != nil {
		t.Fatalf("HasActiveChildren failed: %v", err)
	}
	if hasChildren {
		t.Error("expected no active children after child deleted")
	}
}

// --- Deep Hierarchy Tests ---

func TestDeepHierarchy_ThreeLevels(t *testing.T) {
	ctx := context.Background()

	// Create org -> studio -> title
	org := Organization{ID: uuid.New().String(), Name: "Deep Hierarchy Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	studio := Studio{
		ID:             uuid.New().String(),
		OrganizationID: org.ID,
		Name:           "Deep Studio",
		Slug:           "deep-studio",
	}
	studioItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: studio.ID},
		"name": &types.AttributeValueMemberS{Value: studio.Name},
		"slug": &types.AttributeValueMemberS{Value: studio.Slug},
	}
	if err := testStore.Create(ctx, studio, studioItem); err != nil {
		t.Fatalf("Create studio failed: %v", err)
	}

	title := Title{
		ID:       uuid.New().String(),
		StudioID: studio.ID,
		Name:     "Deep Title",
	}
	titleItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: title.ID},
		"name": &types.AttributeValueMemberS{Value: title.Name},
	}
	if err := testStore.Create(ctx, title, titleItem); err != nil {
		t.Fatalf("Create title failed: %v", err)
	}

	// Verify all were created
	if _, err := testStore.Get(ctx, org.TableName(), org.GetKey()); err != nil {
		t.Errorf("Get org failed: %v", err)
	}
	if _, err := testStore.Get(ctx, studio.TableName(), studio.GetKey()); err != nil {
		t.Errorf("Get studio failed: %v", err)
	}
	if _, err := testStore.Get(ctx, title.TableName(), title.GetKey()); err != nil {
		t.Errorf("Get title failed: %v", err)
	}

	// Verify relationships
	studioChildren, _ := testStore.QueryAllChildren(ctx, studio.EntityRef())
	if len(studioChildren) != 1 {
		t.Errorf("expected studio to have 1 child, got %d", len(studioChildren))
	}
}

// --- Query Tests ---

func TestQuery_WithTTLFiltering(t *testing.T) {
	ctx := context.Background()

	// Create parent org
	org := Organization{ID: uuid.New().String(), Name: "Query Test Org"}
	orgItem := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, orgItem); err != nil {
		t.Fatalf("Create org failed: %v", err)
	}

	// Create multiple studios
	var studioIDs []string
	for i := 0; i < 3; i++ {
		studio := Studio{
			ID:             uuid.New().String(),
			OrganizationID: org.ID,
			Name:           fmt.Sprintf("Query Studio %d", i),
			Slug:           fmt.Sprintf("query-studio-%d", i),
		}
		studioIDs = append(studioIDs, studio.ID)
		item := map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: studio.ID},
			"name": &types.AttributeValueMemberS{Value: studio.Name},
			"slug": &types.AttributeValueMemberS{Value: studio.Slug},
		}
		if err := testStore.Create(ctx, studio, item); err != nil {
			t.Fatalf("Create studio %d failed: %v", i, err)
		}
	}

	// Delete one studio
	deleteStudio := Studio{ID: studioIDs[1], OrganizationID: org.ID, Name: "Query Studio 1", Slug: "query-studio-1"}
	if err := testStore.Delete(ctx, deleteStudio, store.DeleteOptions{}); err != nil {
		t.Fatalf("Delete studio failed: %v", err)
	}

	// Query should only return active studios
	// Note: This requires a GSI on parent_ref for a real query, but we can test the TTL filtering
	// by querying the relationship table
	children, err := testStore.QueryAllChildren(ctx, org.EntityRef())
	if err != nil {
		t.Fatalf("QueryAllChildren failed: %v", err)
	}

	// All 3 children are in relationship table (QueryAllChildren doesn't filter by TTL)
	// This is expected behavior - cascade delete needs all children
	if len(children) != 3 {
		t.Errorf("expected 3 children in relationship table, got %d", len(children))
	}
}

// --- Idempotency Tests ---

func TestDelete_Idempotent(t *testing.T) {
	ctx := context.Background()

	org := Organization{ID: uuid.New().String(), Name: "Idempotent Delete Org"}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Delete twice - should not error
	if err := testStore.Delete(ctx, org, store.DeleteOptions{}); err != nil {
		t.Fatalf("First delete failed: %v", err)
	}
	if err := testStore.Delete(ctx, org, store.DeleteOptions{}); err != nil {
		t.Errorf("Second delete should be idempotent, got: %v", err)
	}
}

func TestSetTTL_Idempotent(t *testing.T) {
	ctx := context.Background()

	org := Organization{ID: uuid.New().String(), Name: "Idempotent TTL Org"}
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: org.ID},
		"name": &types.AttributeValueMemberS{Value: org.Name},
	}
	if err := testStore.Create(ctx, org, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// SetTTL twice - should not error
	if err := testStore.SetTTL(ctx, org); err != nil {
		t.Fatalf("First SetTTL failed: %v", err)
	}
	if err := testStore.SetTTL(ctx, org); err != nil {
		t.Errorf("Second SetTTL should be idempotent, got: %v", err)
	}
}
