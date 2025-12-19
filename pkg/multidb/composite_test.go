package multidb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Verify composite database exists
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, "composite1", info.Name)
	assert.Equal(t, "composite", info.Type)
	assert.Equal(t, 2, len(info.Constituents))
	assert.Equal(t, "db1", info.Constituents[0].DatabaseName)
	assert.Equal(t, "db2", info.Constituents[1].DatabaseName)
}

func TestCreateCompositeDatabase_InvalidConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Try to create composite with non-existent constituent
	constituents := []ConstituentRef{
		{DatabaseName: "nonexistent", Alias: "db1", Type: "local", AccessMode: "read_write"},
	}
	err := manager.CreateCompositeDatabase("composite1", constituents)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCreateCompositeDatabase_DuplicateAlias(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Try to create composite with duplicate alias
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias1", Type: "local", AccessMode: "read_write"}, // Duplicate alias
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestCreateCompositeDatabase_CompositeAsConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create first composite
	constituents1 := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents1)
	require.NoError(t, err)

	// Try to use composite as constituent
	constituents2 := []ConstituentRef{
		{DatabaseName: "composite1", Alias: "comp1", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite2", constituents2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use composite database")
}

func TestDropCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Drop composite database
	err = manager.DropCompositeDatabase("composite1")
	require.NoError(t, err)

	// Verify composite database is gone
	_, err = manager.GetDatabase("composite1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify constituent databases still exist
	_, err = manager.GetDatabase("db1")
	require.NoError(t, err)
	_, err = manager.GetDatabase("db2")
	require.NoError(t, err)
}

func TestAddConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 2 constituents
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Add third constituent
	newConstituent := ConstituentRef{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"}
	err = manager.AddConstituent("composite1", newConstituent)
	require.NoError(t, err)

	// Verify constituent was added
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, 3, len(info.Constituents))

	// Verify new constituent is in the list
	found := false
	for _, c := range info.Constituents {
		if c.DatabaseName == "db3" {
			found = true
			break
		}
	}
	assert.True(t, found, "db3 should be in constituents list")
}

func TestAddConstituent_DuplicateAlias(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Try to add constituent with duplicate alias
	newConstituent := ConstituentRef{DatabaseName: "db3", Alias: "alias1", Type: "local", AccessMode: "read_write"}
	err = manager.AddConstituent("composite1", newConstituent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestRemoveConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 3 constituents
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Remove constituent
	err = manager.RemoveConstituent("composite1", "db2")
	require.NoError(t, err)

	// Verify constituent was removed
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, 2, len(info.Constituents))

	// Verify db2 is not in the list
	for _, c := range info.Constituents {
		assert.NotEqual(t, "db2", c.DatabaseName, "db2 should not be in constituents list")
	}
}

func TestRemoveConstituent_NotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Try to remove non-existent constituent
	err = manager.RemoveConstituent("composite1", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestListConstituents(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias2", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db3", Alias: "alias3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// List constituents
	list, err := manager.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	assert.Equal(t, 3, len(list))

	// Verify all constituents are in the list
	constituentMap := make(map[string]bool)
	for _, c := range list {
		constituentMap[c.DatabaseName] = true
	}
	assert.True(t, constituentMap["db1"])
	assert.True(t, constituentMap["db2"])
	assert.True(t, constituentMap["db3"])
}

func TestListConstituents_NotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Try to list constituents for non-existent composite
	_, err := manager.GetCompositeConstituents("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestConstituentRef_Validate(t *testing.T) {
	tests := []struct {
		name      string
		ref       ConstituentRef
		wantError bool
	}{
		{
			name:      "valid with database name and alias",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
			wantError: false,
		},
		{
			name:      "invalid empty alias",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "", Type: "local", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid empty database name",
			ref:       ConstituentRef{DatabaseName: "", Alias: "alias1", Type: "local", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid type",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "invalid", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid access mode",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "invalid"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ref.Validate()
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestListCompositeDatabases(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite databases
	constituents1 := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents1)
	require.NoError(t, err)

	constituents2 := []ConstituentRef{
		{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite2", constituents2)
	require.NoError(t, err)

	// List composite databases
	composites := manager.ListCompositeDatabases()
	assert.Equal(t, 2, len(composites))

	// Verify both composites are in the list
	compositeMap := make(map[string]bool)
	for _, comp := range composites {
		compositeMap[comp.Name] = true
	}
	assert.True(t, compositeMap["composite1"])
	assert.True(t, compositeMap["composite2"])
}

func TestIsCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create standard database
	err = manager.CreateDatabase("standard_db")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite_db", constituents)
	require.NoError(t, err)

	// Check if composite
	assert.True(t, manager.IsCompositeDatabase("composite_db"))
	assert.False(t, manager.IsCompositeDatabase("standard_db"))
	assert.False(t, manager.IsCompositeDatabase("nonexistent"))
}
