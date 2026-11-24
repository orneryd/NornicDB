package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

const preparedDatabaseFolder = "prepared-databases"
const preparedDatabaseSeal = "prepared.json"

// PreparedDatabasesDirectory is the independent storage area beneath the data root.
func PreparedDatabasesDirectory(root string) string {
	return filepath.Join(root, preparedDatabaseFolder)
}

// PreparedDatabase describes the graph in a closed store before it becomes
// accessible through the catalog. Counts certify the initial import.
type PreparedDatabase struct {
	Database      string `json:"database"`
	Nodes         int64  `json:"nodes"`
	Relationships int64  `json:"relationships"`
}

func validPreparedDatabaseName(name string) bool {
	return name != "" && !strings.ContainsAny(name, ":/\\\x00") && strings.IndexFunc(name, unicode.IsSpace) < 0
}

// CreatePreparedDatabaseDirectory allocates a private store on the server's
// filesystem without opening or locking the server's main Badger directory.
func CreatePreparedDatabaseDirectory(root, database string) (id, directory string, err error) {
	if !validPreparedDatabaseName(database) {
		return "", "", fmt.Errorf("invalid prepared database name: %w", ErrInvalidData)
	}
	parent := PreparedDatabasesDirectory(root)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", "", err
	}
	id = uuid.NewString()
	directory = filepath.Join(parent, id)
	if err := os.Mkdir(directory, 0700); err != nil {
		return "", "", err
	}
	return id, directory, nil
}

// PreparedDatabaseDirectory resolves only a canonical preparation ID within
// the server-owned store directory.
func PreparedDatabaseDirectory(root, id string) (string, error) {
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return "", fmt.Errorf("invalid prepared database ID: %w", ErrInvalidData)
	}
	return filepath.Join(PreparedDatabasesDirectory(root), id), nil
}

// SealPreparedDatabase publishes the readiness marker after the builder has
// synced and closed its Badger handle.
func SealPreparedDatabase(directory string, graph PreparedDatabase) error {
	encoded, err := json.Marshal(graph)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".prepared-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(encoded)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(directory, preparedDatabaseSeal)); err != nil {
		return err
	}
	return errors.Join(SyncDir(directory), SyncDir(filepath.Dir(directory)), SyncDir(filepath.Dir(filepath.Dir(directory))))
}

// ReadPreparedDatabase looks up a sealed store and checks that it is bound
// to the requested namespace. Badger may only open a real existing store.
func ReadPreparedDatabase(root, id, database string) (string, PreparedDatabase, error) {
	dir, graph, err := ReadPreparedDatabaseSeal(root, id, database)
	if err != nil {
		return "", graph, err
	}
	if err := VerifyPreparedDatabaseManifest(dir); err != nil {
		return "", graph, err
	}
	return dir, graph, nil
}

// ReadPreparedDatabaseSeal checks the namespace binding even when the native
// store is damaged. Only the published-store recovery path may use it alone.
func ReadPreparedDatabaseSeal(root, id, database string) (string, PreparedDatabase, error) {
	var graph PreparedDatabase
	dir, err := PreparedDatabaseDirectory(root, id)
	if err != nil {
		return "", graph, err
	}
	for _, path := range []string{filepath.Dir(dir), dir} {
		info, err := os.Lstat(path)
		if err != nil {
			return "", graph, err
		}
		if !info.IsDir() {
			return "", graph, fmt.Errorf("prepared store path must be a directory: %w", ErrInvalidData)
		}
	}
	if err := verifyPreparedFile(dir, preparedDatabaseSeal); err != nil {
		return "", graph, err
	}
	encoded, err := os.ReadFile(filepath.Join(dir, preparedDatabaseSeal))
	if err != nil {
		return "", graph, err
	}
	if err := json.Unmarshal(encoded, &graph); err != nil {
		return "", graph, fmt.Errorf("reading prepared store seal: %v: %w", err, ErrInvalidData)
	}
	if graph.Database != database || !validPreparedDatabaseName(database) || graph.Nodes < 0 || graph.Relationships < 0 {
		return "", graph, fmt.Errorf("prepared store seal does not match the database: %w", ErrInvalidData)
	}
	return dir, graph, nil
}

// VerifyPreparedDatabaseManifest verifies that opening the native engine will
// use its existing manifest instead of silently creating a new one.
func VerifyPreparedDatabaseManifest(dir string) error {
	return verifyPreparedFile(dir, "MANIFEST")
}

func verifyPreparedFile(dir, name string) error {
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("invalid prepared store file %s: %w", name, ErrInvalidData)
	}
	return nil
}
