// Package search - one-off conversion of persisted search index files from gob to msgpack.
// Run the script in scripts/convert_search_index_to_msgpack/ after backing up data/test/search
// so that existing index files are converted in place and the new msgpack-only Load paths will work.

package search

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// errAlreadyMsgpack is returned when the file decodes as msgpack (already converted); caller should skip.
var errAlreadyMsgpack = errors.New("already msgpack")

// ConvertSearchIndexDirFromGobToMsgpack converts search index .gob files under rootDir to msgpack
// and removes the .gob extension (writes to bm25, vectors, hnsw, hnsw_ivf/0, 1, ...).
// rootDir should be the search index root (e.g. data/test/search). Back up the directory before running.
func ConvertSearchIndexDirFromGobToMsgpack(rootDir string) error {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dbDir := filepath.Join(rootDir, e.Name())
		if err := convertOneDbDir(dbDir); err != nil {
			return fmt.Errorf("%s: %w", e.Name(), err)
		}
	}
	return nil
}

func convertOneDbDir(dbDir string) error {
	// bm25.gob -> bm25
	if err := convertFileGobToMsgpack(
		filepath.Join(dbDir, "bm25.gob"),
		filepath.Join(dbDir, "bm25"),
		"fulltext"); err != nil {
		if errors.Is(err, errAlreadyMsgpack) {
			logSearchPrintf("Skip (already msgpack) %s/bm25", dbDir)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("bm25: %w", err)
		}
	} else {
		logSearchPrintf("Converted %s/bm25.gob -> bm25", dbDir)
	}

	// vectors.gob -> vectors
	if err := convertFileGobToMsgpack(
		filepath.Join(dbDir, "vectors.gob"),
		filepath.Join(dbDir, "vectors"),
		"vector"); err != nil {
		if errors.Is(err, errAlreadyMsgpack) {
			logSearchPrintf("Skip (already msgpack) %s/vectors", dbDir)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("vectors: %w", err)
		}
	} else {
		logSearchPrintf("Converted %s/vectors.gob -> vectors", dbDir)
	}

	// hnsw.gob -> hnsw
	if err := convertFileGobToMsgpack(
		filepath.Join(dbDir, "hnsw.gob"),
		filepath.Join(dbDir, "hnsw"),
		"hnsw"); err != nil {
		if errors.Is(err, errAlreadyMsgpack) {
			logSearchPrintf("Skip (already msgpack) %s/hnsw", dbDir)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("hnsw: %w", err)
		}
	} else {
		logSearchPrintf("Converted %s/hnsw.gob -> hnsw", dbDir)
	}

	// hnsw_ivf/N.gob -> hnsw_ivf/N (then remove N.gob)
	ivfDir := filepath.Join(dbDir, "hnsw_ivf")
	ivfEntries, err := os.ReadDir(ivfDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, f := range ivfEntries {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasSuffix(name, ".gob") {
			// Already no extension (e.g. "0", "1") - skip or treat as already converted
			continue
		}
		var cid int
		if _, err := fmt.Sscanf(name, "%d.gob", &cid); err != nil {
			continue
		}
		oldPath := filepath.Join(ivfDir, name)
		newPath := filepath.Join(ivfDir, fmt.Sprintf("%d", cid))
		if err := convertFileGobToMsgpack(oldPath, newPath, "hnsw"); err != nil {
			if errors.Is(err, errAlreadyMsgpack) {
				logSearchPrintf("Skip (already msgpack) %s", newPath)
			} else {
				return fmt.Errorf("hnsw_ivf/%s: %w", name, err)
			}
		} else {
			logSearchPrintf("Converted %s -> %s", oldPath, newPath)
		}
	}
	return nil
}

// convertFileGobToMsgpack reads from oldPath (gob), writes msgpack to newPath, then removes oldPath.
// If newPath already exists and decodes as msgpack, returns errAlreadyMsgpack (skip).
// If oldPath does not exist and newPath doesn't exist or isn't valid msgpack, returns os.ErrNotExist.
func convertFileGobToMsgpack(oldPath, newPath, format string) error {
	// If newPath already exists and is valid msgpack, already converted.
	if f, err := os.Open(newPath); err == nil {
		already := tryDecodeMsgpack(f, format)
		f.Close()
		if already {
			return errAlreadyMsgpack
		}
	}

	// Read from oldPath (gob).
	f, err := os.Open(oldPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var snap interface{}
	switch format {
	case "fulltext":
		var s fulltextIndexSnapshot
		if err := gob.NewDecoder(f).Decode(&s); err != nil {
			return err
		}
		snap = &s
	case "vector":
		var s vectorIndexSnapshot
		if err := gob.NewDecoder(f).Decode(&s); err != nil {
			return err
		}
		snap = &s
	case "hnsw":
		var s hnswIndexSnapshot
		if err := gob.NewDecoder(f).Decode(&s); err != nil {
			return err
		}
		snap = &s
	default:
		return fmt.Errorf("unknown format %q", format)
	}
	f.Close()

	// Write msgpack to newPath (encode pointer so msgpack encodes the struct).
	out, err := os.Create(newPath)
	if err != nil {
		return err
	}
	if err := msgpack.NewEncoder(out).Encode(snap); err != nil {
		out.Close()
		os.Remove(newPath)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(newPath)
		return err
	}

	// Remove old .gob file.
	return os.Remove(oldPath)
}

func tryDecodeMsgpack(f *os.File, format string) bool {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false
	}
	switch format {
	case "fulltext":
		var s fulltextIndexSnapshot
		return msgpack.NewDecoder(f).Decode(&s) == nil
	case "vector":
		var s vectorIndexSnapshot
		return msgpack.NewDecoder(f).Decode(&s) == nil
	case "hnsw":
		var s hnswIndexSnapshot
		return msgpack.NewDecoder(f).Decode(&s) == nil
	}
	return false
}
