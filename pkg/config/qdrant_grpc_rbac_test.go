package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile_QdrantGRPCRBAC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nornicdb.yaml")

	err := os.WriteFile(path, []byte(`
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: "127.0.0.1:6334"
  qdrant_grpc_max_vector_dim: 2048
  qdrant_grpc_max_batch_points: 123
  qdrant_grpc_max_top_k: 77
  qdrant_grpc_rbac:
    methods:
      "Points/Upsert": "write"
      "Points/Search": "read"
`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	if !cfg.Features.QdrantGRPCEnabled {
		t.Fatalf("expected QdrantGRPCEnabled true")
	}
	if cfg.Features.QdrantGRPCListenAddr != "127.0.0.1:6334" {
		t.Fatalf("unexpected listen addr: %q", cfg.Features.QdrantGRPCListenAddr)
	}
	if cfg.Features.QdrantGRPCMaxVectorDim != 2048 {
		t.Fatalf("unexpected max vector dim: %d", cfg.Features.QdrantGRPCMaxVectorDim)
	}
	if cfg.Features.QdrantGRPCMaxBatchPoints != 123 {
		t.Fatalf("unexpected max batch points: %d", cfg.Features.QdrantGRPCMaxBatchPoints)
	}
	if cfg.Features.QdrantGRPCMaxTopK != 77 {
		t.Fatalf("unexpected max top_k: %d", cfg.Features.QdrantGRPCMaxTopK)
	}
	if cfg.Features.QdrantGRPCMethodPermissions == nil {
		t.Fatalf("expected method permissions map")
	}
	if cfg.Features.QdrantGRPCMethodPermissions["Points/Upsert"] != "write" {
		t.Fatalf("unexpected Points/Upsert mapping: %q", cfg.Features.QdrantGRPCMethodPermissions["Points/Upsert"])
	}
	if cfg.Features.QdrantGRPCMethodPermissions["Points/Search"] != "read" {
		t.Fatalf("unexpected Points/Search mapping: %q", cfg.Features.QdrantGRPCMethodPermissions["Points/Search"])
	}
}
