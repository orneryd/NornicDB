package server

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type errFS struct{}

func (errFS) Open(string) (fs.File, error) { return nil, errors.New("open failed") }

type subFailFS struct{}

func (subFailFS) Open(name string) (fs.File, error) {
	if name == "." {
		return nil, errors.New("root open not supported")
	}
	return nil, errors.New("open failed")
}

func (subFailFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, errors.New("readdir failed")
	}
	return []fs.DirEntry{fs.FileInfoToDirEntry(fakeDirInfo{name: "dist"})}, nil
}

type fakeDirInfo struct{ name string }

func (f fakeDirInfo) Name() string       { return f.name }
func (f fakeDirInfo) Size() int64        { return 0 }
func (f fakeDirInfo) Mode() fs.FileMode  { return fs.ModeDir }
func (f fakeDirInfo) ModTime() time.Time { return time.Time{} }
func (f fakeDirInfo) IsDir() bool        { return true }
func (f fakeDirInfo) Sys() interface{}   { return nil }

func TestUIHelpers_AdditionalBranchCoverage(t *testing.T) {
	require.Equal(t, "", normalizeUIBasePath("/a/../b"))
	require.Equal(t, "", normalizeUIBasePath("///"))
	require.Equal(t, "", sanitizeUIBasePath(`/x"y`))
	require.Equal(t, "", sanitizeUIBasePath(`/x\y`))
	require.Equal(t, "", sanitizeUIBasePath("/./"))

	oldEnabled, oldAssets := UIEnabled, UIAssets
	t.Cleanup(func() {
		UIEnabled = oldEnabled
		UIAssets = oldAssets
	})

	UIEnabled = true
	UIAssets = errFS{}
	h, err := newUIHandler()
	require.Nil(t, h)
	require.ErrorContains(t, err, "failed to read embedded root")

	UIAssets = subFailFS{}
	h, err = newUIHandler()
	require.Nil(t, h)
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "failed to get dist subdirectory") ||
			strings.Contains(err.Error(), "failed to read index.html"),
		"unexpected error: %v", err)
}

func TestUIHandler_ServeHTTP_NormalizesRelativePath(t *testing.T) {
	h := &uiHandler{
		fileServer: http.NotFoundHandler(),
		indexHTML:  []byte("<html></html>"),
		basePath:   "",
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.URL.Path = "relative"
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "Invalid path")
}

func TestPublicHandlers_AdditionalCoverage(t *testing.T) {
	server, _ := setupTestServer(t)
	server.config.Address = "0.0.0.0"

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = ""
	rec := httptest.NewRecorder()
	server.handleDiscovery(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var discovery map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &discovery))
	require.Contains(t, discovery["bolt_direct"], "localhost")

	// Ensure embedInfo branch with non-nil stats executes.
	server.db.SetEmbedder(&countingEmbedder{dims: 16})
	statusReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	statusRec := httptest.NewRecorder()
	server.handleStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), `"embeddings"`)
}

func TestStartQdrantGRPC_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// features == nil branch; defaults should leave gRPC disabled and return nil.
	server.config.Features = nil
	require.NoError(t, server.startQdrantGRPC())

	// storage lookup error branch via offline default database.
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled: true,
	}
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.SetDatabaseStatus(t.Context(), dbName, "offline"))
	err := server.startQdrantGRPC()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get storage")
	require.NoError(t, server.dbManager.SetDatabaseStatus(t.Context(), dbName, "online"))

	// start failure branch with invalid listen address.
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:    true,
		QdrantGRPCListenAddr: "bad:addr",
	}
	err = server.startQdrantGRPC()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "failed to start")

	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:           true,
		QdrantGRPCListenAddr:        "127.0.0.1:0",
		QdrantGRPCTLSEnabled:        true,
		QdrantGRPCTLSClientAuthMode: "invalid",
	}
	err = server.startQdrantGRPC()
	require.ErrorContains(t, err, "invalid TLS client auth mode")

	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:    true,
		QdrantGRPCListenAddr: "127.0.0.1:0",
		QdrantGRPCTLSEnabled: true,
		QdrantGRPCTLSCert:    "/missing/grpc.crt",
		QdrantGRPCTLSKey:     "/missing/grpc.key",
	}
	err = server.startQdrantGRPC()
	require.ErrorContains(t, err, "failed to load TLS configuration")

	// Valid permission override parsing branch before failing start.
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:    true,
		QdrantGRPCListenAddr: "bad:addr",
		QdrantGRPCMethodPermissions: map[string]string{
			"/qdrant.Points/Search": string(auth.PermRead),
		},
	}
	err = server.startQdrantGRPC()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "failed to start")

	// running-server early return branch.
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:    true,
		QdrantGRPCListenAddr: "127.0.0.1:0",
	}
	require.NoError(t, server.startQdrantGRPC())
	t.Cleanup(func() { server.stopQdrantGRPC() })
	require.NoError(t, server.startQdrantGRPC())
}

func TestPublicHandleStatus_CanceledContextReturnsEarly(t *testing.T) {
	server, _ := setupTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/status", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	server.handleStatus(rec, req)
	require.Equal(t, "", rec.Body.String())
}
