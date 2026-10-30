package tck

import (
	"path/filepath"
	"testing"
)

func TestVendoredCorpusInventory(t *testing.T) {
	root := filepath.Join("testdata", "opencypher", "features")
	inv, err := BuildInventory(root)
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if inv.Files != 220 {
		t.Fatalf("feature files = %d, want 220", inv.Files)
	}
	if inv.Scenarios != 3897 || inv.Steps != 16006 {
		t.Fatalf("expanded inventory changed: %+v", inv)
	}
	if inv.UpstreamRevision != UpstreamRevision || inv.ArchiveSHA256 != ArchiveSHA256 {
		t.Fatalf("provenance changed: %+v", inv)
	}
	if inv.CorpusSHA256 != "d56bf265aff4e52275b1d027588d1fe7571097df838e3bd220270ab9502ca57e" {
		t.Fatalf("corpus digest changed: %s", inv.CorpusSHA256)
	}
}
