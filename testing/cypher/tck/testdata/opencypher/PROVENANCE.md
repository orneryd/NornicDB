# openCypher TCK provenance

- Repository: `https://github.com/opencypher/openCypher`
- Revision: `370fe27f417730dca2ef712dd1c0c5dadcb99ef8`
- Source archive SHA-256: `9f8e8bb027664e1a951b529a37fe44330cbb7f670cf41fdbd4af65d3ac5369aa`
- Vendored paths: `tck/` plus repository `LICENSE` and `NOTICE`
- License: Apache-2.0; see `LICENSE`, `NOTICE`, and per-feature headers

The corpus is an unmodified extraction from the pinned archive. Generate the
expanded inventory from the repository root with:

```bash
go run ./testing/cypher/tck/cmd/inventory
```

Updating the revision requires a separately reviewed baseline update containing
the new archive checksum, corpus digest, scenario inventory, license review and
known-failure diff.
