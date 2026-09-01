// Package localization provides message catalogs, locale selection, and
// boundary rendering for NornicDB's human-readable production text.
//
//go:generate go run ./internal/proceduremetadata -root ../..
//go:generate go run ../../scripts/localization_catalog -root ../.. -out pkg/localization/catalog_manifest_gen.go
package localization
