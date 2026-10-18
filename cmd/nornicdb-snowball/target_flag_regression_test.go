package main

import "testing"

func TestPackageTargetFlagSupported(t *testing.T) {
	_, err := parsePackageArgs([]string{
		"--language", "ukrainian", "--id", "snowball.ukrainian", "--version", "1.0.0",
		"--module", "./stemmer-module", "--source", "./stemmer-module/stemmer.go",
		"--output", "./plugins/stemmers/snowball-ukrainian.so", "--target", "./bin/nornicdb",
	})
	if err != nil {
		t.Fatal(err)
	}
}
