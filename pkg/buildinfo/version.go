package buildinfo

import (
	_ "embed"
	"fmt"
	"strings"
)

// Version is loaded from the tracked VERSION file, which is synced from the
// latest git tag during make build.
//
//go:embed VERSION
var versionFile string

var (
	Commit    = "dev"
	BuildTime = "unknown"
)

func Version() string {
	v := strings.TrimSpace(versionFile)
	if v == "" {
		return "dev"
	}
	return v
}

func ProductVersion() string {
	return "v" + Version()
}

func ShortCommit() string {
	commit := strings.TrimSpace(Commit)
	if commit == "" {
		return "dev"
	}
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}

func DisplayVersion() string {
	versionInfo := ProductVersion()
	if shortCommit := ShortCommit(); shortCommit != "" && shortCommit != "dev" {
		versionInfo = fmt.Sprintf("%s-%s", versionInfo, shortCommit)
	}
	if buildTime := strings.TrimSpace(BuildTime); buildTime != "" && buildTime != "unknown" {
		versionInfo = fmt.Sprintf("%s (built: %s)", versionInfo, buildTime)
	}
	return versionInfo
}

func ServerAnnouncement() string {
	return "NornicDB/" + Version()
}
