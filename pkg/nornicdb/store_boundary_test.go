package nornicdb

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := func() (code int) {
		// Go cannot unload a plugin. Keep its one shared build until every
		// test run returns, then remove it as the fixture owner exits.
		defer func() {
			if bundledAPOCPluginPath != "" {
				if err := os.RemoveAll(bundledAPOCPluginPath); err != nil {
					fmt.Fprintln(os.Stderr, "remove bundled APOC test build:", err)
					code = 1
				}
			}
		}()
		return fixtureowner.Run(m, "nornicdb")
	}()
	os.Exit(code)
}
