//go:build census_inactive_fixture

package fixture

import aliased "census.fixture/meta"

type inactiveAlias = map[string]aliased.Schema

var globalInactiveSchema = inactiveAlias{"x": {Header: []string{"a"}}}

func arbitraryInactive() inactiveAlias { return globalInactiveSchema }
