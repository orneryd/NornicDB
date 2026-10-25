package fixture

import (
	regex "regexp"
	alias "strings"
)

const patternPrefix = "^"

var crossFilePattern = regex.MustCompile(patternPrefix + "a+$")
var splitAlias = alias.Split
var matchAlias = crossFilePattern.MatchString
