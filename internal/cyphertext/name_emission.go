package cyphertext

// QuoteName is the decoded-name adapter for raw Cypher source. PrintIdentifier
// owns quotation and Unicode source emission; roles are lab observations only.
func QuoteName(name string, roles ...string) string {
	recordNameEmission(roles, "decoded")
	return PrintIdentifier(name, nil, RawSource)
}

// NameText retains the caller's plain-name policy and delegates source emission.
func NameText(name string, roles ...string) string {
	recordNameEmission(roles, "decoded")
	return PrintIdentifier(name, plainEmissionName, RawSource)
}

func plainEmissionName(name string) bool {
	if !IsIdentifier(name) {
		return false
	}
	for index, character := range name {
		if index == 0 && !ValidNameStart(character) || index > 0 && !ValidNamePart(character) {
			return false
		}
	}
	return true
}
