package storage

import "testing"

// The round-6 review cleared an installed `n.age >= 10` contract through a
// pointer view of the handle's revision whose struct differs only in a tag:
// Go converts between such pointers, so the view reached the installed
// memory. A handle now holds its revision by value, so no pointer view of an
// installed revision can be written: each of these planted APIs, named or
// anonymous, tagged or not, must fail to compile. (On the reviewed head they
// compiled, and the external witness committed a violating write.)
func TestConstraintContractRevisionPointerViewsDoNotCompile(t *testing.T) {
	const layout = `struct {
	name              string ` + "`review:\"tagged-view\"`" + `
	targetEntityType  string
	targetLabelOrType string
	definition        string
	entries           []constraintContractRevisionEntry
}`
	for name, source := range map[string]string{
		"tagged-named-view":     "type reviewR6TaggedRevisionView " + layout + "\n\nfunc ReviewR6TaggedView(h CompiledConstraintContract) *reviewR6TaggedRevisionView {\n\treturn (*reviewR6TaggedRevisionView)(h.revision)\n}",
		"tagged-anonymous-view": "func ReviewR6AnonymousView(h CompiledConstraintContract) *" + layout + " {\n\treturn (*" + layout + ")(h.revision)\n}",
		"untagged-named-view":   "type reviewR5RevisionView constraintContractRevision\n\nfunc ReviewR5View(h CompiledConstraintContract) *reviewR5RevisionView {\n\treturn (*reviewR5RevisionView)(h.revision)\n}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := typeCheckStorage("package storage\n\n" + source + "\n"); err == nil {
				t.Fatal("a pointer view of the installed revision compiled")
			} else {
				t.Logf("does not compile: %v", err)
			}
		})
	}
}
