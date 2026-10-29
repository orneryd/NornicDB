package cyphertext_test

import (
	"os/exec"
	"testing"
)

func TestNameEmissionInventory(t *testing.T) {
	output, err := exec.Command("python3", "../testutil/cyphernames/inventory.py").CombinedOutput()
	if err != nil {
		t.Fatalf("inventory admission: %v\n%s", err, output)
	}
	t.Log(string(output))
}

func TestNameEmissionInventoryOmissionControls(t *testing.T) {
	// New files and new constructions in known files pass through the same
	// discovery/admission operation as production. No broken-string oracle.
	source := `import importlib.util, pathlib, json
p=pathlib.Path('../testutil/cyphernames/inventory.py')
s=importlib.util.spec_from_file_location('inventory',p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m)
root=p.resolve().parents[3]
manifest=json.loads((root/'internal/cyphertext/name_emission_inventory.json').read_text())
original=m.discover
for path,text in [('ui/src/new_emitter.ts','const statement = '+chr(96)+'MATCH (n) SET n.${key} = $value RETURN n'+chr(96)),('pkg/cypher/result_columns_projection.go','var newGlobal = fmt.Sprintf("RETURN %s AS %s", name, alias)'),('scripts/new_emitter.py','QUERY = f"CREATE (n:{label})"'),('pkg/new_template.go','var t = template.Must(template.New("q").Parse("MATCH (n:{{.Label}}) RETURN n"))'),('scripts/new_emitter.mjs',"export const relationshipText = name => '[:' + name + ']';"),('ui/src/fragment.ts',"const keyText = name => ':' + name;"),('scripts/fragment.py','def quoted(name): return '+chr(34)+chr(96)+chr(34)+' + name')]:
    def discovery(r):
        found=original(r); found.setdefault(path,{}).update(m.candidates(path,text)); return found
    m.discover=discovery
    errors=m.validate(root,manifest)
    assert any(path in e and 'unadmitted construction' in e for e in errors),(path,errors)
print('omitted UI/key, same-file global alias, Python label and Go template all refused')
`
	output, err := exec.Command("python3", "-c", source).CombinedOutput()
	if err != nil {
		t.Fatalf("omission controls: %v\n%s", err, output)
	}
}
