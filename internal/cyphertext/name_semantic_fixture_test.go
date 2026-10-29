package cyphertext

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"
)

func TestNameSemanticFixtures(t *testing.T) {
	data, err := os.ReadFile("../testutil/cyphernames/semantic_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct{ Name, Quoted string }
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		if got := QuoteName(fixture.Name); got != fixture.Quoted {
			t.Errorf("%q: %q", fixture.Name, got)
		}
		for _, text := range []string{fixture.Quoted, NameText(fixture.Name)} {
			token, ok := ScanIdentifier(text, 0)
			name, end := token.Value, token.End
			if !ok || name != fixture.Name || end != len(text) {
				t.Errorf("%q -> %q -> %q/%d/%v", fixture.Name, text, name, end, ok)
			}
		}
	}
	for _, peer := range []struct{ runtime, program string }{
		{"node", `const fs=require('node:fs'); (async()=>{const {quoteName}=await import('../../ui/src/utils/cypherNames.mjs');for(const f of JSON.parse(fs.readFileSync('../testutil/cyphernames/semantic_fixtures.json','utf8')))if(quoteName(f.name)!==f.quoted)throw Error(f.name);})().catch(e=>{console.error(e);process.exitCode=1});`},
		{"python3", `import sys,json;sys.path.insert(0,'../../scripts/migration');from cypher_names import quote_name
for f in json.load(open('../testutil/cyphernames/semantic_fixtures.json')): assert quote_name(f['name'])==f['quoted'],f
`},
	} {
		flag := "-e"
		if peer.runtime == "python3" {
			flag = "-c"
		}
		output, err := exec.Command(peer.runtime, flag, peer.program).CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", peer.runtime, err, output)
		}
	}
}
