"""Ownership proof contracts, including cases that must remain findings."""
import json
from pathlib import Path
import re
import tempfile
import unittest

import duplicate_ownership as own
import pr_duplicates as pd


SOURCE = '''package example
import "testing"
type resource struct { value int }
func owner(x int) int {
 total := 0
 for i:=0;i<x;i++ { total+=i*i }
 if total>100 { total-=10 }
 return total
}
func direct(x int) int { value:=owner(x); return value+2 }
func shadow(owner func(int) int,x int) int { value:=owner(x); return value+2 }
func wrapper(x int) int { return owner(x) }
func copied(x int) int {
 total := 0
 for i:=0;i<x;i++ { total+=i*i }
 if total>100 { total-=10 }
 owner(x)
 return total
}
func setup(t testing.TB) (*resource, int) { t.Helper(); return &resource{}, 3 }
func run(t testing.TB,r *resource,count int) { t.Helper(); for i:=0;i<count;i++ { r.value+=i } }
func (r *resource) Query() { r.value++ }
func TestShared(t *testing.T) {
 r,n:=setup(t)
 run(t,r,n)
 if r.value<0 { t.Fatal("unexpected value") }
}
func BenchmarkShared(b *testing.B) {
 r,_:=setup(b)
 b.ResetTimer()
 run(b,r,b.N)
}
func TestExtraQuery(t *testing.T) {
 r,n:=setup(t)
 r.Query()
 run(t,r,n)
}
func TestReassigned(t *testing.T) {
 r,n:=setup(t)
 r=&resource{}
 run(t,r,n)
}
func TestShadowedResource(t *testing.T) {
 r,n:=setup(t)
 _=r
 { r:=&resource{}; run(t,r,n) }
}
func TestSharedField(t *testing.T) {
 r,n:=setup(t)
 r.value++
 run(t,r,n)
}
func BenchmarkSharedField(b *testing.B) {
 r,_:=setup(b)
 r.value++
 b.ResetTimer()
 run(b,r,b.N)
}
func TestIndexedWrite(t *testing.T) {
 r,n:=setup(t)
 values:=map[string]int{}
 values["key"]=1
 run(t,r,n)
}
func extracted(x int) int {
 if x>5 { return x*x }
 for x<0 { x++ }
 return x+20
}
func (r *resource) lifecycle(x int) int {
 if x==0 { return r.value }
 r.value++
 result:=extracted(x)
 return result+r.value
}
'''


class OwnershipTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.root = Path(cls.directory.name)
        (cls.root / "go.mod").write_text("module example.com/ownership\ngo 1.26\n")
        (cls.root / "sample_test.go").write_text(SOURCE)
        nodes = []
        cls.symbols = {}
        for line, text in enumerate(SOURCE.splitlines(), 1):
            matched = re.match(r"func (?:\([^)]*\) )?(\w+)\(", text)
            if matched:
                name = matched.group(1)
                node = {"id": name, "label": ("." if name in ("Query", "lifecycle") else "") + name + "()",
                        "source_file": "sample_test.go", "source_location": "L" + str(line)}
                nodes.append(node)
                cls.symbols[name] = node
        cls.graph = cls.root / "graph.json"
        # Deliberately wrong inference: the shadowed callback is NOT owner().
        cls.graph.write_text(json.dumps({"nodes": nodes, "links": [{
            "source": "shadow", "target": "owner", "relation": "calls", "_origin": "ast",
            "confidence": "INFERRED", "source_file": "sample_test.go", "source_location": "L11"}]}))
        cls.context = own.GoOwnership(cls.root, cls.graph, [cls.candidate("direct", SOURCE)],
                                      pd.similarity, pd.go_forwarding_wrapper)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    @classmethod
    def candidate(cls, name, body=None):
        node = cls.symbols[name]
        if body is None:
            body = cls.context.body(name)
        return pd.Candidate(name, node["label"], "sample_test.go",
                            int(node["source_location"][1:]), body, "", 10, 1, True)

    @classmethod
    def hit(cls, name, body=None):
        node = cls.symbols[name]
        if body is None:
            body = cls.context.body(name)
        return pd.Hit(name, node["label"], "sample_test.go",
                      int(node["source_location"][1:]), "function", "example/repo", body, 1, 1, 1)

    def proof(self, candidate, hit):
        return self.context.proof(self.candidate(candidate), self.hit(hit), .8)

    def test_policy_uses_actual_bound_free_call(self):
        proof = self.proof("direct", "owner")
        self.assertIsNotNone(proof)
        self.assertEqual(proof["kind"], "current_owner_call")

    def test_shadowed_callback_and_inferred_edge_are_not_proof(self):
        self.assertIsNone(self.proof("shadow", "owner"))
        self.assertNotIn("owner", self.context.calls.get("shadow", {}))

    def test_forwarding_wrapper_still_requires_caller_migration(self):
        self.assertIsNone(self.proof("wrapper", "owner"))

    def test_copied_body_with_dummy_owner_call_is_not_proof(self):
        self.assertGreaterEqual(pd.similarity(self.context.body("copied"), self.context.body("owner"))["score"], .8)
        self.assertIsNone(self.proof("copied", "owner"))

    def test_current_surviving_caller_proves_extracted_owner(self):
        proof = self.proof("extracted", "lifecycle")
        self.assertIsNotNone(proof)
        self.assertEqual(proof["kind"], "extracted_owner")

    def test_test_and_benchmark_share_constructed_resource_and_execution(self):
        for left, right in (("TestShared", "BenchmarkShared"), ("BenchmarkShared", "TestShared")):
            with self.subTest(direction=left):
                proof = self.proof(left, right)
                self.assertIsNotNone(proof)
                self.assertEqual(proof["kind"], "shared_test_execution")
                self.assertEqual([node["id"] for node in proof["owners"]], ["setup", "run"])

    def test_shared_dummy_helpers_do_not_hide_direct_product_execution(self):
        self.assertIsNone(self.proof("TestExtraQuery", "BenchmarkShared"))

    def test_reassignment_and_shadowing_do_not_prove_resource_flow(self):
        for name in ("TestReassigned", "TestShadowedResource"):
            with self.subTest(name=name):
                self.assertIsNone(self.proof(name, "BenchmarkShared"))

    def test_shared_dummy_helpers_do_not_hide_repeated_or_unresolved_state_writes(self):
        self.assertIsNone(self.proof("TestSharedField", "BenchmarkSharedField"))
        self.assertIsNone(self.proof("TestIndexedWrite", "BenchmarkShared"))

    def test_only_matching_pair_is_proven_and_report_keeps_evidence(self):
        candidate, hit = self.candidate("direct"), self.hit("owner")
        records = []
        finding, moved = pd.evaluate(candidate, [hit], repo="example/repo", retired=set(),
            min_similarity=.8, ownership_context=self.context, ownership_proofs=records)
        self.assertIsNone(finding)
        self.assertFalse(moved)
        report = pd.Report("example/repo", "head", "base", ownership_proofs=records)
        self.assertEqual(pd.report_json(report)["ownership_proofs"][0]["match"]["id"], "owner")
        self.assertIn("Verified current-source ownership", pd.render_summary(report))


if __name__ == "__main__":
    unittest.main()
