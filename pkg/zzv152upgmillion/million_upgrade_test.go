package zzv152upgmillion

// v1.5.2 verification, upgrade lane, report entry 35 reproduction 1 with a store written by
// b6d8e762a (HEAVY; run only after the speed-and-memory lane's 1.1-million-node job has ended).
// One job builds the stock nornicdb servers of b6d8e762a and of this checkout (the pin), seeds about
// 1.1 million nodes with 475,200 64-value vectors once with b6d8e762a over HTTP (the plan,
// statements and seed of the report, helpers_test.go copied from the speed-and-memory lane's
// ff20534c9), stops it with SIGTERM, then for b6d8e762a and then the pin starts the build on its own
// copy (the pin with --upgrade-storage, as the report ran 2dbc67e96), sends one vector write as soon
// as the server answers (5-minute client timeout), then top-300 vector searches every 2 s until the
// log shows "HNSW index created:" and three searches have taken under 2 s (45-minute limit), and
// finally counts whether the write was stored.

import (
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestV152UpgradeMillionVectorBuildWait(t *testing.T) {
	work := t.TempDir()
	bin := filepath.Join(work, "bin")
	sh(t, work, "mkdir -p bin")
	builds := []struct{ name, sha string }{
		{"b6", "b6d8e762ab9a9bd1e954aca5dd4f4257805fa8c2"},
		{"pin", "e8cd436e540ed421327bb72478d4d98479f7a2ac"},
	}
	for _, build := range builds {
		src := filepath.Join(work, "src-"+build.name)
		sh(t, work, "mkdir -p "+src)
		sh(t, "/repo", "git archive "+build.sha+" | tar -x -C "+src)
		shLog(t, src, "go build -tags noui,nolocalllm -o "+bin+"/"+build.name+" ./cmd/nornicdb")
	}
	orig := filepath.Join(work, "original")
	s := startServer(t, "seed-b6", bin+"/b6", orig, nil, nil)
	if _, e := s.waitReady(5 * time.Minute); e != nil {
		s.dump(true, 50)
		t.Fatal(e)
	}
	seedStart := time.Now()
	load(t, s, newPlan(1100000))
	expected := counts(s)
	t.Logf("UPM seed done in %s counts=%v", time.Since(seedStart), expected)
	s.stop()
	sh(t, work, "du -sb "+orig)
	for _, build := range []string{"b6", "pin"} {
		dir := filepath.Join(work, "run-"+build)
		sh(t, work, "cp -a "+orig+" "+dir)
		var args []string
		if build == "pin" {
			args = []string{"--upgrade-storage"}
		}
		s = startServer(t, build, bin+"/"+build, dir, args, nil)
		ready, e := s.waitReady(15 * time.Minute)
		if e != nil {
			s.dump(true, 80)
			t.Fatalf("UPM build=%s did not answer: %v", build, e)
		}
		t.Logf("UPM build=%s ready=%s", build, ready)
		r := rand.New(rand.NewSource(7))
		wstart := time.Now()
		wr := s.query("nornic", "CREATE (:PDSearchable {id: 'during-build', text: 'newly written', embedding: $v})", map[string]any{"v": vec(r, dims)}, 5*time.Minute)
		t.Logf("UPM startup-vector-write build=%s elapsed=%s err=%s", build, time.Since(wstart), wr.Err)
		deadline := time.Now().Add(45 * time.Minute)
		fast := 0
		for time.Now().Before(deadline) {
			beg := time.Now()
			res := s.query("nornic", qVec, map[string]any{"v": vec(r, dims)}, 4*time.Minute)
			d := time.Since(beg)
			s.mu.Lock()
			log := strings.Join(s.lines, "\n")
			s.mu.Unlock()
			built := strings.Contains(log, "HNSW index created:")
			t.Logf("UPM build=%s since=%s query=%s built=%v rows=%d err=%s", build, time.Since(s.start), d, built, len(res.Rows), res.Err)
			if built && d < 2*time.Second && res.Err == "" {
				fast++
			}
			if fast >= 3 {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if fast < 3 {
			t.Logf("UPM build=%s OBSERVATION LIMIT: three fast calls not reached in 45 minutes", build)
		}
		stored := s.query("nornic", "MATCH (n:PDSearchable {id: 'during-build'}) RETURN count(n) AS n", nil, 5*time.Minute)
		t.Logf("UPM build=%s write-stored rows=%v err=%s", build, stored.Rows, stored.Err)
		got := counts(s)
		t.Logf("UPM build=%s counts=%v seed=%v", build, got, expected)
		s.dump(false, 25)
		s.stop()
		t.Logf("UPM DONE build=%s", build)
		sh(t, work, "rm -rf "+dir)
	}
}
