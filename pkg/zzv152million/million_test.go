package zzv152million

// v1.5.2 verification, speed-and-memory lane, report entry 35 reproduction 1: about 1.1 million
// nodes with 475,200 64-value vectors. One job builds both servers from their commits (v1.5.1
// cb0a574f7 and the pin e8cd436e5), seeds the store once with v1.5.1, stops it, then starts each
// build on its own copy: one vector write sent as soon as the server answers (5-minute client
// timeout), then top-300 vector searches every 2 s until the log shows "HNSW index created:" and
// three searches have taken under 2 s (45-minute limit per build). Adapted from the 12 Nov probe
// pendingwarm/warm_test.go (builds b6d8e762a / 2dbc67e96); same plan, statements, seeds and limits.
// A build that does not answer on the copied store is retried once with --upgrade-storage, logged.

import (
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestV152MillionVectorBuildWait(t *testing.T) {
	work := t.TempDir()
	bin := filepath.Join(work, "bin")
	sh(t, work, "mkdir -p bin")
	builds := []struct{ name, sha string }{
		{"v151", "cb0a574f75b183dfebfba6c4726b72244f7f374b"},
		{"pin", "e8cd436e540ed421327bb72478d4d98479f7a2ac"},
	}
	for _, build := range builds {
		src := filepath.Join(work, "src-"+build.name)
		sh(t, work, "mkdir -p "+src)
		sh(t, "/repo", "git archive "+build.sha+" | tar -x -C "+src)
		shLog(t, src, "go build -tags noui,nolocalllm -o "+bin+"/"+build.name+" ./cmd/nornicdb")
	}
	orig := filepath.Join(work, "original")
	s := startServer(t, "seed-v151", bin+"/v151", orig, nil, nil)
	if _, e := s.waitReady(5 * time.Minute); e != nil {
		s.dump(true, 50)
		t.Fatal(e)
	}
	seedStart := time.Now()
	load(t, s, newPlan(1100000))
	t.Logf("PF MILLION seed done in %s", time.Since(seedStart))
	s.stop()
	sh(t, work, "du -sb "+orig)
	for _, build := range []string{"v151", "pin"} {
		dir := filepath.Join(work, "run-"+build)
		sh(t, work, "cp -a "+orig+" "+dir)
		s = startServer(t, build, bin+"/"+build, dir, nil, nil)
		if _, e := s.waitReady(8 * time.Minute); e != nil {
			s.dump(true, 80)
			s.stop()
			t.Logf("PF MILLION build=%s did not answer on the copied store (%v); retrying once with --upgrade-storage", build, e)
			sh(t, work, "rm -rf "+dir+" && cp -a "+orig+" "+dir)
			s = startServer(t, build, bin+"/"+build, dir, []string{"--upgrade-storage"}, nil)
			if _, e := s.waitReady(8 * time.Minute); e != nil {
				s.dump(true, 80)
				t.Fatal(e)
			}
		}
		r := rand.New(rand.NewSource(7))
		deadline := time.Now().Add(45 * time.Minute)
		fast := 0
		wstart := time.Now()
		wr := s.query("nornic", "CREATE (:PDSearchable {id:'pf-during-build', text:'newly written', embedding:$v})", map[string]any{"v": vec(r, dims)}, 5*time.Minute)
		t.Logf("PF startup-vector-write build=%s elapsed=%s err=%s", build, time.Since(wstart), wr.Err)
		for time.Now().Before(deadline) {
			beg := time.Now()
			res := s.query("nornic", qVec, map[string]any{"v": vec(r, dims)}, 4*time.Minute)
			d := time.Since(beg)
			s.mu.Lock()
			log := strings.Join(s.lines, "\n")
			s.mu.Unlock()
			built := strings.Contains(log, "HNSW index created:")
			t.Logf("PF MILLION build=%s since=%s query=%s built=%v rows=%d err=%s", build, time.Since(s.start), d, built, len(res.Rows), res.Err)
			if built && d < 2*time.Second && res.Err == "" {
				fast++
			}
			if fast >= 3 {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if fast < 3 {
			t.Logf("PF MILLION build=%s OBSERVATION LIMIT: three fast calls not reached in 45 minutes", build)
		}
		s.dump(false, 25)
		s.stop()
		t.Logf("PF MILLION DONE build=%s", build)
		sh(t, work, "rm -rf "+dir)
	}
}
