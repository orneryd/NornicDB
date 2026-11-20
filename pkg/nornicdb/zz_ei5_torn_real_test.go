package nornicdb_test

// Earlier-issues batch 5b (untracked probe; never committed): the oversized
// embedding-audit record torn by a real kill. A child process (this build)
// opens a store with embeddings on, creates one document whose embedding has
// EI5R_CHUNKS chunks x 1,024 values, and the embedding worker writes the
// oversized audit record to wal/wal.log. The parent watches the size of
// wal.log and sends SIGKILL as soon as the file has grown well into that
// record (> 1 MiB past its size before the document). After the kill the
// journal is parsed frame by frame: the store is kept only when the file ends
// INSIDE the oversized frame. Each attempt's outcome is printed; kept stores
// go to /work/tnrun/ei5-20261113/torn-real/attempt-N/data, and this build
// reopens a copy of each.

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

const ei5RRoot = "/work/tnrun/v152-upg/b6/torn-real"

func TestEi5TornRealChild(t *testing.T) {
	data := os.Getenv("EI5_RCHILD")
	if data == "" {
		t.Skip("child only")
	}
	chunks, _ := strconv.Atoi(os.Getenv("EI5R_CHUNKS"))
	if chunks <= 0 {
		chunks = 20000 // v152 upgrade lane: the report's real-kill size
	}
	db, err := nornicdb.Open(data, ei5Config(true))
	require.NoError(t, err)
	db.SetEmbedder(&ei5Provider{chunks: chunks, dims: 1024})
	err = upgEnsureSearch(db)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(context.Background(), "CREATE (:Warm {k: 1})", nil)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	st, err := os.Stat(filepath.Join(data, "wal", "wal.log"))
	require.NoError(t, err)
	fmt.Printf("EI5R_ARMED %d\n", st.Size())
	_, err = db.ExecuteCypher(context.Background(), "CREATE (:Doc {key:'d', text:'a long document'})", nil)
	require.NoError(t, err)
	fmt.Println("EI5R_DOC_CREATED")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_ = db.WaitForEmbeddings(ctx)
	cancel()
	fmt.Println("EI5R_EMBEDDED")
	select {}
}

func TestEi5TornReal(t *testing.T) {
	attempts := 20 // v152 upgrade lane: the report's 20-kill run
	if v, err := strconv.Atoi(os.Getenv("EI5R_ATTEMPTS")); err == nil && v > 0 {
		attempts = v
	}
	require.NoError(t, os.RemoveAll(ei5RRoot))
	kept := 0
	for a := 0; a < attempts; a++ {
		keep := filepath.Join(ei5RRoot, fmt.Sprintf("attempt-%02d", a))
		base := keep
		if os.Getenv("EI5R_ON_WORK") == "" {
			// v152 upgrade lane: the report's run had its stores on tmpfs (the
			// job temp dir was tmpfs on 13 Nov); today's temp dir is overlayfs,
			// so a tmpfs location is used when one with room exists.
			base = filepath.Join(ei5TmpfsDir(t), fmt.Sprintf("attempt-%02d", a))
		}
		data := filepath.Join(base, "data")
		require.NoError(t, os.MkdirAll(base, 0o755))
		fsType, _ := exec.Command("stat", "-f", "-c", "%T", base).Output()
		cmd := exec.Command(os.Args[0], "-test.run=^TestEi5TornRealChild$", "-test.v", "-test.timeout=20m")
		cmd.Env = append(os.Environ(), "EI5_RCHILD="+data)
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, cmd.Start())
		armed := make(chan int64, 1)
		go func() {
			sc := bufio.NewScanner(stdout)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for sc.Scan() {
				l := sc.Text()
				if strings.HasPrefix(l, "EI5R_ARMED ") {
					v, _ := strconv.ParseInt(strings.TrimPrefix(l, "EI5R_ARMED "), 10, 64)
					armed <- v
				}
			}
		}()
		var before int64
		select {
		case before = <-armed:
		case <-time.After(5 * time.Minute):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("attempt %d: child never armed", a)
		}
		walPath := filepath.Join(data, "wal", "wal.log")
		trigger := before + 1<<20
		var seen int64
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			if st, err := os.Stat(walPath); err == nil {
				seen = st.Size()
				if seen > trigger {
					break
				}
			}
		}
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		frames, fileLen, ferr := ei5Frames(walPath)
		where := "no oversized frame started"
		inside := false
		for i, f := range frames {
			if f.size > 16<<20 {
				if f.end > fileLen {
					inside = true
					where = fmt.Sprintf("INSIDE oversized frame %d: frame start=%d payload=%d bytes, frame end would be %d, file ends at %d (%d of %d frame bytes written)", i, f.start, f.size, f.end, fileLen, fileLen-f.start, f.end-f.start)
				} else {
					where = fmt.Sprintf("after oversized frame %d completed (frame %d..%d, file %d, %d frames)", i, f.start, f.end, fileLen, len(frames))
				}
			}
		}
		if !inside && len(frames) > 0 {
			last := frames[len(frames)-1]
			if last.end > fileLen && last.size <= 16<<20 {
				where += fmt.Sprintf("; file ends inside ordinary frame at %d", last.start)
			}
		}
		fmt.Printf("EI5R attempt=%d fs=%s chunks=%s armed_size=%d size_seen_at_kill=%d final_size=%d frames=%d parse_err=%v | %s\n", a, strings.TrimSpace(string(fsType)), os.Getenv("EI5R_CHUNKS"), before, seen, fileLen, len(frames), ferr, where)
		if !inside {
			_ = os.RemoveAll(base)
			continue
		}
		kept++
		if base != keep {
			// v152 upgrade lane: sparse copy to the lab dataset, then free the
			// tmpfs attempt (4 GiB would not hold 20 kept stores).
			require.NoError(t, os.MkdirAll(keep, 0o755))
			out, err := exec.Command("cp", "-a", "--sparse=always", data, filepath.Join(keep, "data")).CombinedOutput()
			require.NoError(t, err, string(out))
			_ = os.RemoveAll(base)
			data = filepath.Join(keep, "data")
		}
		restart := filepath.Join(keep+"-b6restart", "data")
		require.NoError(t, os.MkdirAll(filepath.Dir(restart), 0o755))
		if out, err := exec.Command("cp", "-a", "--sparse=always", data, restart).CombinedOutput(); err != nil {
			t.Fatalf("copy for the old build's restart: %v %s", err, out)
		}
		ei5OldRestart(t, fmt.Sprintf("attempt-%02d", a), restart)
		_ = os.RemoveAll(filepath.Dir(restart))
	}
	fmt.Printf("EI5R SUMMARY attempts=%d kept(inside oversized frame)=%d\n", attempts, kept)
}

// ei5TmpfsDir returns a fresh directory on tmpfs with at least 2 GiB free
// (/dev/shm, then the job temp dir), else the job temp dir; it prints the choice.
func ei5TmpfsDir(t *testing.T) string {
	for _, root := range []string{"/dev/shm", os.TempDir()} {
		fsType, _ := exec.Command("stat", "-f", "-c", "%T", root).Output()
		var st syscall.Statfs_t
		if syscall.Statfs(root, &st) != nil {
			continue
		}
		free := int64(st.Bavail) * int64(st.Bsize)
		fmt.Printf("EI5R_FS candidate=%s fs=%s free=%d\n", root, strings.TrimSpace(string(fsType)), free)
		if strings.TrimSpace(string(fsType)) == "tmpfs" && free >= 2<<30 {
			dir, err := os.MkdirTemp(root, "v152-treal-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			return dir
		}
	}
	// No tmpfs with room (20 Nov: /dev/shm is 64 MiB): the lab dataset (ZFS).
	dir := filepath.Join("/work/tnrun/v152-upg/b6/torn-real-scratch", fmt.Sprintf("%d", time.Now().UnixNano()))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	fmt.Printf("EI5R_FS chosen=%s\n", dir)
	return dir
}
