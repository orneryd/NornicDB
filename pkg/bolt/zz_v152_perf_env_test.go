package bolt

// v1.5.2 verification, speed-and-memory lane: one line per test process with
// the host facts that affect timing (CPUs the process may use, GOMAXPROCS,
// load average, available memory). Byte-identical on the pin and v1.5.1.

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

func init() {
	read := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			return "?"
		}
		return strings.TrimSpace(string(b))
	}
	field := func(text, key string) string {
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, key) {
				return strings.Join(strings.Fields(strings.TrimPrefix(line, key)), " ")
			}
		}
		return "?"
	}
	status := read("/proc/self/status")
	meminfo := read("/proc/meminfo")
	host, _ := os.Hostname()
	fmt.Fprintf(os.Stderr, "V152PERFENV host=%s cpus=%s gomaxprocs=%d numcpu=%d loadavg=%q memavailable=%q go=%s tmpdir=%s gotmpdir=%s\n",
		host, field(status, "Cpus_allowed_list:"), runtime.GOMAXPROCS(0), runtime.NumCPU(),
		read("/proc/loadavg"), field(meminfo, "MemAvailable:"), runtime.Version(), os.Getenv("TMPDIR"), os.Getenv("GOTMPDIR"))
}
