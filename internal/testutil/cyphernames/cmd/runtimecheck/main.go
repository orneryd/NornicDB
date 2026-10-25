// Command runtimecheck runs the supported-runtime subjects on the lab worker.
package main

import (
	"os"
	"os/exec"
)

func main() {
	args := append([]string{"internal/testutil/cyphernames/runtime_matrix.py"}, os.Args[1:]...)
	cmd := exec.Command("python3", args...)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = os.Stdout, os.Stderr, os.Stdin
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}
