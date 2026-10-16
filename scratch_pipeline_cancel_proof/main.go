package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		panic(err)
	}
	status, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output()
	if err != nil {
		panic(err)
	}
	identity := strings.TrimSpace(string(head))
	if len(status) != 0 {
		identity += "+captured-overlay"
	}
	os.Setenv("PIPELINE_SOURCE_HEAD", identity)
	os.Setenv("GOMAXPROCS", "2")
	os.Setenv("GOMEMLIMIT", "6GiB")
	fmt.Printf("PROOF_SOURCE=%s parser=%s runtime_limits=GOMAXPROCS2,GOMEMLIMIT6GiB_soft hard_lab_budget_pending\n", identity, os.Getenv("NORNICDB_PARSER"))
	command := exec.Command("go", os.Args[1:]...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		fmt.Printf("PROOF_FAILURE=%v\n", err)
		os.Exit(1)
	}
}
