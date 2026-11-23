//go:build ignore

// Built explicitly with checkpoint.go. Keep checkpoint parsing and tenant keys
// in a fresh process without initializing unrelated control-plane packages.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "checkpoint-cleanup" {
		fmt.Fprintln(os.Stderr, "usage: node-checkpoint checkpoint-cleanup [args]")
		os.Exit(2)
	}
	if err := checkpointCleanupCmd(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
