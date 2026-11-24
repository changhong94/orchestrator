//go:build ignore

// Built explicitly as node-register. No VM/conductor initialization per slot.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "register-shared" {
		fmt.Fprintln(os.Stderr, "usage: node-register register-shared [flags]")
		os.Exit(2)
	}
	if err := registerSharedRun(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
