//go:build ignore

// runenv loads a .env file into the process environment then exec's a binary.
// Usage: go run tools/runenv/main.go <envfile> <binary> [args...]
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: runenv <envfile> <binary> [args...]")
		os.Exit(1)
	}
	envFile, binary := os.Args[1], os.Args[2]
	extraArgs := os.Args[3:]

	f, err := os.Open(envFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runenv: cannot open %s: %v\n", envFile, err)
		os.Exit(1)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		// strip surrounding quotes if present
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		os.Setenv(key, val)
	}

	cmd := exec.Command(binary, extraArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "runenv: %v\n", err)
		os.Exit(1)
	}
}
