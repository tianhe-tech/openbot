//go:build ignore

// buildinfo prints build metadata for use in Makefile.
// Usage: go run tools/buildinfo/main.go [version|commit|date|all]
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func gitOutput(args ...string) string {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func main() {
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	version := gitOutput("describe", "--tags", "--always", "--dirty")
	if version == "" {
		version = "dev"
	}
	commit := gitOutput("rev-parse", "--short", "HEAD")
	if commit == "" {
		commit = "unknown"
	}
	buildTime := time.Now().UTC().Format("2006-01-02T15:04:05Z")

	switch mode {
	case "version":
		fmt.Print(version)
	case "commit":
		fmt.Print(commit)
	case "date":
		fmt.Print(buildTime)
	default: // "all" — print makefile-compatible assignments
		fmt.Printf("VERSION=%s\nCOMMIT=%s\nBUILD_TS=%s\n", version, commit, buildTime)
	}
}
