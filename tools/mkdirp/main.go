//go:build ignore

// mkdirp creates directories (like mkdir -p). Used by Makefile.
// Usage: go run tools/mkdirp/main.go <dir1> [dir2 ...]
package main

import (
	"fmt"
	"os"
)

func main() {
	for _, dir := range os.Args[1:] {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdirp: %v\n", err)
			os.Exit(1)
		}
	}
}
