//go:build ignore

// initenv copies src to dst only if dst does not exist. Used by Makefile.
// Usage: go run tools/initenv/main.go <src> <dst>
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: initenv <src> <dst>")
		os.Exit(1)
	}
	src, dst := os.Args[1], os.Args[2]

	if _, err := os.Stat(dst); err == nil {
		fmt.Printf("%s already exists, skipping.\n", dst)
		return
	}

	in, err := os.Open(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initenv: open %s: %v\n", src, err)
		os.Exit(1)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initenv: create %s: %v\n", dst, err)
		os.Exit(1)
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		fmt.Fprintf(os.Stderr, "initenv: copy: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created %s — please edit it with your credentials.\n", dst)
}
