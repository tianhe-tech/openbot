//go:build ignore

// install copies src to dst (like cp -f). Used by Makefile.
// Usage: go run tools/install/main.go <src> <dst>
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: install <src> <dst>")
		os.Exit(1)
	}
	src, dst := os.Args[1], os.Args[2]

	in, err := os.Open(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "install: open %s: %v\n", src, err)
		os.Exit(1)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "install: create %s: %v\n", dst, err)
		os.Exit(1)
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		fmt.Fprintf(os.Stderr, "install: copy: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("installed %s -> %s\n", src, dst)
}
