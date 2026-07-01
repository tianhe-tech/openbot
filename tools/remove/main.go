//go:build ignore

// remove deletes files or directories. Used by Makefile.
// Usage: go run tools/remove/main.go [-r] <path> [path ...]
//
//	-r  remove directories recursively
package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	recursive := false
	if len(args) > 0 && args[0] == "-r" {
		recursive = true
		args = args[1:]
	}
	for _, p := range args {
		var err error
		if recursive {
			err = os.RemoveAll(p)
		} else {
			err = os.Remove(p)
			if os.IsNotExist(err) {
				err = nil // ignore missing
			}
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "remove: %v\n", err)
			os.Exit(1)
		}
	}
}
