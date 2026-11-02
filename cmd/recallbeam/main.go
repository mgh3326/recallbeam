// Command recallbeam finds fleet context again: it owns a rebuildable search
// index over documents, tasks and decisions kept elsewhere, and answers
// search requests over HTTP and MCP.
package main

import (
	"fmt"
	"os"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		fmt.Println("recallbeam", version)
		return
	}
	fmt.Fprintln(os.Stderr, "usage: recallbeam version")
	os.Exit(2)
}
