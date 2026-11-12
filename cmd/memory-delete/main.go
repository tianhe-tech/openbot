// Command memory-delete removes one principal's archived conversation data.
//
// This is the operator entry point for scoped deletion. It exists as a separate
// binary rather than as a gateway endpoint or an MCP tool for two reasons.
//
// The MCP server opens the archive read-only, deliberately: it is the process a
// model talks to, and a model must not be able to delete a user's history
// because a prompt asked it to. Adding deletion there would mean the read-only
// guarantee is conditional on a tool handler behaving, which is not a guarantee.
//
// The gateway is a long-running capture process. Deletion is a rare, deliberate,
// operator-initiated act, and putting it behind a flag on a process that is
// already holding the writer lease would make it possible to trigger by
// restarting with the wrong environment. A separate command that has to be run
// on purpose is the right amount of friction.
//
// The command is not a secure erase. SQLite DELETE removes rows from the
// b-tree, but the bytes can survive in free pages, in the WAL, and in any backup
// taken before the deletion. Physical erasure follows the storage and backup
// policy, not this program. That is stated here because an operator who believes
// otherwise will make a promise to a user that the software cannot keep.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("memory-delete", flag.ContinueOnError)
	path := flags.String("db", "", "gateway database path (required)")
	adapter := flags.String("adapter", "", "adapter that owns the data, e.g. dingtalk (required)")
	user := flags.String("user", "", "user ID whose archive is deleted (required)")
	session := flags.String("session", "", "delete only this archive session ID")
	project := flags.String("project", "", "delete only sessions archived under this project")
	since := flags.Int64("since", 0, "delete only messages created at or after this Unix millisecond")
	until := flags.Int64("until", 0, "delete only messages created at or before this Unix millisecond")
	confirm := flags.Bool("confirm", false, "required: acknowledge that this cannot be undone")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("--db is required; positional arguments are not accepted")
	}
	if *adapter == "" || *user == "" {
		return fmt.Errorf("--adapter and --user are required; deletion is never archive-wide")
	}
	if *since < 0 || *until < 0 {
		return fmt.Errorf("--since and --until must not be negative")
	}
	if *since > 0 && *until > 0 && *since > *until {
		return fmt.Errorf("--since must not be later than --until")
	}
	// The confirmation flag is not ceremony. Every other guard here is a
	// predicate that narrows the selection, and a predicate that is accidentally
	// empty selects everything: --adapter and --user with no session, project,
	// or time bound deletes the principal's entire history. Requiring an
	// explicit acknowledgement is the only guard that cannot be satisfied by
	// forgetting to pass a filter.
	if !*confirm {
		return fmt.Errorf("refusing to delete without --confirm; this removes archived conversation data and cannot be undone")
	}

	writer, err := memstore.OpenArchiveWriter(ctx, *path, memstore.WriterOptions{})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		writer.Close(shutdown)
	}()

	report, err := writer.DeleteScope(ctx, memstore.DeleteOptions{
		Scope: memstore.SearchScope{
			Adapter: *adapter, UserID: *user, Project: *project,
			Since: *since, Until: *until,
		},
		SessionID: *session,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
