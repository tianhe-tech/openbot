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
	flags := flag.NewFlagSet("memory-migrate", flag.ContinueOnError)
	path := flags.String("db", "", "existing gateway database path (required)")
	backup := flags.String("backup", "", "new consistent backup destination (required unless --resume)")
	resume := flags.Bool("resume", false, "resume an already initialized archive without making another backup")
	batchSize := flags.Int("batch-size", 64, "legacy records per transaction (1-1000)")
	queueBytes := flags.Int("queue-bytes", 16*1024*1024, "maximum accepted in-flight bytes; oversized legacy rows fail explicitly")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("--db is required; positional arguments are not accepted")
	}
	if *batchSize < 1 || *batchSize > 1000 {
		return fmt.Errorf("--batch-size must be 1-1000")
	}
	if *queueBytes <= 0 {
		return fmt.Errorf("--queue-bytes must be positive")
	}
	if *resume && *backup != "" {
		return fmt.Errorf("use --backup for initialization or --resume, not both")
	}
	if !*resume {
		if err := memstore.PrepareArchive(ctx, *path, *backup); err != nil {
			return err
		}
	}
	writer, err := memstore.OpenArchiveWriter(ctx, *path, memstore.WriterOptions{QueueBytes: *queueBytes})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		writer.Close(shutdown)
	}()
	encoder := json.NewEncoder(output)
	for {
		progress, err := writer.ImportLegacyBatch(ctx, *batchSize)
		if err != nil {
			return err
		}
		if err := encoder.Encode(progress); err != nil {
			return err
		}
		if progress.Pending == 0 {
			if progress.Unmapped > 0 {
				return fmt.Errorf("import incomplete: %d records have no adapter/user identity; legacy originals are unchanged", progress.Unmapped)
			}
			return writer.Close(ctx)
		}
	}
}
