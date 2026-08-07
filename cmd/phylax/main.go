// main.go — phylax CLI: a thin wrapper around the phylax.CDC API, using
// only the standard library.
//
// It parses flags into a phylax.Config, registers a change consumer (webhook
// POST with retry, or stdout printing), and runs the CDC client until
// SIGINT/SIGTERM triggers a graceful shutdown.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"phylax"
)

// options holds every CLI flag.
type options struct {
	dsn         string
	tables      string
	webhook     string
	slot        string
	publication string
	verbose     bool
}

func main() {
	opts := parseFlags()
	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "phylax:", err)
		os.Exit(1)
	}
}

// parseFlags reads the command line into options and enforces the required
// flags. Missing required flags print usage and exit with status 2.
func parseFlags() *options {
	opts := &options{}

	flag.StringVar(&opts.dsn, "dsn", "", "libpq connection string (required)")
	flag.StringVar(&opts.tables, "tables", "", "comma-separated tables to replicate (required)")
	flag.StringVar(&opts.webhook, "webhook", "", "URL to POST each change to (optional)")
	flag.StringVar(&opts.slot, "slot", "", "replication slot name (default: phylax.Config default)")
	flag.StringVar(&opts.publication, "publication", "", "publication name (default: phylax.Config default)")
	flag.BoolVar(&opts.verbose, "v", false, "enable debug-level logging")

	flag.Parse()

	if opts.dsn == "" || opts.tables == "" {
		flag.Usage()
		os.Exit(2)
	}
	return opts
}

// run builds the CDC config from flags, registers the change consumer, and
// starts replication, shutting down gracefully on SIGINT/SIGTERM.
func run(opts *options) error {
	level := slog.LevelInfo
	if opts.verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	cfg := phylax.Config{
		DSN:             opts.dsn,
		Tables:          splitTables(opts.tables),
		SlotName:        opts.slot,
		PublicationName: opts.publication,
	}

	cdc, err := phylax.New(cfg)
	if err != nil {
		return err
	}

	if opts.webhook != "" {
		cdc.OnChange(webhookChangeHandler(opts.webhook))
	} else {
		cdc.OnChange(stdoutHandler)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return cdc.Start(ctx)
}

// splitTables parses a comma-separated flag value into a table list,
// trimming whitespace and skipping empty entries.
func splitTables(s string) []string {
	parts := strings.Split(s, ",")
	tables := make([]string, 0, len(parts))
	for _, part := range parts {
		if t := strings.TrimSpace(part); t != "" {
			tables = append(tables, t)
		}
	}
	return tables
}

// stdoutHandler prints each change as a JSON line to stdout.
func stdoutHandler(change *phylax.Change) {
	line, err := json.Marshal(change)
	if err != nil {
		slog.Error("printing change", "error", err)
		return
	}
	fmt.Println(string(line))
}

// webhookConcurrency caps how many webhook POSTs may be in flight at once.
const webhookConcurrency = 5

// webhookChangeHandler returns an OnChange callback that delivers each
// change as a JSON POST to the webhook URL. Delivery runs in a goroutine
// per change, bounded by a semaphore of webhookConcurrency: when all slots
// are busy the callback blocks (backpressure), so a slow webhook queues
// changes in the subscriber channel instead of spawning unbounded
// goroutines.
func webhookChangeHandler(url string) func(*phylax.Change) {
	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, webhookConcurrency)

	return func(change *phylax.Change) {
		sem <- struct{}{} // acquire; blocks while webhookConcurrency POSTs are in flight

		go func() {
			defer func() { <-sem }() // release
			postWithRetry(client, url, change)
		}()
	}
}

// postWithRetry delivers one change, retrying up to three times with a
// 1s/2s/3s backoff on request errors or non-2xx responses. After three
// failures it logs the error and drops the change.
func postWithRetry(client *http.Client, url string, change *phylax.Change) {
	body, err := json.Marshal(change)
	if err != nil {
		slog.Error("webhook: marshaling change", "error", err)
		return
	}

	for attempt := 1; attempt <= 3; attempt++ {
		if err := postJSON(client, url, body); err == nil {
			return
		} else {
			slog.Warn("webhook: delivery attempt failed", "attempt", attempt, "error", err)
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	slog.Error("webhook: giving up on change after 3 attempts",
		"table", change.Table,
		"operation", change.Operation,
	)
}

// postJSON sends one POST and returns an error on transport failure or a
// non-2xx response.
func postJSON(client *http.Client, url string, body []byte) error {
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
}
