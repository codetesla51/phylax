// main.go — phylax CLI: a thin wrapper around the phylax.CDC API.
//
// It parses flags into a phylax.Config, registers a change consumer (webhook
// POST with retry, or stdout printing), and runs the CDC client until
// SIGINT/SIGTERM triggers a graceful shutdown.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"phylax"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

// options holds every CLI flag.
type options struct {
	dsn         string
	tables      string
	webhook     string
	slot        string
	publication string
	verbose     bool
}

func newRootCmd() *cobra.Command {
	opts := &options{}

	cmd := &cobra.Command{
		Use:   "phylax",
		Short: "Replicate PostgreSQL changes to a webhook or stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(opts)
		},
		// Print errors without dumping usage on runtime failures; flag
		// validation errors still show usage.
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&opts.dsn, "dsn", "", "libpq connection string (required)")
	cmd.Flags().StringVar(&opts.tables, "tables", "", "comma-separated tables to replicate (required)")
	cmd.Flags().StringVar(&opts.webhook, "webhook", "", "URL to POST each change to (optional)")
	cmd.Flags().StringVar(&opts.slot, "slot", "", "replication slot name (default: phylax.Config default)")
	cmd.Flags().StringVar(&opts.publication, "publication", "", "publication name (default: phylax.Config default)")
	cmd.Flags().BoolVarP(&opts.verbose, "verbose", "v", false, "enable debug-level logging")

	_ = cmd.MarkFlagRequired("dsn")
	_ = cmd.MarkFlagRequired("tables")

	return cmd
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
		cdc.OnChange(func(change *phylax.Change) {
			line, err := json.Marshal(change)
			if err != nil {
				slog.Error("printing change", "error", err)
				return
			}
			fmt.Println(string(line))
		})
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

// webhookChangeHandler returns an OnChange callback that POSTs each change
// as JSON to the webhook URL, retrying up to three times with a 1s/2s/3s
// backoff on request errors or non-2xx responses. After three failures it
// logs the error and drops the change.
func webhookChangeHandler(url string) func(*phylax.Change) {
	client := &http.Client{Timeout: 10 * time.Second}

	return func(change *phylax.Change) {
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
