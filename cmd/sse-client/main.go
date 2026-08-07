// main.go — a tiny SSE client for the phylax replication client.
//
// It connects to the SSE endpoint served by cmd/phylax and prints every
// change event as it arrives, reconnecting automatically if the server is
// not up yet or goes away.

package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultURL = "http://localhost:8080/events"

func main() {
	url := defaultURL
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	log.Printf("listening to %s (Ctrl-C to stop)", url)

	ctx := context.Background()
	for {
		err := listen(ctx, url)
		if err != nil {
			log.Printf("connection lost: %v — reconnecting in 2s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// listen streams events from the SSE endpoint until the server closes the
// connection or an error occurs.
func listen(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		fmt.Println("change:", strings.TrimPrefix(line, "data: "))
	}
	return scanner.Err()
}
