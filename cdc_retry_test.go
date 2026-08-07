package phylax

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"dial refused", &net.OpError{Op: "dial", Err: errors.New("connect: connection refused")}, true},
		{"connection reset", errors.New("read tcp 127.0.0.1:5432: connection reset by peer"), true},
		{"pgconn connect error", &pgconn.ConnectError{Config: &pgconn.Config{}}, true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"crash shutdown", &pgconn.PgError{Code: "57P02"}, true},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"connection exception", &pgconn.PgError{Code: "08006"}, true},
		{"undefined table", &pgconn.PgError{Code: "42P01"}, false},
		{"invalid password", &pgconn.PgError{Code: "28P01"}, false},
		{"invalid catalog", &pgconn.PgError{Code: "3D000"}, false},
		{"undefined object", &pgconn.PgError{Code: "42704"}, false},
		{"wrapped permanent", fmt.Errorf("setup: %w", &pgconn.PgError{Code: "42P01"}), false},
		{"wrapped transient", fmt.Errorf("stream: %w", &pgconn.PgError{Code: "57P01"}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryable(tt.err); got != tt.want {
				t.Errorf("retryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
