package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const maxRetryConnection = 5

func NewDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	var lastErr error
	for retries := 0; retries < maxRetryConnection; retries++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()

		if lastErr == nil {
			return db, nil
		}

		// Simple backoff to survive docker-compose startup races.
		backoff := time.Duration((retries+1)*(retries+1)) * time.Second
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			_ = db.Close()
			return nil, fmt.Errorf("postgres ping: %w", ctx.Err())
		}
	}

	_ = db.Close()
	return nil, fmt.Errorf("postgres ping: %w", lastErr)
}
