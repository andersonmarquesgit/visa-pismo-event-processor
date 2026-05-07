package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed migrations/001_processed_events.sql
var processedEventsMigration string

// EnsureSchema aplica o DDL idempotente (IF NOT EXISTS). Útil quando o volume do
// Postgres já existia sem ter passado pelos scripts de /docker-entrypoint-initdb.d/.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range splitSQLStatements(processedEventsMigration) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

func splitSQLStatements(script string) []string {
	var out []string
	for _, part := range strings.Split(script, ";") {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		out = append(out, s+";")
	}
	return out
}
