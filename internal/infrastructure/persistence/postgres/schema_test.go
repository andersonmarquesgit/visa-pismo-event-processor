package postgres

import "testing"

func TestSplitSQLStatements(t *testing.T) {
	t.Parallel()

	stmts := splitSQLStatements(`
CREATE TABLE a(id text);

;
CREATE TABLE b(id text);
`)

	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}
	if stmts[0] != "CREATE TABLE a(id text);" {
		t.Fatalf("unexpected stmt[0]=%q", stmts[0])
	}
	if stmts[1] != "CREATE TABLE b(id text);" {
		t.Fatalf("unexpected stmt[1]=%q", stmts[1])
	}
}

