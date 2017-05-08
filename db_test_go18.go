// +build go1.8

package txdb

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestShouldGetMultiRowSet(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("txdb", "multiRows")
	if err != nil {
		t.Fatalf("failed to open a mysql connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT username FROM users; SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("failed to query users: %s", err)
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("unexpected row scan err: %v", err)
		}
		users = append(users, name)
	}

	if !rows.NextResultSet() {
		t.Fatal("expected next result set")
	}

	if !rows.Next() {
		t.Fatal("expected next result set - row")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("unexpected row scan err: %v", err)
	}

	if count != len(users) {
		t.Fatal("unexpected number of users")
	}
}
