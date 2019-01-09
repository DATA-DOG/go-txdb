// +build go1.8

package txdb

import (
	"context"
	"database/sql"
	"testing"
)

func TestShouldGetMultiRowSet(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "multiRows")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		rows, err := db.QueryContext(context.Background(), "SELECT username FROM users; SELECT COUNT(*) FROM users;")
		if err != nil {
			t.Fatalf(driver+": failed to query users: %s", err)
		}
		defer rows.Close()

		var users []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf(driver+": unexpected row scan err: %v", err)
			}
			users = append(users, name)
		}

		if !rows.NextResultSet() {
			t.Fatal(driver + ": expected next result set")
		}

		if !rows.Next() {
			t.Fatal(driver + ": expected next result set - row")
		}

		var count int
		if err := rows.Scan(&count); err != nil {
			t.Fatalf(driver+": unexpected row scan err: %v", err)
		}

		if count != len(users) {
			t.Fatal(driver + ": unexpected number of users")
		}
	}
}

func TestShouldBeAbleToPingWithContext(t *testing.T) {
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "ping")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf(driver+": %v", err)
		}
	}
}
