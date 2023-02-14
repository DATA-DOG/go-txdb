// +build go1.8

package txdb

import (
	"context"
	"database/sql"
	"sort"
	"strings"
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

func TestShouldHandleStmtsWithoutContextPollution(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		t.Run(driver, func(t *testing.T) {
			db, err := sql.Open(driver, "contextpollution")
			if err != nil {
				t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
			}
			defer db.Close()

			insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
			if strings.Index(driver, "psql_") == 0 {
				insertSQL = "INSERT INTO users (username, email) VALUES($1, $2)"
			}

			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()

			_, err = db.ExecContext(ctx1, insertSQL, "first", "first@foo.com")
			if err != nil {
				t.Fatalf("unexpected error inserting user 1: %s", err)
			}
			cancel1()

			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()

			_, err = db.ExecContext(ctx2, insertSQL, "second", "second@foo.com")
			if err != nil {
				t.Fatalf("unexpected error inserting user 2: %s", err)
			}
			cancel2()

			const selectQuery = `
select username
from users
where username = 'first' OR username = 'second'`

			rows, err := db.QueryContext(context.Background(), selectQuery)
			if err != nil {
				t.Fatalf("unexpected error querying users: %s", err)
			}
			defer rows.Close()

			assertRows := func(t *testing.T, rows *sql.Rows) {
				t.Helper()

				var users []string
				for rows.Next() {
					var user string
					err := rows.Scan(&user)
					if err != nil {
						t.Errorf("unexpected scan failure: %s", err)
						continue
					}
					users = append(users, user)
				}
				sort.Strings(users)

				wanted := []string{"first", "second"}

				if len(users) != 2 {
					t.Fatalf("invalid users received; want=%v\tgot=%v", wanted, users)
				}
				for i, want := range wanted {
					if got := users[i]; want != got {
						t.Errorf("invalid user; want=%s\tgot=%s", want, got)
					}
				}
			}

			assertRows(t, rows)

			ctx3, cancel3 := context.WithCancel(context.Background())
			defer cancel3()

			stmt, err := db.PrepareContext(ctx3, selectQuery)
			if err != nil {
				t.Fatalf("unexpected error preparing stmt: %s", err)
			}

			rows, err = stmt.QueryContext(context.TODO())
			if err != nil {
				t.Fatalf("unexpected error in stmt querying users: %s", err)
			}
			defer rows.Close()

			assertRows(t, rows)
		})
	}
}
