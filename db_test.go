package txdb

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func drivers() []string {
	var all []string
	for _, d := range sql.Drivers() {
		if strings.Index(d, "_txdb") != -1 {
			all = append(all, d)
		}
	}
	return all
}

func TestShouldRunWithinTransaction(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		var count int
		db, err := sql.Open(driver, "one")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}

		_, err = db.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test.com')`)
		if err != nil {
			t.Fatalf(driver+": failed to insert an user: %s", err)
		}
		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf(driver+": failed to count users: %s", err)
		}
		if count != 4 {
			t.Fatalf(driver+": expected 4 users to be in database, but got %d", count)
		}
		db.Close()

		db, err = sql.Open(driver, "two")
		if err != nil {
			t.Fatalf(driver+": failed to reopen a mysql connection: %s", err)
		}

		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf(driver+": failed to count users: %s", err)
		}
		if count != 3 {
			t.Fatalf(driver+": expected 3 users to be in database, but got %d", count)
		}
		db.Close()
	}
}

func TestShouldNotHoldConnectionForRows(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "three")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		rows, err := db.Query("SELECT username FROM users")
		if err != nil {
			t.Fatalf(driver+": failed to query users: %s", err)
		}
		defer rows.Close()

		_, err = db.Exec(`INSERT INTO users(username, email) VALUES('txdb', 'txdb@test.com')`)
		if err != nil {
			t.Fatalf(driver+": failed to insert an user: %s", err)
		}
	}
}

func TestShouldPerformParallelActions(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "four")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		wg := &sync.WaitGroup{}
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(d *sql.DB, idx int) {
				defer wg.Done()
				rows, err := d.Query("SELECT username FROM users")
				if err != nil {
					t.Fatalf(driver+": failed to query users: %s", err)
				}
				defer rows.Close()

				insertSQL := "INSERT INTO users(username, email) VALUES(?, ?)"
				if strings.Index(driver, "psql_") == 0 {
					insertSQL = "INSERT INTO users(username, email) VALUES($1, $2)"
				}
				username := fmt.Sprintf("parallel%d", idx)
				email := fmt.Sprintf("parallel%d@test.com", idx)
				_, err = d.Exec(insertSQL, username, email)
				if err != nil {
					t.Fatalf(driver+": failed to insert an user: %s", err)
				}
			}(db, i)
		}
		wg.Wait()
		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf(driver+": failed to count users: %s", err)
		}
		if count != 7 {
			t.Fatalf(driver+": expected 7 users to be in database, but got %d", count)
		}
	}
}

func TestShouldFailInvalidPrepareStatement(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "fail_prepare")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		if _, err = db.Prepare("THIS SHOULD FAIL..."); err == nil {
			t.Fatalf(driver + ": expected an error, since prepare should validate sql query, but got none")
		}
	}
}

func TestShouldHandlePrepare(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "prepare")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		selectSQL := "SELECT email FROM users WHERE username = ?"
		if strings.Index(driver, "psql_") == 0 {
			selectSQL = "SELECT email FROM users WHERE username = $1"
		}

		stmt1, err := db.Prepare(selectSQL)
		if err != nil {
			t.Fatalf(driver+": could not prepare - %s", err)
		}

		insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
		if strings.Index(driver, "psql_") == 0 {
			insertSQL = "INSERT INTO users (username, email) VALUES($1, $2)"
		}
		stmt2, err := db.Prepare(insertSQL)
		if err != nil {
			t.Fatalf(driver+": could not prepare - %s", err)
		}

		var email string
		if err = stmt1.QueryRow("jane").Scan(&email); err != nil {
			t.Fatalf(driver+": could not scan email - %s", err)
		}

		_, err = stmt2.Exec("mark", "mark.spencer@gmail.com")
		if err != nil {
			t.Fatalf(driver+": should have inserted user - %s", err)
		}
	}
}
