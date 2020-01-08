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

func TestShouldRunWithNestedTransaction(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		var count int
		db, err := sql.Open(driver, "five")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}

		func(db *sql.DB) {
			defer db.Close()

			_, err = db.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test1.com')`)
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

			tx, err := db.Begin()
			if err != nil {
				t.Fatalf(driver+": failed to begin transaction: %s", err)
			}
			{
				_, err = tx.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test2.com')`)
				if err != nil {
					t.Fatalf(driver+": failed to insert an user: %s", err)
				}
				err = tx.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
				if err != nil {
					t.Fatalf(driver+": failed to count users: %s", err)
				}
				if count != 5 {
					t.Fatalf(driver+": expected 5 users to be in database, but got %d", count)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf(driver+": failed to rollback transaction: %s", err)
				}
			}

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf(driver+": failed to count users: %s", err)
			}
			if count != 4 {
				t.Fatalf(driver+": expected 4 users to be in database, but got %d", count)
			}

			tx, err = db.Begin()
			if err != nil {
				t.Fatalf(driver+": failed to begin transaction: %s", err)
			}
			{
				_, err = tx.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test2.com')`)
				if err != nil {
					t.Fatalf(driver+": failed to insert an user: %s", err)
				}
				err = tx.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
				if err != nil {
					t.Fatalf(driver+": failed to count users: %s", err)
				}
				if count != 5 {
					t.Fatalf(driver+": expected 5 users to be in database, but got %d", count)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf(driver+": failed to commit transaction: %s", err)
				}
			}

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf(driver+": failed to count users: %s", err)
			}
			if count != 5 {
				t.Fatalf(driver+": expected 5 users to be in database, but got %d", count)
			}
		}(db)

		db, err = sql.Open(driver, "six")
		if err != nil {
			t.Fatalf(driver+": failed to reopen a mysql connection: %s", err)
		}
		func(db *sql.DB) {
			defer db.Close()

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf(driver+": failed to count users: %s", err)
			}
			if count != 3 {
				t.Fatalf(driver+": expected 3 users to be in database, but got %d", count)
			}
		}(db)
	}
}

func TestShouldRunWithinTransaction(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		var count int
		db, err := sql.Open(driver, "one")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}

		func(db *sql.DB) {
			defer db.Close()

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
		}(db)

		db, err = sql.Open(driver, "two")
		if err != nil {
			t.Fatalf(driver+": failed to reopen a mysql connection: %s", err)
		}
		func(db *sql.DB) {
			defer db.Close()

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf(driver+": failed to count users: %s", err)
			}
			if count != 3 {
				t.Fatalf(driver+": expected 3 users to be in database, but got %d", count)
			}
		}(db)
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

func TestShouldCloseRootDB(t *testing.T) {
	for _, driver := range drivers() {
		db1, err := sql.Open(driver, "first")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db1.Close()

		stmt, err := db1.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf(driver+": could not prepare - %s", err)
		}
		defer stmt.Close()

		drv1 := db1.Driver().(*txDriver)
		if drv1.db == nil {
			t.Fatalf(driver+": expected database, drv1.db: %v", drv1.db)
		}

		db2, err := sql.Open(driver, "second")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db2.Close()

		stmt, err = db2.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf(driver+": could not prepare - %s", err)
		}
		defer stmt.Close()

		// Both drivers share the same database.
		drv2 := db2.Driver().(*txDriver)
		if drv2.db != drv1.db {
			t.Fatalf(driver+": drv1.db=%v != drv2.db=%v", drv1.db, drv2.db)
		}

		// Database should remain open while a connection is open.
		if err := db1.Close(); err != nil {
			t.Fatalf(driver+": could not close database - %s", err)
		}

		if drv1.db == nil {
			t.Fatal(driver + ": expected database, not nil")
		}

		if drv2.db == nil {
			t.Fatal(driver + ": expected database ,not nil")
		}

		// Database should close after last connection is closed.
		if err := db2.Close(); err != nil {
			t.Fatalf(driver+": could not close database - %s", err)
		}

		if drv1.db != nil {
			t.Fatalf(driver+": expected closed database, not %v", drv1.db)
		}

		if drv2.db != nil {
			t.Fatalf(driver+": expected closed database, not %v", drv2.db)
		}
	}
}

func TestShouldReopenAfterClose(t *testing.T) {
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "first")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		stmt, err := db.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf(driver+": could not prepare - %s", err)
		}
		defer stmt.Close()

		if err := db.Close(); err != nil {
			t.Fatalf(driver+": could not close database - %s", err)
		}

		if err := db.Ping(); err.Error() != "sql: database is closed" {
			t.Fatalf(driver+": expected closed database - %s", err)
		}

		db, err = sql.Open(driver, "second")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Fatalf(driver+": failed to ping, have you run 'make test'? err: %s", err)
		}
	}
}
