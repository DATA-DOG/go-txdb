package txdb_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-txdb"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var txDrivers = []*struct {
	name       string
	driver     string
	dsn        string
	registered bool
}{
	{name: "mysql_txdb", driver: "mysql", dsn: "root:pass@/txdb_test?multiStatements=true"},
	{name: "psql_txdb", driver: "postgres", dsn: "postgres://postgres:pass@localhost/txdb_test?sslmode=disable"},
}

var registerMu sync.Mutex

func drivers() []string {
	var all []string
	for _, d := range txDrivers {
		registerMu.Lock()
		if !d.registered {
			txdb.Register(d.name, d.driver, d.dsn)
			d.registered = true
		}
		registerMu.Unlock()
		all = append(all, d.name)
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
					t.Errorf(driver+": failed to query users: %s", err)
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
					t.Errorf(driver+": failed to insert an user: %s", err)
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

		drv1 := db1.Driver().(*txdb.TxDriver)
		if drv1.DB() == nil {
			t.Fatalf(driver+": expected database, drv1.db: %v", drv1.DB())
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
		drv2 := db2.Driver().(*txdb.TxDriver)
		if drv2.DB() != drv1.DB() {
			t.Fatalf(driver+": drv1.db=%v != drv2.db=%v", drv1.DB(), drv2.DB())
		}

		// Database should remain open while a connection is open.
		if err := db1.Close(); err != nil {
			t.Fatalf(driver+": could not close database - %s", err)
		}

		if drv1.DB() == nil {
			t.Fatal(driver + ": expected database, not nil")
		}

		if drv2.DB() == nil {
			t.Fatal(driver + ": expected database ,not nil")
		}

		// Database should close after last connection is closed.
		if err := db2.Close(); err != nil {
			t.Fatalf(driver+": could not close database - %s", err)
		}

		if drv1.DB() != nil {
			t.Fatalf(driver+": expected closed database, not %v", drv1.DB())
		}

		if drv2.DB() != nil {
			t.Fatalf(driver+": expected closed database, not %v", drv2.DB())
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

type canceledContext struct{}

func (canceledContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, true }
func (canceledContext) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (canceledContext) Err() error                        { return errors.New("canceled") }
func (canceledContext) Value(key interface{}) interface{} { return nil }

func TestShouldDiscardConnectionWhenClosedBecauseOfError(t *testing.T) {
	for _, driver := range drivers() {
		t.Run(fmt.Sprintf("using driver %s", driver), func(t *testing.T) {
			{
				db, err := sql.Open(driver, "first")
				if err != nil {
					t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
				}
				defer db.Close()

				tx, err := db.Begin()
				defer func() {
					err = tx.Rollback()
					if err != nil {
						t.Fatalf(driver+": rollback err: %s", err)
					}
				}()
				if err != nil {
					t.Fatalf(driver+": failed to begin transaction err: %s", err)
				}

				// TODO: we somehow need to poison the DB connection here so that Rollback fails

				_, err = tx.PrepareContext(canceledContext{}, "SELECT * FROM users")
				if err == nil {
					t.Fatalf(driver + ": should have returned error for prepare")
				}
			}

			fmt.Println("Opening db...")

			{
				db, err := sql.Open(driver, "second")
				if err != nil {
					t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
				}
				defer db.Close()

				if err := db.Ping(); err != nil {
					t.Fatalf(driver+": failed to ping, have you run 'make test'? err: %s", err)
				}
			}
		})
	}
}

func TestPostgresRowsScanTypeTables(t *testing.T) {
	// make sure drivers are registered first
	_ = drivers()
	db, err := sql.Open("psql_txdb", "scantype")
	if err != nil {
		t.Fatalf("psql: failed to open a postgres connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("psql: unable to execute trivial query: %v", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("psql: unable to retrieve column types: %v", err)
	}

	int32Type := reflect.TypeOf(int32(0))
	if colTypes[0].ScanType() != int32Type {
		t.Fatalf("psql: column scan type is %s, but should be %s", colTypes[0].ScanType().String(), int32Type.String())
	}
}

func TestMysqlShouldBeAbleToLockTables(t *testing.T) {
	// make sure drivers are registered first
	_ = drivers()
	db, err := sql.Open("mysql_txdb", "locks")
	if err != nil {
		t.Fatalf("mysql: failed to open a mysql connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	_, err = db.Exec("LOCK TABLE users READ")
	if err != nil {
		t.Fatalf("mysql: should be able to lock table, but got err: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("mysql: unexpected read error: %v", err)
	}
	if count != 3 {
		t.Fatalf("mysql: was expecting 3 users in db")
	}

	_, err = db.Exec("UNLOCK TABLES")
	if err != nil {
		t.Fatalf("mysql: should be able to unlock table, but got err: %v", err)
	}
}

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

// https://github.com/DATA-DOG/go-txdb/issues/49
func TestIssue49(t *testing.T) {
	t.Parallel()
	for _, driver := range drivers() {
		db, err := sql.Open(driver, "rollback")
		if err != nil {
			t.Fatalf(driver+": failed to open a connection, have you run 'make test'? err: %s", err)
		}
		defer db.Close()

		// do a query prior to starting a nested transaction to
		// reproduce the error
		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf(driver+": prepared statement count err %v", err)
		}
		if count != 3 {
			t.Logf("Count not 3: %d", count)
			t.FailNow()
		}

		// start a nested transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf(driver+": failed to start transaction: %s", err)
		}
		// need a prepared statement to reproduce the error
		insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
		if strings.Index(driver, "psql_") == 0 {
			insertSQL = "INSERT INTO users (username, email) VALUES($1, $2)"
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			t.Fatalf(driver+": failed to prepare named statement: %s", err)
		}

		// try to insert already existing username/email
		_, err = stmt.Exec("gopher", "gopher@go.com")
		if err == nil {
			t.Fatalf(driver + ": double insert?")
		}
		// The insert failed, so we need to close the prepared statement
		err = stmt.Close()
		if err != nil {
			t.Fatalf(driver+": error closing prepared statement: %s", err)
		}
		// rollback the transaction now that it has failed
		err = tx.Rollback()
		if err != nil {
			t.Logf(driver+": failed rollback of failed transaction: %s", err)
			t.FailNow()
		}
	}
}
