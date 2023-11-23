package txdb_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

var txDrivers = testDrivers{
	{name: "mysql_txdb", driver: "mysql", dsnEnvKey: "MYSQL_DSN", options: "multiStatements=true"},
	{name: "psql_txdb", driver: "postgres", dsnEnvKey: "PSQL_DSN", options: "sslmode=disable"},
}

type testDriver struct {
	// name is the name we use internally for the connection.
	name string
	// driver is the name registered by the driver when imported.
	driver string
	// dsnEnvKey is the name of an environment variable to fetch the DSN from.
	// It is expected to include the name of the database, and any necessary
	// credentials.
	dsnEnvKey string
	// options are optional parameters appended to the DSN before connecting
	options string
	// registered is set to true once the driver is registered, to prevent
	// duplicate registration
	registered bool
}

type testDrivers []*testDriver

var registerMu sync.Mutex

// dsn returns the base dsn (without DB name) and the full dsn (with dbname)
// for the test driver, or calls t.Skip if it is unset or disabled.
func (d *testDriver) dsn(t *testing.T) (base string, full string) {
	t.Helper()
	base = os.Getenv(d.dsnEnvKey)
	if base == "" {
		t.Skipf("%s not set, skipping tests for %s", d.dsnEnvKey, d.driver)
	}
	full = strings.TrimSuffix(base, "/") + "/" + testDB
	if d.options == "" {
		return base, full
	}
	return base + "?" + d.options, full + "?" + d.options
}

func (d *testDriver) register(t *testing.T) {
	t.Helper()
	registerMu.Lock()
	defer registerMu.Unlock()
	if !d.registered {
		base, full := d.dsn(t)
		d.registered = true
		createDB(t, d.driver, base)
		bootstrap(t, d.driver, full)
		txdb.Register(d.name, d.driver, full)
	}
}

// Run registers the driver, if not already registered, then calls f with the
// driver name.
func (d *testDriver) Run(t *testing.T, f func(t *testing.T, driver *testDriver)) {
	t.Helper()
	t.Run(d.name, func(t *testing.T) {
		d.register(t)
		f(t, d)
	})
}

// Run iterates over the configured drivers, and calls [testDriver.Run] on each.
func (d testDrivers) Run(t *testing.T, f func(t *testing.T, driver *testDriver)) {
	t.Helper()
	for _, driver := range d {
		driver.Run(t, f)
	}
}

// driver returns the subset of d whose driver match one of the provided names.
// Useful for tests that require specific database driver capabilities.
func (d testDrivers) drivers(names ...string) testDrivers {
	result := make(testDrivers, 0, len(d))
	for _, driver := range d {
		for _, name := range names {
			if driver.driver == name {
				result = append(result, driver)
			}
		}
	}
	return result
}

func TestShouldWorkWithOpenDB(t *testing.T) {
	t.Parallel()
	for _, d := range txDrivers {
		d.Run(t, func(t *testing.T, driver *testDriver) {
			_, dsn := driver.dsn(t)
			db := sql.OpenDB(txdb.New(d.driver, dsn))
			defer db.Close()
			_, err := db.Exec("SELECT 1")
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShouldRunWithNestedTransaction(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		var count int
		db, err := sql.Open(driver.name, "five")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}

		func(db *sql.DB) {
			defer db.Close()

			_, err = db.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test1.com')`)
			if err != nil {
				t.Fatalf("failed to insert a user: %s", err)
			}
			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 4 {
				t.Fatalf("expected 4 users to be in database, but got %d", count)
			}

			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("failed to begin transaction: %s", err)
			}
			{
				_, err = tx.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test2.com')`)
				if err != nil {
					t.Fatalf("failed to insert an user: %s", err)
				}
				err = tx.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
				if err != nil {
					t.Fatalf("failed to count users: %s", err)
				}
				if count != 5 {
					t.Fatalf("expected 5 users to be in database, but got %d", count)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("failed to rollback transaction: %s", err)
				}
			}

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 4 {
				t.Fatalf("expected 4 users to be in database, but got %d", count)
			}

			tx, err = db.Begin()
			if err != nil {
				t.Fatalf("failed to begin transaction: %s", err)
			}
			{
				_, err = tx.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test2.com')`)
				if err != nil {
					t.Fatalf("failed to insert an user: %s", err)
				}
				err = tx.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
				if err != nil {
					t.Fatalf("failed to count users: %s", err)
				}
				if count != 5 {
					t.Fatalf("expected 5 users to be in database, but got %d", count)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("failed to commit transaction: %s", err)
				}
			}

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 5 {
				t.Fatalf("expected 5 users to be in database, but got %d", count)
			}
		}(db)

		db, err = sql.Open(driver.name, "six")
		if err != nil {
			t.Fatalf("failed to reopen a mysql connection: %s", err)
		}
		func(db *sql.DB) {
			defer db.Close()

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 3 {
				t.Fatalf("expected 3 users to be in database, but got %d", count)
			}
		}(db)
	})
}

func TestShouldRunWithinTransaction(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		var count int
		db, err := sql.Open(driver.name, "one")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}

		func(db *sql.DB) {
			defer db.Close()

			_, err = db.Exec(`INSERT INTO users (username, email) VALUES('txdb', 'txdb@test.com')`)
			if err != nil {
				t.Fatalf("failed to insert an user: %s", err)
			}
			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 4 {
				t.Fatalf("expected 4 users to be in database, but got %d", count)
			}
		}(db)

		db, err = sql.Open(driver.name, "two")
		if err != nil {
			t.Fatalf("failed to reopen a mysql connection: %s", err)
		}
		func(db *sql.DB) {
			defer db.Close()

			err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
			if err != nil {
				t.Fatalf("failed to count users: %s", err)
			}
			if count != 3 {
				t.Fatalf("expected 3 users to be in database, but got %d", count)
			}
		}(db)
	})
}

func TestShouldNotHoldConnectionForRows(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "three")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		rows, err := db.Query("SELECT username FROM users")
		if err != nil {
			t.Fatalf("failed to query users: %s", err)
		}
		defer rows.Close()

		_, err = db.Exec(`INSERT INTO users(username, email) VALUES('txdb', 'txdb@test.com')`)
		if err != nil {
			t.Fatalf("failed to insert an user: %s", err)
		}
	})
}

func TestShouldPerformParallelActions(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "four")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		wg := &sync.WaitGroup{}
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(d *sql.DB, idx int) {
				defer wg.Done()
				rows, err := d.Query("SELECT username FROM users")
				if err != nil {
					t.Errorf("failed to query users: %s", err)
				}
				defer rows.Close()

				insertSQL := "INSERT INTO users(username, email) VALUES(?, ?)"
				if strings.Index(driver.name, "psql_") == 0 {
					insertSQL = "INSERT INTO users(username, email) VALUES($1, $2)"
				}
				username := fmt.Sprintf("parallel%d", idx)
				email := fmt.Sprintf("parallel%d@test.com", idx)
				_, err = d.Exec(insertSQL, username, email)
				if err != nil {
					t.Errorf("failed to insert an user: %s", err)
				}
			}(db, i)
		}
		wg.Wait()
		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf("failed to count users: %s", err)
		}
		if count != 7 {
			t.Fatalf("expected 7 users to be in database, but got %d", count)
		}
	})
}

func TestShouldFailInvalidPrepareStatement(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "fail_prepare")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		if _, err = db.Prepare("THIS SHOULD FAIL..."); err == nil {
			t.Fatal("expected an error, since prepare should validate sql query, but got none")
		}
	})
}

func TestShouldHandlePrepare(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "prepare")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		selectSQL := "SELECT email FROM users WHERE username = ?"
		if strings.Index(driver.name, "psql_") == 0 {
			selectSQL = "SELECT email FROM users WHERE username = $1"
		}

		stmt1, err := db.Prepare(selectSQL)
		if err != nil {
			t.Fatalf("could not prepare - %s", err)
		}

		insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
		if strings.Index(driver.name, "psql_") == 0 {
			insertSQL = "INSERT INTO users (username, email) VALUES($1, $2)"
		}
		stmt2, err := db.Prepare(insertSQL)
		if err != nil {
			t.Fatalf("could not prepare - %s", err)
		}

		var email string
		if err = stmt1.QueryRow("jane").Scan(&email); err != nil {
			t.Fatalf("could not scan email - %s", err)
		}

		_, err = stmt2.Exec("mark", "mark.spencer@gmail.com")
		if err != nil {
			t.Fatalf("should have inserted user - %s", err)
		}
	})
}

func TestShouldCloseRootDB(t *testing.T) {
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db1, err := sql.Open(driver.name, "first")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db1.Close()

		stmt, err := db1.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf("could not prepare - %s", err)
		}
		defer stmt.Close()

		drv1 := db1.Driver().(*txdb.TxDriver)
		if drv1.DB() == nil {
			t.Fatalf("expected database, drv1.db: %v", drv1.DB())
		}

		db2, err := sql.Open(driver.name, "second")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db2.Close()

		stmt, err = db2.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf("could not prepare - %s", err)
		}
		defer stmt.Close()

		// Both drivers share the same database.
		drv2 := db2.Driver().(*txdb.TxDriver)
		if drv2.DB() != drv1.DB() {
			t.Fatalf("drv1.db=%v != drv2.db=%v", drv1.DB(), drv2.DB())
		}

		// Database should remain open while a connection is open.
		if err := db1.Close(); err != nil {
			t.Fatalf("could not close database - %s", err)
		}

		if drv1.DB() == nil {
			t.Fatal("expected database, not nil")
		}

		if drv2.DB() == nil {
			t.Fatal("expected database ,not nil")
		}

		// Database should close after last connection is closed.
		if err := db2.Close(); err != nil {
			t.Fatalf("could not close database - %s", err)
		}

		if drv1.DB() != nil {
			t.Fatalf("expected closed database, not %v", drv1.DB())
		}

		if drv2.DB() != nil {
			t.Fatalf("expected closed database, not %v", drv2.DB())
		}
	})
}

func TestShouldReopenAfterClose(t *testing.T) {
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "first")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		stmt, err := db.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf("could not prepare - %s", err)
		}
		defer stmt.Close()

		if err := db.Close(); err != nil {
			t.Fatalf("could not close database - %s", err)
		}

		if err := db.Ping(); err.Error() != "sql: database is closed" {
			t.Fatalf("expected closed database - %s", err)
		}

		db, err = sql.Open(driver.name, "second")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Fatalf("failed to ping: %s", err)
		}
	})
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
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		{
			db, err := sql.Open(driver.name, "first")
			if err != nil {
				t.Fatalf("failed to open a connection: %s", err)
			}
			defer db.Close()

			tx, err := db.Begin()
			defer func() {
				err = tx.Rollback()
				if err != nil {
					t.Fatalf("rollback err: %s", err)
				}
			}()
			if err != nil {
				t.Fatalf("failed to begin transaction err: %s", err)
			}

			// TODO: we somehow need to poison the DB connection here so that Rollback fails

			_, err = tx.PrepareContext(canceledContext{}, "SELECT * FROM users")
			if err == nil {
				t.Fatal("should have returned error for prepare")
			}
		}

		fmt.Println("Opening db...")

		{
			db, err := sql.Open(driver.name, "second")
			if err != nil {
				t.Fatalf("failed to open a connection: %s", err)
			}
			defer db.Close()

			if err := db.Ping(); err != nil {
				t.Fatalf("failed to ping: %s", err)
			}
		}
	})
}

func TestPostgresRowsScanTypeTables(t *testing.T) {
	txDrivers.drivers("postgres").Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "scantype")
		if err != nil {
			t.Fatalf("psql: failed to open a postgres connection: %s", err)
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
	})
}

func TestMysqlShouldBeAbleToLockTables(t *testing.T) {
	txDrivers.drivers("mysql").Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "locks")
		if err != nil {
			t.Fatalf("mysql: failed to open a mysql connection: %s", err)
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
	})
}

func TestShouldGetMultiRowSet(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "multiRows")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		rows, err := db.QueryContext(context.Background(), "SELECT username FROM users; SELECT COUNT(*) FROM users;")
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
	})
}

func TestShouldBeAbleToPingWithContext(t *testing.T) {
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "ping")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("%v", err)
		}
	})
}

func TestShouldHandleStmtsWithoutContextPollution(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "contextpollution")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
		if strings.Index(driver.name, "psql_") == 0 {
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

// https://github.com/DATA-DOG/go-txdb/issues/49
func TestIssue49(t *testing.T) {
	t.Parallel()
	txDrivers.Run(t, func(t *testing.T, driver *testDriver) {
		db, err := sql.Open(driver.name, "rollback")
		if err != nil {
			t.Fatalf("failed to open a connection: %s", err)
		}
		defer db.Close()

		// do a query prior to starting a nested transaction to
		// reproduce the error
		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
		if err != nil {
			t.Fatalf("prepared statement count err %v", err)
		}
		if count != 3 {
			t.Logf("Count not 3: %d", count)
			t.FailNow()
		}

		// start a nested transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("failed to start transaction: %s", err)
		}
		// need a prepared statement to reproduce the error
		insertSQL := "INSERT INTO users (username, email) VALUES(?, ?)"
		if strings.Index(driver.name, "psql_") == 0 {
			insertSQL = "INSERT INTO users (username, email) VALUES($1, $2)"
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			t.Fatalf("failed to prepare named statement: %s", err)
		}

		// try to insert already existing username/email
		_, err = stmt.Exec("gopher", "gopher@go.com")
		if err == nil {
			t.Fatal("double insert?")
		}
		// The insert failed, so we need to close the prepared statement
		err = stmt.Close()
		if err != nil {
			t.Fatalf("error closing prepared statement: %s", err)
		}
		// rollback the transaction now that it has failed
		err = tx.Rollback()
		if err != nil {
			t.Logf("failed rollback of failed transaction: %s", err)
			t.FailNow()
		}
	})
}
