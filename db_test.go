package txdb

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	// we register an sql driver txdb
	Register("txdb", "mysql", "root@/txdb_test")
}

func TestShouldRunWithinTransaction(t *testing.T) {
	t.Parallel()
	var count int
	db, err := sql.Open("txdb", "one")
	if err != nil {
		t.Fatalf("failed to open a mysql connection, have you run 'make test'? err: %s", err)
	}

	_, err = db.Exec(`INSERT INTO users(username, email) VALUES("txdb", "txdb@test.com")`)
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
	db.Close()

	db, err = sql.Open("txdb", "two")
	if err != nil {
		t.Fatalf("failed to reopen a mysql connection: %s", err)
	}

	err = db.QueryRow("SELECT COUNT(id) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count users: %s", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 users to be in database, but got %d", count)
	}
	db.Close()
}

func TestShouldNotHoldConnectionForRows(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("txdb", "three")
	if err != nil {
		t.Fatalf("failed to open a mysql connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT username FROM users")
	if err != nil {
		t.Fatalf("failed to query users: %s", err)
	}
	defer rows.Close()

	_, err = db.Exec(`INSERT INTO users(username, email) VALUES("txdb", "txdb@test.com")`)
	if err != nil {
		t.Fatalf("failed to insert an user: %s", err)
	}
}

func TestShouldPerformParallelActions(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	t.Parallel()
	db, err := sql.Open("txdb", "four")
	if err != nil {
		t.Fatalf("failed to open a mysql connection, have you run 'make test'? err: %s", err)
	}
	defer db.Close()

	wg := &sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(d *sql.DB, idx int) {
			defer wg.Done()
			rows, err := d.Query("SELECT username FROM users")
			if err != nil {
				t.Fatalf("failed to query users: %s", err)
			}
			defer rows.Close()

			username := fmt.Sprintf("parallel%d", idx)
			email := fmt.Sprintf("parallel%d@test.com", idx)
			_, err = d.Exec(`INSERT INTO users(username, email) VALUES(?, ?)`, username, email)
			if err != nil {
				t.Fatalf("failed to insert an user: %s", err)
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
}
