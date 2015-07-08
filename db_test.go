package txdb

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	// we register an sql driver txdb
	Register("txdb", "mysql", "root@/txdb_test")
}

func TestShouldRunWithinTransaction(t *testing.T) {
	var count int
	db, err := sql.Open("txdb", "")
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

	db, err = sql.Open("txdb", "")
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
}

func TestShouldNotHoldConnectionForRows(t *testing.T) {
	db, err := sql.Open("txdb", "")
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
