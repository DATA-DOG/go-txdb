// +build mysql

package txdb

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	Register("mysql_txdb", "mysql", "root@/txdb_test?multiStatements=true")
}

func TestShouldBeAbleToLockTables(t *testing.T) {
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
