package txdb_test

import (
	"database/sql"
	"testing"
)

const (
	mysql_sql = `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,
		username VARCHAR(32) NOT NULL,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		UNIQUE INDEX uniq_email (email)
	) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB`

	psql_sql = `CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		username VARCHAR(32) NOT NULL,
		email VARCHAR(255) UNIQUE NOT NULL
	)`

	inserts = `INSERT INTO users (username, email) VALUES ('gopher', 'gopher@go.com'), ('john', 'john@doe.com'), ('jane', 'jane@doe.com')`

	testDB = "txdb_test"
)

// bootstrap bootstraps the database with the nfor tests.
func bootstrap(t *testing.T, driver, dsn string) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	switch driver {
	case "mysql":
		if _, err := db.Exec(mysql_sql); err != nil {
			t.Fatal(err)
		}
	case "postgres":
		if _, err := db.Exec(psql_sql); err != nil {
			t.Fatal(err)
		}
	default:
		panic("unrecognized driver: " + driver)
	}
	if _, err := db.Exec(inserts); err != nil {
		t.Fatal(err)
	}
}

func createDB(t *testing.T, driver, dsn string) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DROP DATABASE IF EXISTS txdb_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE txdb_test"); err != nil {
		t.Fatal(err)
	}
}
