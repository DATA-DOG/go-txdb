package txdb_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

// bootstrap bootstraps the database for tests.
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
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + testDB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE " + testDB); err != nil {
		t.Fatal(err)
	}
}

func startPostgres(t *testing.T) string {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx, "docker.io/postgres:15.2-alpine",
		postgres.WithDatabase(testDB),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	dsn, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(dsn, testDB+"?")
}

func startMySQL(t *testing.T) string {
	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx, "mysql:8",
		mysql.WithUsername("root"),
		mysql.WithPassword("password"),
		mysql.WithDatabase(testDB),
	)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := mysqlContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(dsn, testDB)
}
