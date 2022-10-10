// +build psql

package txdb

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/lib/pq"
)

func init() {
	Register("psql_txdb", "postgres", "postgres://postgres@localhost/txdb_test?sslmode=disable")
}

func TestRowsScanTypeTables(t *testing.T) {
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
