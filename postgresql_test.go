// +build psql

package txdb

import _ "github.com/lib/pq"

func init() {
	Register("psql_txdb", "postgres", "postgres://postgres@localhost/txdb_test?sslmode=disable")
}
