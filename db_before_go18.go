// +build !go1.8

package txdb

import (
	"database/sql"
	"database/sql/driver"
)

func buildRows(r *sql.Rows) (driver.Rows, error) {
	rows := &rows{}
	err := rows.read(r)
	return rows, err
}
