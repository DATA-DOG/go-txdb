// +build go1.8

package txdb

import (
	"database/sql"
	"database/sql/driver"
	"io"
)

func buildRows(r *sql.Rows) (driver.Rows, error) {
	set := &rowSets{}
	rs := &rows{}
	if err := rs.read(r); err != nil {
		return set, err
	}
	set.sets = append(set.sets, rs)
	for r.NextResultSet() {
		rss := &rows{}
		if err := rss.read(r); err != nil {
			return set, err
		}
		set.sets = append(set.sets, rss)
	}
	return set, nil
}

// Implement the "RowsNextResultSet" interface
func (rs *rowSets) HasNextResultSet() bool {
	return rs.pos+1 < len(rs.sets)
}

// Implement the "RowsNextResultSet" interface
func (rs *rowSets) NextResultSet() error {
	if !rs.HasNextResultSet() {
		return io.EOF
	}

	rs.pos++
	return nil
}
