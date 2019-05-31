// +build go1.9

package txdb

import (
	"database/sql"
	"database/sql/driver"
)

// Implement the NamedValueChecker interface
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case sql.Out:
		return nil
	default:
		return driver.ErrSkip
	}
}
