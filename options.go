package txdb

import "fmt"

// SavePoint defines the syntax to create savepoints
// within transaction
type SavePoint interface {
	Create(id string) string
	Release(id string) string
	Rollback(id string) string
}

type defaultSavePoint struct{}

func (dsp *defaultSavePoint) Create(id string) string {
	return fmt.Sprintf("SAVEPOINT %s", id)
}
func (dsp *defaultSavePoint) Release(id string) string {
	return fmt.Sprintf("RELEASE SAVEPOINT %s", id)
}
func (dsp *defaultSavePoint) Rollback(id string) string {
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", id)
}

// SavePointOption allows to modify the logic for
// transaction save points. In such cases if your driver
// does not support it, use nil. If not compatible with default
// use custom.
func SavePointOption(savePoint SavePoint) func(*conn) error {
	return func(c *conn) error {
		c.savePoint = savePoint
		return nil
	}
}
