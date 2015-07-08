/*
Package txdb is a single transaction based database sql driver. When the connection
is opened, it starts a transaction and all operations performed on this *sql.DB
will be within that transaction. If concurrent actions are performed, the lock is
acquired and connection is always released the statements and rows are not holding the
connection.

Why is it useful. A very basic use case would be if you want to make functional tests
you can prepare a test database and within each test you do not have to reload a database.
All tests are isolated within transaction and though, performs fast. And you do not have
to interface your sql.DB reference in your code, txdb is like a standard sql.Driver.

This driver supports any sql.Driver connection to be opened. You can register txdb
for different sql drivers and have it under different driver names. Under the hood
whenever a txdb driver is opened, it attempts to open a real connection and starts
transaction. When close is called, it rollbacks transaction leaving your prepared
test database in the same state as before.

Given, you have a mysql database called txdb_test and a table users with a username
column.

Example:
	package main

	import (
		"database/sql"
		"log"

		"github.com/DATA-DOG/go-txdb"
		_ "github.com/go-sql-driver/mysql"
	)

	func init() {
		// we register an sql driver named "txdb"
		txdb.Register("txdb", "mysql", "root@/txdb_test")
	}

	func main() {
		db, err := sql.Open("txdb", "") // dsn is not necessary for our tx driver
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		if _, err := db.Exec(`INSERT INTO users(username) VALUES("gopher")`); err != nil {
			log.Fatal(err)
		}
	}

Every time you will run this application, it will remain in the same state as before.
*/
package txdb

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
)

// Register a txdb sql driver under the given sql driver name
// which can be used to open a single transaction based database
// connection.
//
// When Open is called any number of times it returns
// the same transaction connection. Any Begin, Commit calls
// will not start or close the transaction.
//
// When Close is called, the transaction is rolled back.
//
// Use drv and dsn as the standard sql properties for
// your test database connection to be isolated within transaction.
func Register(name, drv, dsn string) {
	sql.Register(name, &txDriver{dsn: dsn, drv: drv})
}

// txDriver is an sql driver which runs on single transaction
// when the Close is called, transaction is rolled back
type txDriver struct {
	conns int
	sync.Mutex
	tx *sql.Tx

	drv string
	dsn string
	db  *sql.DB
}

func (d *txDriver) Open(dsn string) (driver.Conn, error) {
	// first open a real database connection
	var err error
	if d.db == nil {
		db, err := sql.Open(d.drv, d.dsn)
		if err != nil {
			return d, err
		}
		d.db = db
	}
	if d.tx == nil {
		d.tx, err = d.db.Begin()
	}
	d.conns++
	return d, err
}

func (d *txDriver) Close() (err error) {
	d.conns--
	if d.conns == 0 {
		err = d.tx.Rollback()
		if err != nil {
			return
		}
		d.tx = nil
		err = d.db.Close()
		if err != nil {
			return
		}
		d.db = nil
	}
	return
}

func (d *txDriver) Begin() (driver.Tx, error) {
	return d, nil
}

func (d *txDriver) Commit() error {
	return nil
}

func (d *txDriver) Rollback() error {
	return nil
}

func (d *txDriver) Prepare(query string) (driver.Stmt, error) {
	return &stmt{drv: d, query: query}, nil
}

type stmt struct {
	query string
	drv   *txDriver
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.drv.Lock()
	defer s.drv.Unlock()

	st, err := s.drv.tx.Prepare(s.query)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	var iargs []interface{}
	for _, arg := range args {
		iargs = append(iargs, arg)
	}
	return st.Exec(iargs...)
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.drv.Lock()
	defer s.drv.Unlock()

	// create stement
	st, err := s.drv.tx.Prepare(s.query)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	// query rows
	var iargs []interface{}
	for _, arg := range args {
		iargs = append(iargs, arg)
	}
	rs, err := st.Query(iargs...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	// build all rows in memory, prevent statement lock
	rows := &rows{}
	rows.cols, err = rs.Columns()
	if err != nil {
		return nil, err
	}
	for rs.Next() {
		values := make([]interface{}, len(rows.cols))
		for i := range values {
			values[i] = new(interface{})
		}
		if err := rs.Scan(values...); err != nil {
			return rows, err
		}
		row := make([]driver.Value, len(rows.cols))
		for i, v := range values {
			row[i] = driver.Value(v)
		}
		rows.rows = append(rows.rows, row)
	}
	if err := rs.Err(); err != nil {
		return rows, err
	}
	return rows, nil
}

type rows struct {
	rows [][]driver.Value
	pos  int
	cols []string
}

func (r *rows) Columns() (cols []string) {
	return r.cols
}

func (r *rows) Next(dest []driver.Value) error {
	r.pos++
	if r.pos > len(r.rows) {
		return io.EOF
	}

	for i, val := range r.rows[r.pos-1] {
		dest[i] = *(val.(*interface{}))
	}

	return nil
}

func (r *rows) Close() error {
	return nil
}
