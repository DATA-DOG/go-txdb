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
        // dsn serves as an unique identifier for connection pool
		db, err := sql.Open("txdb", "identifier")
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
//
// Note: if you open a secondary database, make sure to differianciate
// the dsn string when opening the sql.DB. The transaction will be
// isolated within that dsn
func Register(name, drv, dsn string) {
	sql.Register(name, &txDriver{
		dsn:   dsn,
		drv:   drv,
		conns: make(map[string]*conn),
	})
}

// txDriver is an sql driver which runs on single transaction
// when the Close is called, transaction is rolled back
type conn struct {
	sync.Mutex
	tx     *sql.Tx
	dsn    string
	opened int
	drv    *txDriver
}

type txDriver struct {
	sync.Mutex
	db    *sql.DB
	conns map[string]*conn

	drv string
	dsn string
}

func (d *txDriver) Open(dsn string) (driver.Conn, error) {
	d.Lock()
	defer d.Unlock()
	// first open a real database connection
	var err error
	if d.db == nil {
		db, err := sql.Open(d.drv, d.dsn)
		if err != nil {
			return nil, err
		}
		d.db = db
	}
	c, ok := d.conns[dsn]
	if !ok {
		c = &conn{dsn: dsn, drv: d}
		c.tx, err = d.db.Begin()
		d.conns[dsn] = c
	}
	c.opened++
	return c, err
}

func (c *conn) Close() (err error) {
	c.drv.Lock()
	defer c.drv.Unlock()
	c.opened--
	if c.opened == 0 {
		err = c.tx.Rollback()
		if err != nil {
			return
		}
		c.tx = nil
		delete(c.drv.conns, c.dsn)
	}
	return
}

func (c *conn) Begin() (driver.Tx, error) {
	return c, nil
}

func (c *conn) Commit() error {
	return nil
}

func (c *conn) Rollback() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	c.Lock()
	defer c.Unlock()

	st, err := c.tx.Prepare(query)
	if err == nil {
		st.Close()
	}
	return &stmt{query: query, conn: c}, err
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	c.Lock()
	defer c.Unlock()

	return c.tx.Exec(query, mapArgs(args)...)
}

func mapArgs(args []driver.Value) []interface{} {
	var iargs []interface{}
	for _, arg := range args {
		iargs = append(iargs, arg)
	}
	return iargs
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	c.Lock()
	defer c.Unlock()

	// query rows
	rs, err := c.tx.Query(query, mapArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	return buildRows(rs)
}

type stmt struct {
	query string
	conn  *conn
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.Exec(s.query, args)
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.Query(s.query, args)
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

func (r *rows) read(rs *sql.Rows) error {
	var err error
	r.cols, err = rs.Columns()
	if err != nil {
		return err
	}
	for rs.Next() {
		values := make([]interface{}, len(r.cols))
		for i := range values {
			values[i] = new(interface{})
		}
		if err := rs.Scan(values...); err != nil {
			return err
		}
		row := make([]driver.Value, len(r.cols))
		for i, v := range values {
			row[i] = driver.Value(v)
		}
		r.rows = append(r.rows, row)
	}
	return rs.Err()
}

type rowSets struct {
	sets []*rows
	pos  int
}

func (rs *rowSets) Columns() []string {
	return rs.sets[rs.pos].cols
}

func (rs *rowSets) Close() error {
	return nil
}

// advances to next row
func (rs *rowSets) Next(dest []driver.Value) error {
	return rs.sets[rs.pos].Next(dest)
}
