[![Build Status](https://travis-ci.org/DATA-DOG/go-txdb.svg?branch=master)](https://travis-ci.org/DATA-DOG/go-txdb)
[![GoDoc](https://godoc.org/github.com/DATA-DOG/go-txdb?status.svg)](https://godoc.org/github.com/DATA-DOG/go-txdb)

# Single transaction based sql.Driver for GO

Package **txdb** is a single transaction based database sql driver. When the connection
is opened, it starts a transaction and all operations performed on this **sql.DB**
will be within that transaction. If concurrent actions are performed, the lock is
acquired and connection is always released the statements and rows are not holding the
connection.

Why is it useful. A very basic use case would be if you want to make functional tests
you can prepare a test database and within each test you do not have to reload a database.
All tests are isolated within transaction and though, performs fast. And you do not have
to interface your **sql.DB** reference in your code, **txdb** is like a standard **sql.Driver**.

This driver supports any **sql.Driver** connection to be opened. You can register txdb
for different sql drivers and have it under different driver names. Under the hood
whenever a txdb driver is opened, it attempts to open a real connection and starts
transaction. When close is called, it rollbacks transaction leaving your prepared
test database in the same state as before.

Given, you have a mysql database called **txdb_test** and a table **users** with a **username**
column.

``` go
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
    db.SetMaxOpenConns(1)

    if _, err := db.Exec(`INSERT INTO users(username) VALUES("gopher")`); err != nil {
        log.Fatal(err)
    }
}
```

Note that `db.SetMaxOpenConns(1)` disables concurrent database transactions,
which [txdb does not support](https://github.com/DATA-DOG/go-txdb/issues/69).

You can also use [`sql.OpenDB`](https://golang.org/pkg/database/sql/#OpenDB) (added in Go 1.10) rather than registering a txdb driver instance, if you prefer:

``` go
package main

import (
    "database/sql"
    "log"

    "github.com/DATA-DOG/go-txdb"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    db := sql.OpenDB(txdb.New("mysql", "root@/txdb_test"))
    defer db.Close()
    db.SetMaxOpenConns(1)

    if _, err := db.Exec(`INSERT INTO users(username) VALUES("gopher")`); err != nil {
        log.Fatal(err)
    }
}
```

Every time you will run this application, it will remain in the same state as before.

### Testing

Usage is mainly intended for testing purposes. Tests require database access, support using `postgres` and `mysql` databases. The easiest way to do this is by using [testcontainers](https://golang.testcontainers.org/), which is enabled by setting the respective database DSN values to `AUTO`. Example:

```bash
MYSQL_DSN=AUTO PSQL_DSN=AUTO go test ./...
```

If you wish to use a running local database instance, you can also provide the DSN directly, and it will be used:

```bash
MYSQL_DSN=root:pass@/ PSQL_DSN=postgres://postgres:pass@localhost/ go test ./...
```

To run tests only against MySQL or PostgreSQL, you may provide only the respective DSN values; any unset DSN is skipped for tests.

### Documentation

See [godoc][godoc] for general API details.
See **.travis.yml** for supported **go** versions.

### Contributions

Feel free to open a pull request. Note, if you wish to contribute an extension to public (exported methods or types) -
please open an issue before to discuss whether these changes can be accepted. All backward incompatible changes are
and will be treated cautiously.

The public API is locked since it is an **sql.Driver** and will not change.

### License

**txdb** is licensed under the [three clause BSD license][license]

[godoc]: http://godoc.org/github.com/DATA-DOG/go-txdb "Documentation on
godoc"

[golang]: https://golang.org/  "GO programming language"

[license]:http://en.wikipedia.org/wiki/BSD_licenses "The three clause BSD license"
