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

        if _, err := db.Exec(`INSERT INTO users(username) VALUES("gopher")`); err != nil {
            log.Fatal(err)
        }
    }
```

Every time you will run this application, it will remain in the same state as before.

### Testing

Usage is mainly intended for testing purposes. See the **db_test.go** as an example.

In order to run tests, you need a mysql database with a root access without password locally.

    make test

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

[godoc]: http://godoc.org/github.com/DATA-DOG/go-txdb "Documentation on godoc"
[golang]: https://golang.org/  "GO programming language"
[license]: http://en.wikipedia.org/wiki/BSD_licenses "The three clause BSD license"
