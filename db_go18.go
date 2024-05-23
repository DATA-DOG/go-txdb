//go:build go1.8

package txdb

import (
	"context"
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

func (c *conn) beginTxOnce(ctx context.Context, done <-chan struct{}) (*sql.Tx, error) {
	if c.tx == nil {
		rootCtx, cancel := context.WithCancel(context.Background())
		tx, err := c.drv.db.BeginTx(rootCtx, &sql.TxOptions{})
		if err != nil {
			cancel()
			return nil, err
		}
		c.tx, c.ctx, c.cancel = tx, rootCtx, cancel
	}
	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-done:
				// the operation successfully finished at the "same time" as context cancellation, so we won't close ctx on tx
			default:
				// operation was interrupted by context cancel, so we cancel parent as well
				c.cancel()
			}
		case <-done:
			// operation was successfully finished, so we don't close ctx on tx
		case <-c.ctx.Done():
		}
	}()
	return c.tx, nil
}

// Implement the "QueryerContext" interface
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.Lock()
	defer c.Unlock()

	done := make(chan struct{})
	defer close(done)

	tx, err := c.beginTxOnce(ctx, done)
	if err != nil {
		return nil, err
	}

	rs, err := tx.QueryContext(ctx, query, mapNamedArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	return buildRows(rs)
}

// Implement the "ExecerContext" interface
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.Lock()
	defer c.Unlock()

	done := make(chan struct{})
	defer close(done)

	tx, err := c.beginTxOnce(ctx, done)
	if err != nil {
		return nil, err
	}

	return tx.ExecContext(ctx, query, mapNamedArgs(args)...)
}

// Implement the "ConnBeginTx" interface
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}

// Implement the "ConnPrepareContext" interface
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.Lock()
	defer c.Unlock()

	done := make(chan struct{})
	defer close(done)

	tx, err := c.beginTxOnce(ctx, done)
	if err != nil {
		return nil, err
	}

	st, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	stmtFailedStr := make(chan bool)
	go func() {
		select {
		case <-c.ctx.Done():
		case erred := <-stmtFailedStr:
			if erred {
				st.Close()
			}
		}
	}()
	return &stmt{st: st, done: stmtFailedStr}, nil
}

// Implement the "Pinger" interface
func (c *conn) Ping(ctx context.Context) error {
	return c.drv.db.PingContext(ctx)
}

// Implement the "StmtExecContext" interface
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dr, err := s.st.ExecContext(ctx, mapNamedArgs(args)...)
	if err != nil {
		s.closeDone(true)
	}
	return dr, err
}

// Implement the "StmtQueryContext" interface
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.st.QueryContext(ctx, mapNamedArgs(args)...)
	if err != nil {
		s.closeDone(true)
		return nil, err
	}
	return buildRows(rows)
}

func mapNamedArgs(args []driver.NamedValue) (res []interface{}) {
	res = make([]interface{}, len(args))
	for i := range args {
		name := args[i].Name
		if name != "" {
			res[i] = sql.Named(name, args[i].Value)
		} else {
			res[i] = args[i].Value
		}
	}
	return
}
