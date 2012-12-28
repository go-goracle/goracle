//Go Oracle driver
package godrv

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	// "fmt"
	"github.com/tgulacsi/goracle/oracle"
	// "io"
	// "math"
	// "net"
	// "log"
	// "reflect"
	"strings"
	// "time"
	"unsafe"
)

var (
	NotImplemented = errors.New("Not implemented")
)

type conn struct {
	cx *oracle.Connection
}

type stmt struct {
	cu        *oracle.Cursor //Stmt ?
	statement string
}

// Prepare the query for execution, return a prepared statement and error
func (c conn) Prepare(query string) (driver.Stmt, error) {
	cu := c.cx.NewCursor()
	err := cu.Prepare(query, "")
	if err != nil {
		return nil, err
	}
	return stmt{cu: cu, statement: query}, nil
}

// closes the connection
func (c conn) Close() error {
	err := c.cx.Close()
	c.cx = nil
	return err
}

type tx struct {
	cx *oracle.Connection //Transaction ?
}

// begins a transaction
func (c conn) Begin() (driver.Tx, error) {
	if !c.cx.IsConnected() {
		if err := c.cx.Connect(0, false); err != nil {
			return tx{cx: nil}, err
		}
	}
	return tx{cx: c.cx}, nil
}

// commits currently opened transaction
func (t tx) Commit() error {
	return t.cx.NewCursor().Execute("COMMIT", nil, nil)
}

// rolls back current transaction
func (t tx) Rollback() error {
	return t.cx.Rollback()
}

// closes statement
func (s stmt) Close() error {
	if s.cu != nil {
		s.cu.Close()
		s.cu = nil
	}
	return nil
}

// number of input parameters
func (s stmt) NumInput() int {
	bva, bvm := s.cu.GetBindVars()
	if bva != nil {
		return len(bva)
	} else if bvm != nil {
		return len(bvm)
	}
	return 0
}

type rowsRes struct {
	cu   *oracle.Cursor
	cols []oracle.VariableDescription
}

// executes the statement
func (s stmt) run(args []driver.Value) (*rowsRes, error) {
	//A driver Value is a value that drivers must be able to handle.
	//A Value is either nil or an instance of one of these types:
	//int64
	//float64
	//bool
	//[]byte
	//string   [*] everywhere except from Rows.Next.
	//time.Time

	a := (*[]interface{})(unsafe.Pointer(&args))
	if err := s.cu.Execute(s.statement, *a, nil); err != nil {
		return nil, err
	}
	cols, err := s.cu.GetDescription()
	if err != nil {
		return nil, err
	}
	return &rowsRes{cu: s.cu, cols: cols}, nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.run(args)
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.run(args)
}

func (r rowsRes) LastInsertId() (int64, error) {
	return -1, NotImplemented
}

func (r rowsRes) RowsAffected() (int64, error) {
	return int64(r.cu.GetRowCount()), nil
}

// resultset column names
func (r rowsRes) Columns() []string {
	cls := make([]string, len(r.cols))
	for i, c := range r.cols {
		cls[i] = c.Name
	}
	return cls
}

// closes the resultset
func (r rowsRes) Close() error {
	if r.cu != nil {
		r.cu.Close()
		r.cu = nil
	}
	return nil
}

// DATE, DATETIME, TIMESTAMP are treated as they are in Local time zone
func (r rowsRes) Next(dest []driver.Value) error {
	a := (*[]interface{})(unsafe.Pointer(&dest))
	return r.cu.FetchOneInto(*a...)
}

type Driver struct {
	// Defaults
	user, passwd, db string

	initCmds []string
}

// Open new connection. The uri need to have the following syntax:
//
//   USER/PASSWD@SID
//
// SID (database identifier) can be a DSN (see goracle/oracle.MakeDSN)
func (d *Driver) Open(uri string) (driver.Conn, error) {
	p := strings.Index(uri, "/")
	d.user = uri[:p]
	q := strings.Index(uri[p+1:], "@")
	if q < 0 {
		q = len(uri) - 1
	} else {
		q += p + 1
	}
	d.passwd = uri[p+1 : q]
	d.db = uri[q+1:]

	// Establish the connection
	cx, err := oracle.NewConnection(d.user, d.passwd, d.db)
	if err == nil {
		err = cx.Connect(0, false)
	}
	if err != nil {
		return nil, err
	}
	return &conn{cx: &cx}, nil
}

// Driver automatically registered in database/sql
var d = Driver{}

func init() {
	sql.Register("goracle", &d)
}
