/*
Package godrv implements a Go Oracle driver

Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
	"log"
	// "reflect"
	"strings"
	// "time"
	"unsafe"
)

var (
	// NotImplemented prints Not implemented
	NotImplemented = errors.New("Not implemented")
	// IsDebug should we print debug logs?
	IsDebug bool
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
	debug("%p.Prepare(%s)", cu, query)
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
func (c conn) Begin() (tx driver.Tx, err error) {
	if !c.cx.IsConnected() {
		if err = c.cx.Connect(0, false); err != nil {
			return
		}
	}
	tx = tx{cx: c.cx}
	return
}

// commits currently opened transaction
func (t tx) Commit() error {
	if t.cx != nil {
		return t.cx.Commit()
	}
	return nil
}

// rolls back current transaction
func (t tx) Rollback() error {
	if t.cx != nil {
		return t.cx.Rollback()
	}
	return nil
}

// closes statement
func (s stmt) Close() error {
	if s.cu != nil {
		debug("CLOSEing statement %p (%s)", s.cu, s.statement)
		s.cu.Close()
		s.cu = nil
	}
	return nil
}

// number of input parameters
func (s stmt) NumInput() int {
	names, err := s.cu.GetBindNames()
	if err != nil {
		log.Printf("error getting bind names of %p: %s", s.cu, err)
		return -1
	}
	return len(names)
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

	var err error
	a := (*[]interface{})(unsafe.Pointer(&args))
	debug("%p.run(%s, %v)", s.cu, s.statement, *a)
	if err = s.cu.Execute(s.statement, *a, nil); err != nil {
		return nil, err
	}

	var cols []oracle.VariableDescription
	if !s.cu.IsDDL() {
		cols, err = s.cu.GetDescription()
		debug("cols: %+v err: %s", cols, err)
		if err != nil {
			return nil, err
		}
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
		debug("CLOSEing result %p", r.cu)
		// r.cu.Close() // FIXME
		r.cu = nil
	}
	return nil
}

// DATE, DATETIME, TIMESTAMP are treated as they are in Local time zone
func (r rowsRes) Next(dest []driver.Value) error {
	row := (*[]interface{})(unsafe.Pointer(&dest))
	// log.Printf("FetcOneInto(%p %+v len=%d) %T", row, *row, len(*row), *row)
	err := r.cu.FetchOneInto(*row...)
	debug("fetched row=%p %+v (len=%d) err=%s", row, *row, len(*row), err)
	return err
}

// Driver implements a Driver
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

// use log.Printf for log messages if IsDebug
func debug(fmt string, args ...interface{}) {
	if IsDebug {
		log.Printf(fmt, args...)
	}
}

// Driver automatically registered in database/sql
var d = Driver{}

func init() {
	sql.Register("goracle", &d)
}
