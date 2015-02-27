/*
Copyright 2014 Tamás Gulácsi

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

// Package rdbdrv implements a Go Oracle driver for bitbucket.org/kardianos/rdb
package rdbdrv

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"bitbucket.org/kardianos/rdb"
	"github.com/tgulacsi/goracle/oracle"
)

// Interface implementation checks.
var (
	_ = rdb.Driver(&driver{})
	_ = rdb.DriverConn(&conn{})
)

var (
	driverInfo = rdb.DriverInfo{
		DriverSupport: rdb.DriverSupport{
			PreparePerConn: true,
			NamedParameter: true,
			MultipleResult: true,
			Notification:   true,
		},
	}

	pingCommand = &rdb.Command{
		Sql: "SELECT 1 FROM DUAL", Arity: rdb.One,
		Prepare: true, Name: "ping"}
)

type driver struct {
}

// Open a database.
func (d *driver) Open(c *rdb.Config) (rdb.DriverConn, error) {
	if c.Secure {
		return nil, errors.New("Secure connection is not supported.")
	}

	// Establish the connection
	cx, err := oracle.NewConnection(c.Username, c.Password, c.Instance, false)
	if err != nil {
		return nil, err
	}
	return &conn{cx: cx}, nil
}

// Return information about the database driver's capabilities.
// Connection-independent.
func (d driver) DriverInfo() *rdb.DriverInfo {
	return &driverInfo
}

// Return the command to send a NOOP to the server.
func (d driver) PingCommand() *rdb.Command {
	return pingCommand
}

type conn struct {
	cx         *oracle.Connection
	statements []*stmt
	available  int32
}

// Close the underlying connection to the database.
func (c *conn) Close() {
	if c.cx == nil {
		return
	}
	c.cx.Close()
	c.cx = nil
}

// Connectioninfo returns version information regarding the currently connected server.
func (c conn) ConnectionInfo() *rdb.ConnectionInfo {
	// TODO(tgulacsi): proper versions
	return &rdb.ConnectionInfo{Server: nil, Protocol: nil}
}

// True if not currently in a connection pool.
func (c conn) Available() bool {
	return atomic.LoadInt32(&c.available) != 0
}

// Set when adding or removing from connection pool.
func (c *conn) SetAvailable(available bool) {
	var av int32
	if available {
		av = 1
	}
	atomic.StoreInt32(&c.available, av)
}

func (c conn) Status() rdb.DriverConnStatus {
	if c.cx == nil || !c.cx.IsConnected() {
		return rdb.StatusDisconnected
	}
	if len(c.statements) > 0 {
		return rdb.StatusQuery
	}
	return rdb.StatusReady
}

type stmt struct {
	cu         *oracle.Cursor
	query, tag string
	cols       []oracle.VariableDescription
}

// NextQuery stops the active query and gets the connection for the next one.
func (c *conn) NextQuery() (err error) {
	return errors.New("not implemented")
}

// Query executes the query defined in cmd.
// Should return "PreparedTokenNotValid" if the preparedToken was not recognized.
func (c *conn) Query(cmd *rdb.Command, params []rdb.Param, preparedToken interface{}, val rdb.DriverValuer) error {
	st, ok := preparedToken.(stmt)
	if !ok || st.cu == nil {
		return rdb.PreparedTokenNotValid
	}
	var err error
	if len(params) == 0 {
		err = st.cu.Execute(st.query, nil, nil)
	} else if params[0].Name == "" {
		args := make([]interface{}, len(params))
		for i, p := range params {
			args[i] = p.Value
		}
		err = st.cu.Execute(st.query, args, nil)
	} else {
		args := make(map[string]interface{}, len(params))
		for _, p := range params {
			args[p.Name] = p.Value
		}
		err = st.cu.Execute(st.query, nil, args)
	}
	if err != nil {
		return filterErr(err)
	}

	if !st.cu.IsDDL() {
		st.cols, err = st.cu.GetDescription()
		if err != nil {
			return err
		}
		c.statements = append(c.statements, &st)
	}

	return nil
}

// Prepare the query specified in cmd.
func (c *conn) Prepare(cmd *rdb.Command) (preparedToken interface{}, err error) {
	query := cmd.Sql
	if strings.Index(query, ":1") < 0 && strings.Index(query, "?") >= 0 {
		q := strings.Split(query, "?")
		q2 := make([]string, 0, 2*len(q)-1)
		for i := 0; i < len(q); i++ {
			if i > 0 {
				q2 = append(q2, ":"+strconv.Itoa(i))
			}
			q2 = append(q2, q[i])
		}
		query = strings.Join(q2, "")
	}
	cu := c.cx.NewCursor()
	st := stmt{cu: cu, query: query, tag: genQueryTag(query)}
	if err = cu.Prepare(st.query, st.tag); err != nil {
		return nil, err
	}
	return interface{}(st), nil
}

// Unprepare clears the prepared token.
func (c *conn) Unprepare(preparedToken interface{}) error {
	st, ok := preparedToken.(stmt)
	if !ok {
		return nil
	}
	st.cu.Close()
	st.cu = nil
	return nil
}

// NextResult advances to the next result if there are multiple results.
func (c *conn) NextResult() (more bool, err error) {
	if len(c.statements) == 0 {
		return false, errors.New("no more results")
	}
	st := c.statements[0]
	if st.cu != nil {
		st.cu.Close()
		st.cu = nil
	}
	c.statements = c.statements[1:]
	if len(c.statements) == 0 {
		return false, nil
	}
	return true, nil
}

// Scan reads the next row from the connection.
// For each field in the row call the Valuer.WriteField(...) method.
func (c *conn) Scan() error {
	if len(c.statements) == 0 {
		return errors.New("no statement has been executed")
	}
	st := c.statements[0]
	if st.cu == nil {
		return errors.New("cursor is nil")
	}

	row := make([]interface{}, len(st.cols))
	err := st.cu.FetchOneInto(row...)
	if err == io.EOF {
		st.cu.Close()
		st.cu = nil
	}
	return err
}

// Begin a new transaction.
func (c *conn) Begin(iso rdb.IsolationLevel) error {
	if c.cx.IsConnected() {
		return nil
	}
	return filterErr(c.cx.Connect(0, false))
}

// Rollback to savepoint.
func (c *conn) Rollback(savepoint string) error {
	savepoint = filterName(savepoint)
	if savepoint == "" {
		return c.cx.Rollback()
	}
	return c.cx.NewCursor().Execute("ROLLBACK TO "+savepoint, nil, nil)
}

func (c *conn) Commit() error {
	return c.cx.Commit()
}

// SavePoint creates a new savepoint.
func (c *conn) SavePoint(savepoint string) error {
	return c.cx.NewCursor().Execute("SAVEPOINT "+filterName(savepoint), nil, nil)
}

// filterName returns the name filtered to be able to used as a name in Oracle
func filterName(name string) string {
	if name == "" {
		return ""
	}
	if i := strings.IndexByte(name, ' '); i >= 0 {
		name = name[:i]
	}
	i := 0
	name = strings.Map(func(r rune) rune {
		i++
		switch {
		case r >= 'a' && r < 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return 'a' + (r - 'A')
		case r >= '0' && r <= '9':
			if i > 1 {
				return r
			}
		}
		return -1
	}, name)
	if len(name) > 30 {
		return name[:30]
	}
	return name
}

// filterErr filters the error, returns driver.ErrBadConn if appropriate
func filterErr(err error) error {
	if err == nil {
		return nil
	}
	if oraErr, ok := err.(*oracle.Error); ok {
		switch oraErr.Code {
		case 115, 451, 452, 609, 1090, 1092, 1073, 3113, 3114, 3135, 3136, 12153, 12161, 12170, 12224, 12230, 12233, 12510, 12511, 12514, 12518, 12526, 12527, 12528, 12539: //connection errors - try again!
			return errors.New("bad connection")
		}
	}
	return err
}

func genQueryTag(query string) string {
	if query == "" {
		return ""
	}
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, _ = w.Write([]byte(query))
	_ = w.Close()
	return b.String()
}
