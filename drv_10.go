// +build go1.10

// Copyright 2019 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package goracle

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	errors "golang.org/x/xerrors"
)

var _ = driver.Connector((*connector)(nil))

type connector struct {
	ConnectionParams
	*Drv
	onInit func(driver.Conn) error
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Drv) OpenConnector(name string) (driver.Connector, error) {
	P, err := ParseConnString(name)
	if err != nil {
		return nil, err
	}

	return connector{ConnectionParams: P, Drv: d}, nil
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes.
//
// The returned connection is only used by one goroutine at a
// time.
func (c connector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Drv.openConn(c.ConnectionParams)
	if err != nil || c.onInit == nil || !conn.newSession {
		return conn, err
	}
	if err = c.onInit(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c connector) Driver() driver.Driver { return c.Drv }

// NewConnector returns a driver.Connector to be used with sql.OpenDB,
// which calls the given onInit if the connection is new.
func NewConnector(name string, onInit func(driver.Conn) error) (driver.Connector, error) {
	cxr, err := defaultDrv.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	cx := cxr.(connector)
	cx.onInit = onInit
	return cx, err
}

// NewSessionIniter returns a function suitable for use in NewConnector as onInit,
// which calls "ALTER SESSION SET <key>='<value>'" for each element of the given map.
func NewSessionIniter(m map[string]string) func(driver.Conn) error {
	return func(cx driver.Conn) error {
		for k, v := range m {
			qry := fmt.Sprintf("ALTER SESSION SET %s = '%s'", k, strings.Replace(v, "'", "''", -1))
			st, err := cx.Prepare(qry)
			if err != nil {
				return errors.Errorf("%s: %w", qry, err)
			}
			_, err = st.Exec(nil) //lint:ignore SA1019 it's hard to use ExecContext here
			st.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}
}
