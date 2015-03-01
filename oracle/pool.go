/*
Copyright 2015 Tamás Gulácsi

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

package oracle

import (
	"fmt"
	"runtime"
	"sync"
)

// Pool is a simple Pool for connections.
type Pool struct {
	user, passw, sid string
	pool             *sync.Pool
}

// NewPool returns a new pool.
func NewPool(dsn string) Pool {
	p := Pool{pool: &sync.Pool{}}
	p.user, p.passw, p.sid = SplitDSN(dsn)
	return p
}

// Get an old/new connection.
func (p Pool) Get() (*Connection, error) {
	cx := p.pool.Get()
	if cx != nil {
		return cx.(*Connection), nil
	}
	conn, err := NewConnection(p.user, p.passw, p.sid, false)
	if err != nil {
		return conn, err
	}
	runtime.SetFinalizer(conn, func(cx *Connection) {
		if cx != nil {
			Log.Warn("Finalizer closes connection " + fmt.Sprintf("%v", cx))
			cx.Close()
		}
	})
	return conn, nil
}

// Put the connection back to the pool.
func (p Pool) Put(cx *Connection) {
	if cx == nil {
		return
	}
	_ = cx.Rollback()
	p.pool.Put(cx)
}
