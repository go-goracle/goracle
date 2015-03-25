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

package godrv

import (
	"database/sql/driver"

	"gopkg.in/errgo.v1"
	"gopkg.in/goracle.v1/oracle"
)

type varCreator interface {
	NewVar(interface{}) (*oracle.Variable, error)
}

// NewVar calls NewVar on the underlying *oracle.Cursor.
// This allows out binds, and if value is a pointer, then GetValue is not needed.
func NewVar(stmt driver.Stmt, value interface{}) (*oracle.Variable, error) {
	if stmt, ok := stmt.(varCreator); ok {
		return stmt.NewVar(value)
	}
	return nil, errgo.New("stmt must be a varCreator")
}
