/*
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
//"github.com/tgulacsi/goracle/oracle"
)

// ColumnDescriber interface allows the column's description
type ColumnDescriber interface {
	// DescribeColumn returns the column description
	DescribeColumns() []ColDesc
}

// ColDesc is a column's description
type ColDesc struct {
	// Name is the name of the column
	Name string

	// TypeName is the name of the type of the column
	TypeName string

	// DisplaySize is the display (char/rune) size
	DisplaySize int

	// InternalSize is the byte size
	InternalSize int

	// Precision is the number of all digits this number-like column can hold
	Precision int

	// Scale is the number of digits after the point
	Scale int

	// Nullable is true if the column can be null
	Nullable bool
}

func (r rowsRes) DescribeColumns() []ColDesc {
	cls := make([]ColDesc, len(r.cols))
	for i, c := range r.cols {
		cls[i] = ColDesc{Name: c.Name, TypeName: "",
			DisplaySize:  c.DisplaySize,
			InternalSize: c.InternalSize,
			Precision:    c.Precision,
			Scale:        c.Scale,
			Nullable:     c.NullOk}
	}
	return cls
}
