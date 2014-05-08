package oracle

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

/*

#cgo LDFLAGS: -lclntsh

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <oci.h>

const unsigned int sof_OCIStmtp = sizeof(OCIStmt*);

static void cursorVarsetHandle(void *data, OCIStmt *handle) {
    memcpy(data, &handle, sof_OCIStmtp);
}
*/
import "C"

import (
	"unsafe"

	"github.com/juju/errgo"
)

//CursorVarType is the VariableType for a cursor
var CursorVarType *VariableType

// cursorVarInitialize Initializes the (cursor) variable.
func cursorVarInitialize(v *Variable, cur *Cursor) error {
	var tempCursor *Cursor
	var err error
	var j int

	v.connection = cur.connection
	v.cursors = make([]*Cursor, v.allocatedElements)
	debug("cursorVarInitialize conn=%x ae=%d typ.Name=%s\n", v.connection, v.allocatedElements, v.typ.Name)
	for i := uint(0); i < v.allocatedElements; i++ {
		tempCursor = v.connection.NewCursor()
		if err = tempCursor.allocateHandle(); err != nil {
			return errgo.Mask(err)
		}
		v.cursors[int(i)] = tempCursor
		j = int(i * v.typ.size)
		C.cursorVarsetHandle(unsafe.Pointer(&v.dataBytes[j]),
			tempCursor.handle)
		debug("set position %d(%d) in dataBytes to %x [%s]", i, j,
			v.dataBytes[j:j+int(v.typ.size)], v.typ.size)
	}

	return nil
}

// Prepare for variable destruction.
func cursorVarFinalize(v *Variable) error {
	v.connection = nil
	v.cursors = nil
	return nil
}

// Set the value of the variable.
func cursorVarSetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.(*Cursor)
	if !ok {
		return errgo.Newf("requires *Cursor, got %T", value)
	}
	if uint(len(v.cursors)) <= pos {
		return errgo.Newf("can't set cursor at pos %d in array of %d length",
			pos, len(v.cursors))
	}

	var err error
	v.cursors[pos] = x
	if !x.isOwned {
		if err = x.freeHandle(); err != nil {
			return errgo.Mask(err)
		}
		x.isOwned = true
		if err = x.allocateHandle(); err != nil {
			return errgo.Mask(err)
		}
	}
	C.cursorVarsetHandle(v.getHandle(pos),
		x.handle)

	x.statementType = -1
	return nil
}

// Set the value of the variable.
func cursorVarGetValue(v *Variable, pos uint) (interface{}, error) {
	if v == nil {
		return nil, errgo.New("variable is nil")
	}
	if v.cursors == nil {
		return nil, errgo.New("v.cursors is nil")
	}
	if uint(len(v.cursors)) <= pos {
		return nil, errgo.Newf("can't get cursor at pos %d from array of %d length",
			pos, len(v.cursors))
	}
	debug("cursorVarGetValue(%v, %d): v.cursors=%v", v, pos, v.cursors)
	cur := v.cursors[pos]
	cur.statementType = -1
	return cur, nil
}

func init() {
	CursorVarType = &VariableType{
		Name:        "cursor",
		initialize:  cursorVarInitialize,
		finalize:    cursorVarFinalize,
		setValue:    cursorVarSetValue,
		getValue:    cursorVarGetValue,
		oracleType:  C.SQLT_RSET,          // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,     // charset form
		size:        uint(C.sof_OCIStmtp), // element length
	}
}
