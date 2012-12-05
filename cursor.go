// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const int bindInfo_elementSize = (sizeof(char*) + sizeof(ub1) + sizeof(char*) + sizeof(ub1) +
            sizeof(ub1) + sizeof(OCIBind*));
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	"unsafe"
)

func init() {
	log.Printf("bindInfo_elementSize=%d", C.bindInfo_elementSize)
}

type Cursor struct {
	handle                                      *C.OCIStmt
	connection                                  *Connection
	environment                                 *Environment
	bindVariables, fetchVariables               []*Variable
	arraySize, bindArraySize, fetchArraySize    int
	setInputSizes, outputSize, outputSizeColumn int
	rowCount, actualRows, rownum                int
	statement                                   []byte
	statementTag                                int
	statementType                               int
	numbersAsStrings, isDML, isOpen, isOwned    bool
}

var DefaultArraySize int = 50

//statement // statementTag // rowFactory // inputTypeHandler // outputTypeHandler

//   Allocate a new handle.
func (cur *Cursor) allocateHandle(typ C.ub4) *Error {
	cur.isOwned = true
	return ociHandleAlloc(unsafe.Pointer(cur.environment.handle),
		(unsafe.Pointer)(unsafe.Pointer(&cur.handle)))
}

//   Free the handle which may be reallocated if necessary.
func (cur *Cursor) freeHandle() *Error {
	if cur.handle == nil {
		return nil
	}
	if cur.isOwned {
		return CheckStatus(C.OCIHandleFree(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT),
			"freeCursor")
	} else if cur.connection.handle != nil {
		return CheckStatus(C.OCIStmtRelease(unsafe.Pointer(cur.handle),
			cur.environment.errorHandle, (*C.text)(&cur.statementTag[0]),
			len(cur.statementTag), C.OCI_DEFAULT))
	}
	cur.handle = nil
}

func (cur *Cursor) IsOpen() bool {
	if !cur.isOpen {
		return false
	}
	return cur.connection.IsConnected()
}

// Creates new cursor
func NewCursor(conn *Connection) *Cursor {
	return &Cursor{connection: conn, environment: conn.environment,
		arraySize: DefaultArraySize, fetchArraySize: DefaultArraySize,
		bindArraySize: 1, statementType: -1, outputSize: -1, outputSizeColumn: -1,
		isOpen: true}
}

func (cur Cursor) String() {
	return fmt.Sprintf("<goracle.Cursor on %s>", cur.connection)
}

//   Return a list of bind variable names. At this point the cursor must have
// already been prepared.
func (cur *Cursor) GetBindNames(numElements int) (names []string, err error) {
	// ensure that a statement has already been prepared
	if cur.statement == nil {
		err = errors.New("statement must be prepared first!")
	}

	// avoid bus errors on 64-bit platforms
	// numElements = numElements + (sizeof(void*) - numElements % sizeof(void*));

	// initialize the buffers
	buffer := make([]byte, numElements*int(C.bindInfo_elementSize))
	bindNames := (*unsafe.Pointer)(unsafe.Pointer(&buffer[0]))
	bindNameLengths := &buffer[0+numElements]
	indicatorNames := (*unsafe.Pointer)(unsafe.Pointer(&buffer[1*numElements+numElements]))
	indicatorNameLengths := &buffer[2*numElements+numElements]
	duplicate := (*unsafe.Pointer)(unsafe.Pointer(&buffer[3*numElements+numElements]))
	bindHandles := (*C.OCIBind)(unsafe.Pointer(&buffer[4*numElements+numElements]))

	// get the bind information
	status := C.OCIStmtGetBindInfo(cur.handle,
		cur.environment.errorHandle, numElements, 1, &foundElements,
		(**C.text)(bindNames), bindNameLengths, (**C.text)(indicatorNames),
		indicatorNameLengths, duplicate, bindHandles)
	if status != C.OCI_NO_DATA {
		if e := CheckStatus(status, "GetBindInfo"); e != nil {
			return e
		}
	}
	if foundElements < 0 {
		return NewError(-foundElements, "negative foundElements")
	}

	// create the list which is to be returned
	names = make([]string, 0, foundElements)
	// process the bind information returned
	for i = 0; i < foundElements; i++ {
		if duplicate[i] > 0 {
			continue
		}
		names = append(names, cur.environment.FromEncodedString(bindNames[i][:bindNameLengths[i]]))
	}

	return nil
}

//-----------------------------------------------------------------------------
//   Perform the defines for the cursor. At this point it is assumed that the
// statement being executed is in fact a query.
//-----------------------------------------------------------------------------
func (cur *Cursor) PerformDefine() error {
	var numParams, pos int

	// determine number of items in select-list
	if err := CheckStatus(C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		unsafe.Pointer(&numParams), 0,
		C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
		"PerformDefine"); err != nil {
		return err
	}

	// create a list corresponding to the number of items
	cur.fetchVariables = make([]*Variable, numParams)

	// define a variable for each select-item
	cur.fetchArraySize = cur.arraySize
	for pos := 1; pos <= numParams; pos++ {
		v, e := defineVariable(cur, cur.fetchArraySize, pos)
		if e != nil {
			return e
		}
		cur.fetchVariables[pos-1] = v
	}
}
