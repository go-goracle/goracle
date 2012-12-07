// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const int sizeof_OraText = sizeof(OraText);
*/
import "C"

import (
	"errors"
	"fmt"
	// "log"
	// "reflect"
	"unsafe"
)

func init() {
	// log.Printf("bindInfo_elementSize=%d", C.bindInfo_elementSize)
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
	statementTag                                []byte
	statementType                               int
	numbersAsStrings, isDML, isOpen, isOwned    bool
}

var DefaultArraySize int = 50

//statement // statementTag // rowFactory // inputTypeHandler // outputTypeHandler

//   Allocate a new handle.
func (cur *Cursor) allocateHandle(typ C.ub4) *Error {
	cur.isOwned = true
	return ociHandleAlloc(unsafe.Pointer(cur.environment.handle),
		C.OCI_HTYPE_STMT,
		(*unsafe.Pointer)(unsafe.Pointer(&cur.handle)),
		"allocate statement handle")
}

//   Free the handle which may be reallocated if necessary.
func (cur *Cursor) freeHandle() *Error {
	if cur.handle == nil {
		return nil
	}
	if cur.isOwned {
		return cur.environment.CheckStatus(C.OCIHandleFree(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT),
			"freeCursor")
	} else if cur.connection.handle != nil {
		return cur.environment.CheckStatus(C.OCIStmtRelease(cur.handle,
			cur.environment.errorHandle, (*C.OraText)(&cur.statementTag[0]),
			C.ub4(len(cur.statementTag)), C.OCI_DEFAULT),
			"statement release")
	}
	cur.handle = nil
	return nil
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

func (cur Cursor) String() string {
	return fmt.Sprintf("<goracle.Cursor on %s>", cur.connection)
}

//   Return a list of bind variable names. At this point the cursor must have
// already been prepared.
func (cur *Cursor) GetBindNames(numElements int) (names []string, err error) {
	// ensure that a statement has already been prepared
	if cur.statement == nil {
		err = errors.New("statement must be prepared first!")
	}
	var foundElements C.sb4

	// avoid bus errors on 64-bit platforms
	// numElements = numElements + (sizeof(void*) - numElements % sizeof(void*));

	// initialize the buffers
	// buffer := make([]byte, numElements*int(C.bindInfo_elementSize))
	// bindNames := (**C.OraText)(unsafe.Pointer(&buffer[0]))
	bindNames := make([](*C.OraText), numElements)
	// bindNameLengths := (*C.ub1)(&buffer[0+numElements])
	bindNameLengths := make([]byte, numElements)
	// indicatorNames := (**C.OraText)(unsafe.Pointer(&buffer[1*numElements+numElements]))
	indicatorNames := make([](*C.OraText), numElements)
	// indicatorNameLengths := (*C.ub1)(&buffer[2*numElements+numElements])
	indicatorNameLengths := make([]byte, numElements)
	// duplicate := (*C.ub1)(unsafe.Pointer(&buffer[3*numElements+numElements]))
	duplicate := make([]byte, numElements)
	// bindHandles := (**C.OCIBind)(unsafe.Pointer(&buffer[4*numElements+numElements]))
	bindHandles := make([](*C.OCIBind), numElements)

	// get the bind information
	status := C.OCIStmtGetBindInfo(cur.handle,
		cur.environment.errorHandle, C.ub4(numElements), 1, &foundElements,
		(**C.OraText)(unsafe.Pointer(&bindNames[0])),
		(*C.ub1)(&bindNameLengths[0]),
		(**C.OraText)(&indicatorNames[0]), (*C.ub1)(&indicatorNameLengths[0]),
		(*C.ub1)(&duplicate[0]), (**C.OCIBind)(&bindHandles[0]))
	if status != C.OCI_NO_DATA {
		if e := cur.environment.CheckStatus(status, "GetBindInfo"); e != nil {
			return nil, e
		}
	}
	if foundElements < 0 {
		return nil, NewError(-int(foundElements), "negative foundElements")
	}

	// create the list which is to be returned
	names = make([]string, 0, foundElements)
	// process the bind information returned
	for i := 0; i < int(foundElements); i++ {
		if duplicate[i] > 0 {
			continue
		}
		names = append(names, FromOraText(bindNames[i], int(bindNameLengths[i])))
	}

	return names, nil
}

//-----------------------------------------------------------------------------
//   Perform the defines for the cursor. At this point it is assumed that the
// statement being executed is in fact a query.
//-----------------------------------------------------------------------------
func (cur *Cursor) PerformDefine() error {
	var numParams int
	var x C.ub4 = 0

	// determine number of items in select-list
	if err := cur.environment.CheckStatus(C.OCIAttrGet(
		unsafe.Pointer(cur.handle),
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&numParams), &x,
		C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
		"PerformDefine"); err != nil {
		return err
	}

	// create a list corresponding to the number of items
	cur.fetchVariables = make([]*Variable, numParams)

	// define a variable for each select-item
	cur.fetchArraySize = cur.arraySize
	for pos := 1; pos <= numParams; pos++ {
		// FIXME defineVariable
		// v, e := defineVariable(cur, cur.fetchArraySize, pos)
		var v *Variable
		var e error
		v, e = nil, nil
		if e != nil {
			return e
		}
		cur.fetchVariables[pos-1] = v
	}
	return nil
}

func FromOraText(textp *C.OraText, length int) string {
	/*
        var theGoSlice []TheCType
        sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
        sliceHeader.Cap = length
        sliceHeader.Len = length
        sliceHeader.Data = uintptr(unsafe.Pointer(&theCArray[0]))
        // now theGoSlice is a normal Go slice backed by the C array
    */

	return string(C.GoBytes(unsafe.Pointer(textp), C.int(C.sizeof_OraText*length)))
}
