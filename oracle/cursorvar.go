package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const unsigned int sof_OCIStmtp = sizeof(OCIStmt*);

static void cursorVar_setHandle(void *data, OCIStmt *handle) {
	data = handle;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

var CursorVarType *VariableType

// Initialize the variable.
func cursorVar_Initialize(v *Variable, cur *Cursor) error {
	var tempCursor *Cursor
	var err error

	v.connection = cur.connection
	v.cursors = make([]*Cursor, v.allocatedElements)
	for i := uint(0); i < v.allocatedElements; i++ {
		tempCursor = v.connection.NewCursor()
		if err = tempCursor.allocateHandle(); err != nil {
			return err
		}
		C.cursorVar_setHandle(unsafe.Pointer(&v.dataBytes[i]), tempCursor.handle)
	}

	return nil
}

// Prepare for variable destruction.
func cursorVar_Finalize(v *Variable) error {
	v.connection = nil
	v.cursors = nil
	return nil
}

// Set the value of the variable.
func cursorVar_SetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.(*Cursor)
	if !ok {
		return fmt.Errorf("requires *Cursor, got %T", value)
	}

	var err error
	v.cursors[pos] = x
	if !x.isOwned {
		if err = x.freeHandle(); err != nil {
			return err
		}
		x.isOwned = true
		if err = x.allocateHandle(); err != nil {
			return err
		}
	}
	C.cursorVar_setHandle(unsafe.Pointer(&v.dataBytes[pos]), x.handle)
	x.statementType = -1
	return nil
}

// Set the value of the variable.
func cursorVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	cur := v.cursors[pos]
	cur.statementType = -1
	return cur, nil
}

func init() {
	CursorVarType = &VariableType{
		initialize:  cursorVar_Initialize,
		finalize:    cursorVar_Finalize,
		setValue:    cursorVar_SetValue,
		getValue:    cursorVar_GetValue,
		oracleType:  C.SQLT_RSET,          // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,     // charset form
		size:        uint(C.sof_OCIStmtp), // element length
	}
}
