// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
// "unsafe"
)

type Variable struct {
	bindHandle        *C.OCIBind
	defineHandle      *C.OCIDefine
	boundCursorHandle *C.OCIStmt
	//PyObject*boundName;
	//PyObject*inConverter;
	//PyObject*outConverter;
	boundPos, allocatedElements, actualElements, internalFetchNum, size, bufferSize int
	environment                                                                     *Environment
	isArray, isAllocatedInternally                                                  bool
	indicator                                                                       *C.sb2
	returnCode, actualLength                                                        *C.ub2
	data                                                                            []byte
}

type VariableDescription struct {
	Name string
	Type, InternalSize, DisplaySize, Precision, Scale int
	NullOk bool
}

//   Returns a boolean indicating if the object is a variable.
func isVariable(value interface{}) bool {
	//FIXME
	if _, ok := value.(StringVar); ok {
		return true
	}
	return false
	/*
    return (Py_TYPE(object) == &g_CursorVarType ||
            Py_TYPE(object) == &g_DateTimeVarType ||
            Py_TYPE(object) == &g_BFILEVarType ||
            Py_TYPE(object) == &g_BLOBVarType ||
            Py_TYPE(object) == &g_CLOBVarType ||
            Py_TYPE(object) == &g_LongStringVarType ||
            Py_TYPE(object) == &g_LongBinaryVarType ||
            Py_TYPE(object) == &g_NumberVarType ||
            Py_TYPE(object) == &g_StringVarType ||
            Py_TYPE(object) == &g_FixedCharVarType ||
            Py_TYPE(object) == &g_NCLOBVarType ||
#if PY_MAJOR_VERSION < 3
            Py_TYPE(object) == &g_UnicodeVarType ||
            Py_TYPE(object) == &g_FixedUnicodeVarType ||
#endif
            Py_TYPE(object) == &g_RowidVarType ||
            Py_TYPE(object) == &g_BinaryVarType ||
            Py_TYPE(object) == &g_TimestampVarType ||
            Py_TYPE(object) == &g_IntervalVarType
#ifdef SQLT_BFLOAT
            || Py_TYPE(object) == &g_NativeFloatVarType
#endif
            );
     */
}

