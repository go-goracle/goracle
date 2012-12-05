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
