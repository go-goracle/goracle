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
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const unsigned int sof_OCILobLocatorp = sizeof(OCILobLocator*); // element length
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

var (
	//ClobVarType is a VariableType for CLOB
	ClobVarType *VariableType
	//NClobVarType is a VariableType for NCLOB
	NClobVarType *VariableType
	//BlobVarType is a VariableType for BLOB
	BlobVarType *VariableType
	//BFileVarType is a VariableType for BFILE
	BFileVarType *VariableType
)

// Initialize the variable.
func lobVarInitialize(v *Variable, cur *Cursor) error {
	// initialize members
	v.connection = cur.connection
	// v.isFile = v.typ == BFileVarType

	// initialize the LOB locators
	var err error
	for i := uint(0); i < v.allocatedElements; i++ {
		if err = v.environment.CheckStatus(
			C.OCIDescriptorAlloc(unsafe.Pointer(v.environment.handle),
				(*unsafe.Pointer)(unsafe.Pointer(&v.dataBytes[i*v.typ.size])),
				C.OCI_DTYPE_LOB, 0, nil),
			"DescrAlloc"); err != nil {
			return err
		}
	}

	return nil
}

// Free temporary LOBs prior to fetch.
func lobVarPreFetch(v *Variable) error {
	var isTemporary C.boolean

	var err error
	var j int
	for i := uint(0); i < v.allocatedElements; i++ {
		j = int(i * v.typ.size)
		if v.dataBytes != nil && len(v.dataBytes) > j && v.dataBytes[j] != 0 {
			if err = v.environment.CheckStatus(
				C.OCILobIsTemporary(v.environment.handle,
					v.environment.errorHandle,
					(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[j])),
					&isTemporary),
				"LobIsTemporary"); err != nil {
				return err
			}
			if isTemporary == C.TRUE {
				// Py_BEGIN_ALLOW_THREADS
				if err = v.environment.CheckStatus(
					C.OCILobFreeTemporary(v.connection.handle,
						v.environment.errorHandle,
						(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[j]))),
					"LobFreeTemporary"); err != nil {
					return err
				}
				// Py_END_ALLOW_THREADS
			}
		}
	}

	return nil
}

// Prepare for variable destruction.
func lobVarFinalize(v *Variable) error {
	var j int
	for i := uint(0); i < v.allocatedElements; i++ {
		j = int(i * v.typ.size)
		if v.dataBytes != nil && len(v.dataBytes) > j && v.dataBytes[j] != 0 {
			C.OCIDescriptorFree(unsafe.Pointer(&v.dataBytes[j]), C.OCI_DTYPE_LOB)
		}
	}
	return nil
}

// Write data to the LOB variable.
func (v *Variable) lobVarWrite(data []byte, pos uint, off int64) (amount int, err error) {
	if !(v.typ == BlobVarType || v.typ == ClobVarType ||
		v.typ == NClobVarType || v.typ == BFileVarType) {
		return 0, fmt.Errorf("only LOBs an be written into, not %T", v.typ)
	}

	amount = len(data)
	// verify the data type
	if v.typ == BFileVarType {
		return 0, errors.New("BFILEs are read only")
	}
	if v.typ == BlobVarType {
		// amount = len(data)
		/*
			#if PY_MAJOR_VERSION < 3
			    } else if (var->type == &vt_NCLOB) {
			        if (cxBuffer_FromObject(&buffer, dataObj,
			                var->environment->nencoding) < 0)
			            return -1;
			        *amount = buffer.size;
			#endif
		*/
	} else {
		if v.environment.FixedWidth && (v.environment.MaxBytesPerCharacter > 1) {
			amount /= int(v.environment.MaxBytesPerCharacter)
		} else {
			// amount = len(p)
		}
	}

	// nothing to do if no data to write
	if amount == 0 {
		return 0, nil
	}

	oamount := C.ub4(amount)
	// Py_BEGIN_ALLOW_THREADS
	if err := v.environment.CheckStatus(
		C.OCILobWrite(v.connection.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[pos*v.typ.size])),
			&oamount, C.ub4(off),
			unsafe.Pointer(&data[0]), C.ub4(len(data)),
			C.OCI_ONE_PIECE, nil, nil, 0,
			v.typ.charsetForm),
		"LobWrite"); err != nil {
		return 0, err
	}
	amount = int(oamount)
	// Py_END_ALLOW_THREADS
	return int(oamount), nil
}

// Returns the value stored at the given array position.
func lobVarGetValue(v *Variable, pos uint) (interface{}, error) {
	return NewExternalLobVar(v, pos), nil
}

func lobVarGetValueInto(v *Variable, pos uint, lv *ExternalLobVar) error {
	*lv = *NewExternalLobVar(v, pos)
	return nil
}

// Sets the value stored at the given array position.
func lobVarSetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("requires []byte, got %T", value)
	}
	var (
		isTemporary C.boolean
		lobType     C.ub1
		err         error
	)
	j := pos * v.typ.size

	// make sure have temporary LOBs set up
	if err = v.environment.CheckStatus(
		C.OCILobIsTemporary(v.environment.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[j])), &isTemporary),
		"LobIsTemporary"); err != nil {
		return err
	}
	if isTemporary != C.TRUE {
		if v.typ.oracleType == C.SQLT_BLOB {
			lobType = C.OCI_TEMP_BLOB
		} else {
			lobType = C.OCI_TEMP_CLOB
		}
		// Py_BEGIN_ALLOW_THREADS
		if err = v.environment.CheckStatus(
			C.OCILobCreateTemporary(v.connection.handle,
				v.environment.errorHandle,
				(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[j])),
				C.OCI_DEFAULT, v.typ.charsetForm, lobType, C.FALSE,
				C.OCI_DURATION_SESSION),
			"LobCreateTemporary"); err != nil {
			// Py_END_ALLOW_THREADS
			return err
		}
	}

	// trim the current value
	// Py_BEGIN_ALLOW_THREADS
	if err = v.environment.CheckStatus(
		C.OCILobTrim(v.connection.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[j])), 0),
		"LobTrim"); err != nil {
		return err
	}
	// Py_END_ALLOW_THREADS

	// set the current value
	// func (v *Variable) lobVarWrite(data []byte, pos uint, off int64) (amount int, err error) {
	_, err = v.lobVarWrite(x, pos, 0)
	return err
}

func init() {
	ClobVarType = &VariableType{
		initialize:  lobVarInitialize,
		finalize:    lobVarFinalize,
		preFetch:    lobVarPreFetch,
		setValue:    lobVarSetValue,
		getValue:    lobVarGetValue,
		oracleType:  C.SQLT_CLOB,                // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  true,                       // is character data
	}

	NClobVarType = &VariableType{
		initialize:  lobVarInitialize,
		finalize:    lobVarFinalize,
		preFetch:    lobVarPreFetch,
		setValue:    lobVarSetValue,
		getValue:    lobVarGetValue,
		oracleType:  C.SQLT_CLOB,                // Oracle type
		charsetForm: C.SQLCS_NCHAR,              // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  true,                       // is character data
	}

	BlobVarType = &VariableType{
		initialize:  lobVarInitialize,
		finalize:    lobVarFinalize,
		preFetch:    lobVarPreFetch,
		setValue:    lobVarSetValue,
		getValue:    lobVarGetValue,
		oracleType:  C.SQLT_BLOB,                // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  false,                      // is character data
	}

	BFileVarType = &VariableType{
		initialize:  lobVarInitialize,
		finalize:    lobVarFinalize,
		preFetch:    lobVarPreFetch,
		setValue:    lobVarSetValue,
		getValue:    lobVarGetValue,
		oracleType:  C.SQLT_BFILE,               // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  false,                      // is character data
	}

}
