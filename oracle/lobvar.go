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

package oracle

/*

#cgo LDFLAGS: -lclntsh

//#include <stdlib.h>
#include <stdio.h>
#include <oci.h>

const unsigned int sof_OCILobLocatorp = sizeof(OCILobLocator*); // element length

OCILobLocator *getLobLoc(void *data, int pos) {
    return ((OCILobLocator**)data)[pos];
}

sword lobAlloc(OCIEnv *envhp, void *data, int allocatedElements) {
    int i;
    sword status;

	for (i = 0; i < allocatedElements; i++) {
        //fprintf(stderr, "=== data[%d]=%p\n", i, ((OCILobLocator**)data)[i]);
		if ((status = OCIDescriptorAlloc(envhp,
                (void**)((OCILobLocator**)data + i),
                OCI_DTYPE_LOB, 0, NULL)) != OCI_SUCCESS) {
            return status;
        }
        //fprintf(stderr, "=== data[%d]=%p\n", i, ((OCILobLocator**)data)[i]);
    }
    return status;
}
*/
import "C"

import (

	//"runtime"
	"unsafe"

	"github.com/juju/errgo"
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
	if CTrace {
		ctrace("%s.lobVarInitialize", v)
	}
	// initialize members
	v.connection = cur.connection
	// v.isFile = v.typ == BFileVarType

	// initialize the LOB locators
	//dst := unsafe.Pointer(&v.dataBytes[0])
	if err := v.environment.CheckStatus(
		C.lobAlloc(v.environment.handle, unsafe.Pointer(&v.dataBytes[0]), //unsafe.Pointer(&dst),
			C.int(v.allocatedElements)),
		"DescrAlloc"); err != nil {
		return errgo.Mask(

			/*
				var (
					x   **C.OCILobLocator
					err error
				)
				for i := uint(0); i < v.allocatedElements; i++ {
					x = (**C.OCILobLocator)(v.getHandle(i))
					if err = v.environment.CheckStatus(
						C.OCIDescriptorAlloc(unsafe.Pointer(v.environment.handle),
							(*unsafe.Pointer)(unsafe.Pointer(x)), C.OCI_DTYPE_LOB, 0, nil),
						"DescrAlloc"); err != nil {
						return err
					}
					if CTrace {
						ctrace("lobVarInitialize(x=%p (%x))", x, x)
					}
					v.setHandle(i, unsafe.Pointer(*x))
					if CTrace {
						ctrace("lobVarInitialize(env=%p, i=%d, lob=%x)",
							v.environment.handle, i, v.getHandleBytes(i))
					}
				}
			*/err)
	}

	return nil
}

func (v *Variable) getLobLoc(pos uint) (*C.OCILobLocator, error) {
	switch v.typ {
	case ClobVarType, NClobVarType, BlobVarType, BFileVarType:
	default:
		return nil, errgo.Newf("getLobLoc is usable only for LOB vars, not for %s", v.typ.Name)
	}
	return C.getLobLoc(unsafe.Pointer(&v.dataBytes[0]), C.int(pos)), nil
}

func (v *Variable) getLobInternalSize(pos uint) (length C.ub4, err error) {
	switch v.typ {
	case ClobVarType, NClobVarType, BlobVarType, BFileVarType:
	default:
		return 0, errgo.Newf("getLobInternalSize is usable only for LOB vars! not for %T", v.typ)
	}
	lob, _ := v.getLobLoc(pos)
	// Py_BEGIN_ALLOW_THREADS
	if CTrace {
		ctrace("OCILobGetLength(conn=%p, pos=%d lob=%x, &length=%p)",
			v.connection.handle, pos*v.typ.size,
			v.getHandleBytes(pos), &length)
		//buf := make([]byte, 8192)
		//ctrace("Stack: %s", buf[:runtime.Stack(buf, false)])
		//ctrace("data[%d]=%p", pos, lob)
	}
	if err = v.environment.CheckStatus(
		C.OCILobGetLength(v.connection.handle,
			v.environment.errorHandle, lob,
			&length),
		"LobGetLength"); err != nil {
		return
	}
	// Py_END_ALLOW_THREADS

	return
}

// Free temporary LOBs prior to fetch.
func lobVarPreFetch(v *Variable) (err error) {
	if v.dataBytes == nil {
		return
	}

	var (
		isTemporary C.boolean
		hndl        *C.OCILobLocator
	)
	for i := uint(0); i < v.allocatedElements; i++ {
		//hndl = (*C.OCILobLocator)(v.getHandle(i))
		if hndl, err = v.getLobLoc(i); err != nil {
			return
		}
		if hndl == nil {
			continue
		}
		if err = v.environment.CheckStatus(
			C.OCILobIsTemporary(v.environment.handle,
				v.environment.errorHandle, hndl,
				&isTemporary),
			"LobIsTemporary"); err != nil {
			return
		}
		if isTemporary == C.TRUE {
			// Py_BEGIN_ALLOW_THREADS
			if err = v.environment.CheckStatus(
				C.OCILobFreeTemporary(v.connection.handle,
					v.environment.errorHandle,
					hndl),
				"LobFreeTemporary"); err != nil {
				return
			}
			// Py_END_ALLOW_THREADS
		}
	}

	return
}

// Prepare for variable destruction.
func lobVarFinalize(v *Variable) error {
	if v == nil || v.dataBytes == nil {
		return nil
	}
	for i := uint(0); i < v.allocatedElements; i++ {
		if CTrace {
			ctrace("lobVarFinalize(lob=%x)", v.getHandle(i))
		}
		C.OCIDescriptorFree(v.getHandle(i), C.OCI_DTYPE_LOB)
	}
	return nil
}

// Write data to the LOB variable.
func (v *Variable) lobVarWrite(data []byte, pos uint, off int64) (amount int, err error) {
	if !(v.typ == BlobVarType || v.typ == ClobVarType ||
		v.typ == NClobVarType || v.typ == BFileVarType) {
		return 0, errgo.Newf("only LOBs an be written into, not %T", v.typ)
	}

	amount = len(data)
	// verify the data type
	if v.typ == BFileVarType {
		return 0, errgo.New("BFILEs are read only")
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
	if CTrace {
		ctrace("OCILobWrite(conn=%p, lob=%x, &oamount=%p, off=%d, cF=%d)",
			v.connection.handle, v.getHandleBytes(pos), &oamount, off,
			v.typ.charsetForm)
	}
	if err := v.environment.CheckStatus(
		C.OCILobWrite(v.connection.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(v.getHandle(pos)),
			&oamount, C.ub4(off),
			unsafe.Pointer(&data[0]), C.ub4(len(data)),
			C.OCI_ONE_PIECE, nil, nil, 0,
			v.typ.charsetForm),
		"LobWrite"); err != nil {
		return 0, errgo.Mask(err)
	}
	amount = int(oamount)
	// Py_END_ALLOW_THREADS
	return int(oamount), nil
}

// Returns the value stored at the given array position.
func lobVarGetValue(v *Variable, pos uint) (interface{}, error) {
	if CTrace {
		ctrace("lobVarGetValue(%s, %d)", v, pos)
	}
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
		return errgo.Newf("requires []byte, got %T", value)
	}
	var (
		isTemporary C.boolean
		lobType     C.ub1
		err         error
	)

	// make sure have temporary LOBs set up
	if err = v.environment.CheckStatus(
		C.OCILobIsTemporary(v.environment.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(v.getHandle(pos)), &isTemporary),
		"LobIsTemporary"); err != nil {
		return errgo.Mask(err)
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
				(*C.OCILobLocator)(v.getHandle(pos)),
				C.OCI_DEFAULT, v.typ.charsetForm, lobType, C.FALSE,
				C.OCI_DURATION_SESSION),
			"LobCreateTemporary"); err != nil {
			// Py_END_ALLOW_THREADS
			return errgo.Mask(err)
		}
	}

	// trim the current value
	// Py_BEGIN_ALLOW_THREADS
	if err = v.environment.CheckStatus(
		C.OCILobTrim(v.connection.handle,
			v.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(v.getHandle(pos))), 0),
		"LobTrim"); err != nil {
		return errgo.Mask(

			// Py_END_ALLOW_THREADS
			err)
	}

	// set the current value
	// func (v *Variable) lobVarWrite(data []byte, pos uint, off int64) (amount int, err error) {
	_, err = v.lobVarWrite(x, pos, 0)
	return err
}

func init() {
	ClobVarType = &VariableType{
		Name:        "ClobVar",
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
		Name:        "NClobVar",
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
		Name:        "BlobVar",
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
		Name:        "BFileVar",
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
