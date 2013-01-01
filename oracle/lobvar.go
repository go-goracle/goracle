package oracle

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
	// "log"
	// "time"
	"unsafe"
)

var (
	ClobVarType, NClobVarType, BlobVarType, BFileVarType *VariableType
)

// Initialize the variable.
func lobVar_Initialize(v *Variable, cur *Cursor) error {
	// initialize members
	v.connection = cur.connection
	// v.isFile = v.typ == BFileVarType

	// initialize the LOB locators
	var err error
	for i := uint(0); i < v.allocatedElements; i++ {
		if err = v.environment.CheckStatus(
			C.OCIDescriptorAlloc(unsafe.Pointer(v.environment.handle),
				(*unsafe.Pointer)(unsafe.Pointer(&v.dataBytes[i])),
				C.OCI_DTYPE_LOB, 0, nil),
			"DescrAlloc"); err != nil {
			return err
		}
	}

	return nil
}

// Free temporary LOBs prior to fetch.
func lobVar_PreFetch(v *Variable) error {
	var isTemporary C.boolean

	var err error
	for i := uint(0); i < v.allocatedElements; i++ {
		if v.dataBytes != nil && uint(len(v.dataBytes)) > i && v.dataBytes[i] != 0 {
			if err = v.environment.CheckStatus(
				C.OCILobIsTemporary(v.environment.handle,
					v.environment.errorHandle,
					(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[i])),
					&isTemporary),
				"LobIsTemporary"); err != nil {
				return err
			}
			if isTemporary == C.TRUE {
				// Py_BEGIN_ALLOW_THREADS
				if err = v.environment.CheckStatus(
					C.OCILobFreeTemporary(v.connection.handle,
						v.environment.errorHandle,
						(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[i]))),
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
func lobVar_Finalize(v *Variable) error {
	for i := uint(0); i < v.allocatedElements; i++ {
		if v.dataBytes != nil && uint(len(v.dataBytes)) > i && v.dataBytes[i] != 0 {
			C.OCIDescriptorFree(unsafe.Pointer(&v.dataBytes[i]), C.OCI_DTYPE_LOB)
		}
	}
	return nil
}

// Write data to the LOB variable.
func (v *Variable) lobVar_Write(data []byte, pos uint, off int64) (amount int, err error) {
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
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[pos])),
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
func lobVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	return NewExternalLobVar(v, pos), nil
}

func lobVar_GetValueInto(v *Variable, pos uint, lv *ExternalLobVar) error {
	*lv = *NewExternalLobVar(v, pos)
	return nil
}

// Sets the value stored at the given array position.
func lobVar_SetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("requires []byte, got %T", value)
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
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[pos])), &isTemporary),
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
				(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[pos])),
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
			(*C.OCILobLocator)(unsafe.Pointer(&v.dataBytes[pos])), 0),
		"LobTrim"); err != nil {
		return err
	}
	// Py_END_ALLOW_THREADS

	// set the current value
	// func (v *Variable) lobVar_Write(data []byte, pos uint, off int64) (amount int, err error) {
	_, err = v.lobVar_Write(x, pos, 0)
	return err
}

func init() {
	ClobVarType = &VariableType{
		initialize:  lobVar_Initialize,
		finalize:    lobVar_Finalize,
		preFetch:    lobVar_PreFetch,
		setValue:    lobVar_SetValue,
		getValue:    lobVar_GetValue,
		oracleType:  C.SQLT_CLOB,                // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  true,                       // is character data
	}

	NClobVarType = &VariableType{
		initialize:  lobVar_Initialize,
		finalize:    lobVar_Finalize,
		preFetch:    lobVar_PreFetch,
		setValue:    lobVar_SetValue,
		getValue:    lobVar_GetValue,
		oracleType:  C.SQLT_CLOB,                // Oracle type
		charsetForm: C.SQLCS_NCHAR,              // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  true,                       // is character data
	}

	BlobVarType = &VariableType{
		initialize:  lobVar_Initialize,
		finalize:    lobVar_Finalize,
		preFetch:    lobVar_PreFetch,
		setValue:    lobVar_SetValue,
		getValue:    lobVar_GetValue,
		oracleType:  C.SQLT_BLOB,                // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  false,                      // is character data
	}

	BFileVarType = &VariableType{
		initialize:  lobVar_Initialize,
		finalize:    lobVar_Finalize,
		preFetch:    lobVar_PreFetch,
		setValue:    lobVar_SetValue,
		getValue:    lobVar_GetValue,
		oracleType:  C.SQLT_BFILE,               // Oracle type
		charsetForm: C.SQLCS_IMPLICIT,           // charset form
		size:        uint(C.sof_OCILobLocatorp), // element length
		isCharData:  false,                      // is character data
	}

}
