// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <oci.h>
*/
import "C"

import (
	"unsafe"
)

type Environment struct {
	handle                       *C.OCIEnv
	errorHandle                  *C.OCIError
	maxBytesPerCharacter         int
	fixedWidth                   int
	encoding                     string
	nencoding                    string
	maxStringBytes               int
	numberToStringFormatBuffer   []byte
	numberFromStringFormatBuffer []byte
	nlsNumericCharactersBuffer   []byte
}

// maximum number of characters/bytes applicable to strings/binaries
const (
	MAX_STRING_CHARS = 4000
	MAX_BINARY_BYTES = 4000
)

// Create a new environment object, using the environment handle
func NewEnvironment(handle *C.OCIEnv) (*Environment, error) {
	var err *Error

	// create a new object for the Oracle environment
	env := new(Environment)
	env.fixedWidth = 1
	env.maxBytesPerCharacter = 4
	env.maxStringBytes = MAX_STRING_CHARS

	// create the error handle
	if err = ociHandleAlloc(unsafe.Pointer(handle),
		C.OCI_HTYPE_ERROR, (*unsafe.Pointer)(unsafe.Pointer(&env.errorHandle))); err != nil {
		err.At = "NewEnvironment"
		return nil, err
	}
	return env, nil
}

func ociHandleAlloc(parent unsafe.Pointer, typ C.ub4, dst *unsafe.Pointer) *Error {
	var vsize C.ub4
	return CheckStatus(C.OCIHandleAlloc(parent, dst, typ,
		C.size_t(0), (*unsafe.Pointer)(unsafe.Pointer(&vsize))))
}

func (env *Environment) AttrSet(parent unsafe.Pointer, parentTyp C.ub4,
	key C.ub4, value unsafe.Pointer) *Error {
	return CheckStatus(C.OCIAttrSet(parent, parentTyp, value, 0, key, env.errorHandle))
}

// Retrieve and store the IANA character set name for the attribute.
func (env *Environment) GetCharacterSetName(attribute uint32) (string, error) {
	//, overrideValue string
	var (
		charsetId, vsize C.ub4
		status           C.sword
		err              *Error
	)

	/*
	   // if override value specified, use it
	   if (overrideValue) {
	       *result = PyMem_Malloc(strlen(overrideValue) + 1);
	       if (!*result)
	           return -1;
	       strcpy(*result, overrideValue);
	       return 0;
	   }
	*/

	// get character set id
	status = C.OCIAttrGet(unsafe.Pointer(env.handle), //void *trgthndlp
		C.OCI_HTYPE_ENV,            //ub4 trghndltyp
		unsafe.Pointer(&charsetId), //void *attributep
		&vsize,           //ub4 *sizep
		C.ub4(attribute), //ub4 attrtype
		env.errorHandle)  //OCIError *errhp
	if err = CheckStatus(status); err != nil {
		err.At = "GetCharacterSetName[get charset id]"
		return "", err
	}

	charsetName := make([]byte, C.OCI_NLS_MAXBUFSZ)
	ianaCharsetName := make([]byte, C.OCI_NLS_MAXBUFSZ)

	// get character set name
	status = C.OCINlsCharSetIdToName(unsafe.Pointer(env.handle),
		(*C.oratext)(&charsetName[0]),
		C.OCI_NLS_MAXBUFSZ, C.ub2(charsetId))
	if err = CheckStatus(status); err != nil {
		err.At = "GetCharacterSetName[get Oracle charset name]"
		return "", err
	}

	// get IANA character set name
	status = C.OCINlsNameMap(unsafe.Pointer(env.handle),
		(*C.oratext)(&ianaCharsetName[0]),
		C.OCI_NLS_MAXBUFSZ, (*C.oratext)(&charsetName[0]),
		C.OCI_NLS_CS_ORA_TO_IANA)
	if err = CheckStatus(status); err != nil {
		err.At = "GetCharacterSetName[translate NLS charset]"
		return "", err
	}

	// store results
	// oratext = unsigned char
	return string(ianaCharsetName), nil
}

// Check for an error in the last call and if an error has occurred.
func CheckStatus(status C.sword) *Error {
	if status != C.OCI_SUCCESS && status != C.OCI_SUCCESS_WITH_INFO {
		if status != C.OCI_INVALID_HANDLE {
			return NewError(-1, "other error")
		}
		return NewError(0, "Invalid handle!")
	}
	return nil
}
