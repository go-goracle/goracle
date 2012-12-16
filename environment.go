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
	"bytes"
	"fmt"
	"log"
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

// Create and initialize a new environment object
func NewEnvironment() (*Environment, error) {
	var err *Error

	// create a new object for the Oracle environment
	env := new(Environment)
	env.fixedWidth = 1
	env.maxBytesPerCharacter = 4
	env.maxStringBytes = MAX_STRING_CHARS

	// create the new environment handle
	if err = checkStatus(C.OCIEnvNlsCreate(&env.handle,
		C.OCI_OBJECT|C.OCI_THREADED, nil, nil, nil, nil, 0, nil, 0, 0),
		false); err != nil { //, C.ub2(873), 0),
		err.At = "Unable to acquire Oracle environment handle"
		return nil, err
	}
	/*
		buffer := []byte("AL32UTF8\000")
		csid := C.OCINlsCharSetNameToId(unsafe.Pointer(env.handle),
			(*C.oratext)(&buffer[0]))
		log.Printf("csid=%d", csid)
		if err = CheckStatus(C.OCIEnvNlsCreate(
			&env.handle, C.OCI_OBJECT|C.OCI_THREADED, nil, nil, nil, nil, 0, nil,
			csid, 0),
			"Unable to acquire Oracle environment handle with AL32UTF8 charset"); err != nil {
			return nil, err
		}
	*/
	// log.Printf("env=%+v err=%+v", env.handle, err)

	// create the error handle
	if err = ociHandleAlloc(unsafe.Pointer(env.handle),
		C.OCI_HTYPE_ERROR, (*unsafe.Pointer)(unsafe.Pointer(&env.errorHandle)),
		"env.errorHandle"); err != nil || env.handle == nil {
		return nil, err
	}

	var sb4 C.sb4
	// acquire max bytes per character
	if err = env.CheckStatus(C.OCINlsNumericInfoGet(unsafe.Pointer(env.handle),
		env.errorHandle, &sb4, C.OCI_NLS_CHARSET_MAXBYTESZ),
		"Environment_New(): get max bytes per character"); err != nil {
		return nil, err
	}
	env.maxBytesPerCharacter = int(sb4)
	env.maxStringBytes = MAX_STRING_CHARS * env.maxBytesPerCharacter
	log.Printf("maxBytesPerCharacter=%d", env.maxBytesPerCharacter)

	// acquire whether character set is fixed width
	if err = env.CheckStatus(C.OCINlsNumericInfoGet(unsafe.Pointer(env.handle),
		env.errorHandle, &sb4, C.OCI_NLS_CHARSET_FIXEDWIDTH),
		"Environment_New(): determine if charset fixed width"); err != nil {
		return nil, err
	}
	env.fixedWidth = int(sb4)

	var e error
	// determine encodings to use for Unicode values
	if env.encoding, e = env.GetCharacterSetName(C.OCI_ATTR_ENV_CHARSET_ID); e != nil {
		return nil, e
	}
	if env.nencoding, e = env.GetCharacterSetName(C.OCI_ATTR_ENV_NCHARSET_ID); e != nil {
		return nil, e
	}

	return env, nil
}

func ociHandleAlloc(parent unsafe.Pointer, typ C.ub4, dst *unsafe.Pointer, at string) *Error {
	// var vsize C.ub4
	return checkStatus(C.OCIHandleAlloc(parent, dst, typ, C.size_t(0), nil), false)
}

func (env *Environment) AttrSet(parent unsafe.Pointer, parentTyp C.ub4,
	key C.ub4, value unsafe.Pointer, valueLength int) *Error {
	return env.CheckStatus(C.OCIAttrSet(parent, parentTyp,
		value, C.ub4(valueLength),
		key, env.errorHandle),
		"AttrSet")
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
	if err = env.CheckStatus(status, "GetCharacterSetName[get charset id]"); err != nil {
		return "", err
	}

	charsetName := make([]byte, C.OCI_NLS_MAXBUFSZ)
	ianaCharsetName := make([]byte, C.OCI_NLS_MAXBUFSZ)

	// get character set name
	if err = env.CheckStatus(C.OCINlsCharSetIdToName(unsafe.Pointer(env.handle),
		(*C.oratext)(&charsetName[0]),
		C.OCI_NLS_MAXBUFSZ, C.ub2(charsetId)),
		"GetCharacterSetName[get Oracle charset name]"); err != nil {
		return "", err
	}

	// get IANA character set name
	status = C.OCINlsNameMap(unsafe.Pointer(env.handle),
		(*C.oratext)(&ianaCharsetName[0]),
		C.OCI_NLS_MAXBUFSZ, (*C.oratext)(&charsetName[0]),
		C.OCI_NLS_CS_ORA_TO_IANA)
	if err = env.CheckStatus(status, "GetCharacterSetName[translate NLS charset]"); err != nil {
		return "", err
	}

	// store results
	// oratext = unsigned char
	return string(ianaCharsetName), nil
}

// Error handling
var (
	Warning        = &Error{Code: -(1<<30 - 1), Message: "WARNING"}
	NeedData       = &Error{Code: -(1<<30 - 2), Message: "NEED_DATA"}
	NoDataFound    = &Error{Code: -(1<<30 - 3), Message: "NO_DATA_FOUND"}
	StillExecuting = &Error{Code: -(1<<30 - 4), Message: "STILL_EXECUTING"}
	Continue       = &Error{Code: -(1<<30 - 5), Message: "CONTINUE"}
	Other          = &Error{Code: -(1<<30 - 6), Message: "other"}
	InvalidHandle  = &Error{Code: -(1<<30 - 7), Message: "invalid handle"}
)

// Check for an error in the last call and if an error has occurred.
func checkStatus(status C.sword, justSpecific bool) *Error {
	switch status {
	case C.OCI_SUCCESS, C.OCI_SUCCESS_WITH_INFO:
		return nil
	case C.OCI_NEED_DATA:
		return NeedData
	case C.OCI_NO_DATA:
		return NoDataFound
	case C.OCI_STILL_EXECUTING:
		return StillExecuting
	case C.OCI_CONTINUE:
		return Continue
	case C.OCI_INVALID_HANDLE:
		return InvalidHandle
	}
	if !justSpecific {
		return Other
	}
	return nil
}

// Check for an error in the last call and if an error has occurred,
// retrieve the error message from the database environment
func (env *Environment) CheckStatus(status C.sword, at string) (err *Error) {
	if status == C.OCI_SUCCESS || status == C.OCI_SUCCESS_WITH_INFO {
		return nil
	}
	if err = checkStatus(status, true); err != nil {
		return err
	}
	var (
		errcode C.sb4
		errbuf  = make([]byte, 2001)
		i       = C.ub4(0)
		errstat C.sword
		message = make([]byte, 0, 2000)
	)
	for {
		i++
		errstat = C.OCIErrorGet(unsafe.Pointer(env.errorHandle), i, nil,
			&errcode, (*C.OraText)(&errbuf[0]), C.ub4(len(errbuf)-1),
			C.OCI_HTYPE_ERROR)
		message = append(message, errbuf[:bytes.IndexByte(errbuf, 0)]...)
		if errstat == C.OCI_NO_DATA {
			break
		}
	}
	return &Error{Code: int(errcode),
		Message: fmt.Sprintf("[%d] %s", status, message),
		At:      at}
}

func (env *Environment) AttrGet(parent unsafe.Pointer, parentType, key int,
	dst unsafe.Pointer, errText string) (size int, err error) {
	var osize C.ub4
	if err = env.CheckStatus(
		C.OCIAttrGet(parent, C.ub4(parentType), dst, &osize, C.ub4(key),
			env.errorHandle), errText); err != nil {
		return
	}
	size = int(osize)
	return
}

func (env *Environment) FromEncodedString(text []byte, length int) string {
	return string(text[:length])
}
