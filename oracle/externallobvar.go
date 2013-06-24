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
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Defines the routines for handling LOB variables external to this module.

// ExternalLobVar is an external LOB var type
type ExternalLobVar struct {
	lobVar           *Variable
	pos              uint
	internalFetchNum uint
	isFile           bool
}

// NewExternalLobVar creates a new external LOB variable.
func NewExternalLobVar(v *Variable, // variable to encapsulate
	pos uint, // position in array to encapsulate
) *ExternalLobVar {
	return &ExternalLobVar{lobVar: v, pos: pos,
		internalFetchNum: v.internalFetchNum,
		isFile:           v.typ == BFileVarType}
}

// Verify that the external LOB var is still valid.
func (lv *ExternalLobVar) Verify() error {
	if lv.internalFetchNum != lv.lobVar.internalFetchNum {
		return errors.New("LOB variable no longer valid after subsequent fetch")
	}
	return nil
}

// internalRead returns the size of the LOB variable for internal comsumption.
func (lv *ExternalLobVar) internalRead(p []byte, off int64) (length int64, err error) {
	var charsetID C.ub2
	j := lv.pos * lv.lobVar.typ.size

	if lv.isFile {
		// Py_BEGIN_ALLOW_THREADS
		if err = lv.lobVar.environment.CheckStatus(
			C.OCILobFileOpen(lv.lobVar.connection.handle,
				lv.lobVar.environment.errorHandle,
				(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[j])),
				C.OCI_FILE_READONLY),
			"LobFileOpen"); err != nil {
			return
		}
	}
	// Py_END_ALLOW_THREADS

	// Py_BEGIN_ALLOW_THREADS
	if lv.lobVar.typ == NClobVarType {
		// charsetID = C.OCI_UTF16ID
		charsetID = CsIDAl32UTF8
	} else {
		charsetID = 0
	}
	length = int64(len(p))
	olength := C.ub4(length + 1)
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobRead(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[j])),
			&olength, C.ub4(off+1), unsafe.Pointer(&p[0]),
			C.ub4(len(p)), nil, nil, charsetID, lv.lobVar.typ.charsetForm),
		"LobRead"); err != nil {
		// Py_END_ALLOW_THREADS
		C.OCILobFileClose(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[j])))
		return
	}

	if lv.isFile {
		// Py_BEGIN_ALLOW_THREADS
		if err = lv.lobVar.environment.CheckStatus(
			C.OCILobFileClose(lv.lobVar.connection.handle,
				lv.lobVar.environment.errorHandle,
				(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[j]))),
			"LobFileClose"); err != nil {
			return
		}
	}

	return
}

// internalSize returns the size of the LOB variable for internal comsumption.
func (lv *ExternalLobVar) internalSize() (length C.ub4, err error) {
	// Py_BEGIN_ALLOW_THREADS
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobGetLength(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			&length),
		"LobGetLength"); err != nil {
		return
	}
	// Py_END_ALLOW_THREADS

	return
}

// Size returns the size of the data in the LOB variable.
func (lv *ExternalLobVar) Size(inChars bool) (int64, error) {
	if err := lv.Verify(); err != nil {
		return 0, err
	}
	length, err := lv.internalSize()
	if inChars {
		if lv.lobVar.typ == ClobVarType {
			length /= C.ub4(lv.lobVar.environment.MaxBytesPerCharacter)
		} else if lv.lobVar.typ == NClobVarType {
			length /= 2
		}
	}
	return int64(length), err
}

// ReadAt returns a portion (or all) of the data in the external LOB variable.
func (lv *ExternalLobVar) ReadAt(p []byte, off int64) (int, error) {
	/*
		length, err := lv.Size(false)
		if err != nil {
			return 0, err
		}
		var bufferSize C.ub4
	*/

	// modify the arguments
	if off < 0 {
		off = 0
	}
	/*
		bufferSize = C.ub4(len(p))
		length = bufferSize
		if lv.lobVar.typ == ClobVarType {
			length = bufferSize / C.ub4(lv.lobVar.environment.maxBytesPerCharacter)
		} else if lv.lobVar.typ == NClobVarType {
			length = bufferSize / 2
		}
		if C.ub4(len(p)) > length-C.ub4(off) {
			p = p[:size-off]
		}
		if C.ub4(len(p)) < length-C.ub4(off) {
			length = C.ub4(len(p)) - C.ub4(off)
		}
	*/

	n, err := lv.internalRead(p, off)
	return int(n), err

	/*
	   // return the result
	   if (lv.lobVar.type == &vt_CLOB) {
	       if (lv.lobVar.environment->fixedWidth)
	           length = length * lv.lobVar.environment->maxBytesPerCharacter;
	       result = cxString_FromEncodedString(buffer, length,
	               lv.lobVar.environment->encoding);
	   } else if (lv.lobVar.type == &vt_NCLOB) {
	       result = PyUnicode_DecodeUTF16(buffer, length * 2, NULL, NULL);
	   } else {
	       result = PyBytes_FromStringAndSize(buffer, length);
	   }
	   PyMem_Free(buffer);
	*/
}

// Open the LOB to speed further accesses.
func (lv *ExternalLobVar) Open() error {
	if err := lv.Verify(); err != nil {
		return err
	}
	// Py_BEGIN_ALLOW_THREADS
	return lv.lobVar.environment.CheckStatus(
		C.OCILobOpen(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			C.OCI_LOB_READWRITE),
		"LobOpen")
	// Py_END_ALLOW_THREADS
}

// Close the LOB.
func (lv *ExternalLobVar) Close() error {
	if err := lv.Verify(); err != nil {
		return err
	}
	// Py_BEGIN_ALLOW_THREADS
	return lv.lobVar.environment.CheckStatus(
		C.OCILobClose(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size]))),
		"LobClose")
	// Py_END_ALLOW_THREADS
}

// Read returns a portion (or all) of the data in the external LOB variable.
func (lv *ExternalLobVar) Read(p []byte) (int, error) {
	if err := lv.Verify(); err != nil {
		return 0, err
	}
	return lv.ReadAt(p, 0)
}

// ReadAll returns all of the data in the external LOB variable.
func (lv *ExternalLobVar) ReadAll() ([]byte, error) {
	amount, err := lv.internalSize()
	if err != nil {
		return nil, err
	}
	p := make([]byte, uint(amount))
	_, err = lv.ReadAt(p, 0)
	return p, err
}

// WriteAt writes a value to the LOB variable; return the number of bytes written.
func (lv *ExternalLobVar) WriteAt(value []byte, off int64) (n int, err error) {
	// perform the write, if possible
	if err = lv.Verify(); err != nil {
		return 0, err
	}
	return lv.lobVar.lobVarWrite(value, 0, off)
}

// Trim the LOB variable to the specified length.
func (lv *ExternalLobVar) Trim(newSize int) error {
	var (
		err error
	)

	if err = lv.Verify(); err != nil {
		return err
	}
	// Py_BEGIN_ALLOW_THREADS
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobTrim(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			C.ub4(newSize)),
		"LobTrim"); err != nil {
		return err
	}
	// Py_END_ALLOW_THREADS
	return nil
}

// GetChunkSize returns the chunk size that should be used when
// reading/writing the LOB in chunks.
func (lv *ExternalLobVar) GetChunkSize() (int, error) {
	var chunkSize C.ub4
	var err error

	if err = lv.Verify(); err != nil {
		return 0, err
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobGetChunkSize(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			&chunkSize),
		"LobGetChunkSize"); err != nil {
		return 0, err
	}
	return int(chunkSize), nil
}

// IsOpen returns a boolean indicating if the lob is open or not.
func (lv *ExternalLobVar) IsOpen() (bool, error) {
	var (
		err    error
		isOpen C.boolean
	)
	if err = lv.Verify(); err != nil {
		return false, err
	}
	// Py_BEGIN_ALLOW_THREADS
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobIsOpen(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			&isOpen),
		"LobIsOpen"); err != nil {
		return false, err
	}
	// Py_END_ALLOW_THREADS
	return isOpen == C.TRUE, nil
}

// GetFileName returns the directory alias and file name for the BFILE lob.
func (lv *ExternalLobVar) GetFileName() (string, string, error) {
	var err error
	// determine the directory alias and name
	if err = lv.Verify(); err != nil {
		return "", "", err
	}
	dirAliasB := make([]byte, 120)
	nameB := make([]byte, 1020)
	var dirAliasLength, nameLength C.ub2

	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileGetName(lv.lobVar.environment.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			(*C.OraText)(&dirAliasB[0]), &dirAliasLength,
			(*C.OraText)(&nameB[0]), &nameLength),
		"LobFileGetName"); err != nil {
		return "", "", err
	}

	return string(dirAliasB[:dirAliasLength]), string(nameB[:nameLength]), nil
}

// SetFileName sets the directory alias and file name for the BFILE lob.
func (lv *ExternalLobVar) SetFileName(dirAlias, name string) error {
	var err error
	// create a string for retrieving the value
	if err = lv.Verify(); err != nil {
		return err
	}
	nameB := []byte(name)
	dirAliasB := []byte(dirAlias)

	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileSetName(lv.lobVar.environment.handle,
			lv.lobVar.environment.errorHandle,
			(**C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			(*C.OraText)(&dirAliasB[0]), C.ub2(len(dirAliasB)),
			(*C.OraText)(&nameB[0]), C.ub2(len(nameB))),
		"LobFileSetName"); err != nil {
		return err
	}

	return nil
}

// FileExists returns a boolean indicating if the BFIILE lob exists.
func (lv *ExternalLobVar) FileExists() (bool, error) {
	var (
		err  error
		flag C.boolean
	)
	if err = lv.Verify(); err != nil {
		return false, err
	}
	// Py_BEGIN_ALLOW_THREADS
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileExists(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			(*C.OCILobLocator)(unsafe.Pointer(&lv.lobVar.dataBytes[lv.pos*lv.lobVar.size])),
			&flag), "LobFileExists"); err != nil {
		return false, err
	}
	// Py_END_ALLOW_THREADS

	return flag == C.TRUE, nil
}
