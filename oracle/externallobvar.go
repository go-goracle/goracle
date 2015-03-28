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

#include <stdlib.h>
#include <oci.h>
#include "version.h"
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"

	"gopkg.in/errgo.v1"
)

const useLobRead2 = OracleVersion >= OracleV10_1

// Compile-time guarantees for interface implementations.
var _ = io.Writer(&ExternalLobVar{})
var _ = io.Reader(&ExternalLobVar{})
var _ = io.ReaderAt(&ExternalLobVar{})
var _ = io.WriterAt(&ExternalLobVar{})

// Defines the routines for handling LOB variables external to this module.

// ExternalLobVar is an external LOB var type.
// It is NOT concurrent-safe ATM.
type ExternalLobVar struct {
	lobVar           *Variable
	idx              uint
	internalFetchNum uint
	isFile           bool
	rwPos            int64
}

func (lv ExternalLobVar) getHandle() *C.OCILobLocator {
	lob, _ := lv.lobVar.getLobLoc(lv.idx)
	return lob
}
func (lv ExternalLobVar) getHandleBytes() []byte {
	return lv.lobVar.getHandleBytes(lv.idx)
}

// NewExternalLobVar creates a new external LOB variable.
func NewExternalLobVar(v *Variable, // variable to encapsulate
	idx uint, // position in array to encapsulate
) *ExternalLobVar {
	if CTrace {
		ctrace("NewExternalLobVar(%s, %d)", v, idx)
	}
	ret := &ExternalLobVar{
		lobVar:           v,
		idx:              idx,
		internalFetchNum: v.internalFetchNum,
		isFile:           v.typ == BFileVarType}
	if CTrace {
		if n, err := ret.internalSize(); err != nil {
			ctrace("error getting internal size in NewExternalLobVar(%v, %d): %s",
				v, idx, err)
		} else {
			ctrace("internal size: %d", n)
		}
	}
	return ret
}

// Verify that the external LOB var is still valid.
func (lv *ExternalLobVar) Verify() error {
	if CTrace {
		ctrace("externalLobVar.Verify(%x) %d =?= %d", lv.getHandleBytes(),
			lv.internalFetchNum, lv.lobVar.internalFetchNum)
	}
	if lv.internalFetchNum != lv.lobVar.internalFetchNum {
		return errgo.New("LOB variable no longer valid after subsequent fetch")
	}
	return nil
}

// internalRead returns the size of the LOB variable for internal comsumption.
func (lv *ExternalLobVar) internalRead(p []byte, off int64) (length int, err error) {
	var charsetID C.ub2

	if lv.isFile {
		// Py_BEGIN_ALLOW_THREADS
		if CTrace {
			ctrace("OCILobFileOpen(conn=%p, lob=%x, OCI_FILE_READONLY)",
				lv.lobVar.connection.handle, lv.getHandleBytes())
		}
		if err = lv.lobVar.environment.CheckStatus(
			C.OCILobFileOpen(lv.lobVar.connection.handle,
				lv.lobVar.environment.errorHandle,
				lv.getHandle(), C.OCI_FILE_READONLY),
			"LobFileOpen"); err != nil {
			return
		}
		defer func() {
			// Py_BEGIN_ALLOW_THREADS
			if CTrace {
				ctrace("OCILobFileClose(conn=%p, lob=%x)",
					lv.lobVar.connection.handle, lv.getHandleBytes())
			}
			if closeErr := lv.lobVar.environment.CheckStatus(
				C.OCILobFileClose(lv.lobVar.connection.handle,
					lv.lobVar.environment.errorHandle,
					lv.getHandle()),
				"LobFileClose"); closeErr != nil && err == nil {
				err = closeErr
			}
		}()
	}
	// Py_END_ALLOW_THREADS

	// Py_BEGIN_ALLOW_THREADS
	if lv.lobVar.typ == NClobVarType {
		// charsetID = C.OCI_UTF16ID
		charsetID = CsIDAl32UTF8
	} else {
		charsetID = 0
	}
	var (
		status   C.sword
		pos      = int(0)
		byteLen2 = C.LOB_LENGTH_TYPE(len(p))
		charLen2 = C.LOB_LENGTH_TYPE(0)
		byteLen  = C.ub4(byteLen2)
	)
	if CTrace {
		ctrace("internalRead is called with byteLen2=%d byteLen=%d off=%d bufl=%d",
			byteLen2, byteLen, off, len(p)-pos)
	}
	for {
		if useLobRead2 {
			status = C.OCILobRead2(lv.lobVar.connection.handle,
				lv.lobVar.environment.errorHandle,
				lv.getHandle(), &byteLen2, &charLen2, C.LOB_LENGTH_TYPE(off+1),
				unsafe.Pointer(&p[pos]), C.LOB_LENGTH_TYPE(len(p)-pos),
				C.OCI_ONE_PIECE,
				nil, nil, charsetID, lv.lobVar.typ.charsetForm)
		} else {
			status = C.OCILobRead(lv.lobVar.connection.handle,
				lv.lobVar.environment.errorHandle,
				lv.getHandle(), &byteLen, C.ub4(off+1),
				unsafe.Pointer(&p[pos]), C.ub4(len(p)-pos),
				nil, nil,
				charsetID, lv.lobVar.typ.charsetForm)
		}
		if CTrace {
			ctrace("OCILobRead(conn=%p, lob=%x, byteLen=%d, off=%d, &p=%p "+
				"len(p)=%d, csID=%d, csF=%d): "+
				"(byteLen2=%d charLen2=%d byteLen=%d status=%d)",
				lv.lobVar.connection.handle,
				lv.getHandleBytes(), byteLen, off,
				&p[pos], len(p)-pos,
				charsetID, lv.lobVar.typ.charsetForm,
				byteLen2, charLen2, byteLen, status,
			)
		}
		if !(status == C.OCI_SUCCESS || status == C.OCI_NEED_DATA) {
			err = lv.lobVar.environment.CheckStatus(status, "LobRead")
			return
		}

		if useLobRead2 {
			byteLen = C.ub4(byteLen2)
		}
		off += int64(byteLen)
		length += int(byteLen)
		if status == C.OCI_SUCCESS {
			break
		}
		pos += int(byteLen)
		if useLobRead2 {
			byteLen2 = C.oraub8(len(p) - pos)
		} else {
			byteLen = C.ub4(len(p) - pos)
		}
	}

	if 0 == length && err == nil {
		err = io.EOF
	}
	if CTrace {
		ctrace("internalRead returns %d, %s", length, err)
	}
	return
}

// internalSize returns the size in bytes (!) of the LOB variable for internal comsumption.
func (lv *ExternalLobVar) internalSize() (length C.LOB_LENGTH_TYPE, err error) {
	// Py_BEGIN_ALLOW_THREADS
	err = lv.lobVar.environment.CheckStatus(
		C.OCILOBGETLENGTH(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), &length),
		"LobGetLength")
	if CTrace {
		ctrace("OCILobGetLength(conn=%p, pos=%d lob=%x): (%v, %v)",
			lv.lobVar.connection.handle, lv.idx*lv.lobVar.typ.size,
			lv.getHandleBytes(), length, err)
	}
	if err != nil {
		return
	}
	// Py_END_ALLOW_THREADS
	if lv.lobVar.typ == ClobVarType {
		length *= C.LOB_LENGTH_TYPE(lv.lobVar.environment.MaxBytesPerCharacter)
	} else if lv.lobVar.typ == NClobVarType {
		length *= 2
	}

	return
}

// Size returns the size of the data in the LOB variable.
func (lv *ExternalLobVar) Size(inChars bool) (int64, error) {
	if err := lv.Verify(); err != nil {
		return 0, errgo.Mask(err)
	}
	length, err := lv.internalSize()
	if inChars {
		if lv.lobVar.typ == ClobVarType {
			length /= C.LOB_LENGTH_TYPE(lv.lobVar.environment.MaxBytesPerCharacter)
		} else if lv.lobVar.typ == NClobVarType {
			length /= 2
		}
	}
	return int64(length), err
}

// String returns a short representation of the ExternalLobVar
func (lv *ExternalLobVar) String() string {
	return fmt.Sprintf("<ExternalLobVar of %s>", lv.lobVar)
}

// ReadAt returns a portion (or all) of the data in the external LOB variable.
func (lv *ExternalLobVar) ReadAt(p []byte, off int64) (int, error) {
	length, err := lv.internalSize()
	if err != nil {
		return 0, errgo.Mask(err)
	} else if int64(length) < off {
		return 0, io.EOF
	}

	// modify the arguments
	if off < 0 {
		off = 0
	}

	n, err := lv.internalRead(p, off)
	return int(n), err
}

// Open the LOB to speed further accesses.
func (lv *ExternalLobVar) Open() error {
	if err := lv.Verify(); err != nil {
		return errgo.Mask(err)
	}

	if CTrace {
		ctrace("OCILobOpen(conn=%p, lob=%x, OCI_LOB_READWRITE)",
			lv.lobVar.connection.handle, lv.getHandleBytes())
	}
	return lv.lobVar.environment.CheckStatus(
		C.OCILobOpen(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), C.OCI_LOB_READWRITE),
		"LobOpen")
}

// Close the LOB.
func (lv *ExternalLobVar) Close() error {
	if err := lv.Verify(); err != nil {
		return errgo.Mask(

			// Py_BEGIN_ALLOW_THREADS
			err)
	}

	if CTrace {
		ctrace("OCILobFileClose(conn=%p, lob=%x)",
			lv.lobVar.connection.handle, lv.getHandleBytes())
	}
	return lv.lobVar.environment.CheckStatus(
		C.OCILobClose(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle()),
		"LobClose")
	// Py_END_ALLOW_THREADS
}

// Read returns a portion (or all) of the data in the external LOB variable.
//
// WARNING: if you use concurrency with database/sql, and sometimes gets
//          "cannot get internal size of <ExternalLobVar>: -1073741817: invalid handle"
//          errors, then the culprit may be database/sql closing the underlying driver.Conn.
// The usual reason for this is that you use QueryRow, which closes the connection
// right after the Scan. To fix this, don't be lazy and use Query, and the returned Rows.
func (lv *ExternalLobVar) Read(p []byte) (int, error) {
	if err := lv.Verify(); err != nil {
		return 0, errgo.Mask(err)
	}
	n, e := lv.ReadAt(p, lv.rwPos)
	if CTrace {
		ctrace("ReadAt %d => %d, %s => %d", lv.rwPos, n, e, lv.rwPos+int64(n))
	}
	lv.rwPos += int64(n)
	return n, e
}

// ReadAll returns all of the data in the external LOB variable.
func (lv *ExternalLobVar) ReadAll() ([]byte, error) {
	if err := lv.Verify(); err != nil {
		return nil, errgo.Mask(err)
	}
	amount, err := lv.internalSize()
	Log.Debug("internalSize", "lob", lv, "amount", amount, "err", err)
	if err != nil {
		return nil, errgo.Newf("cannot get internal size of %s: %s", lv, err)
	}
	p := make([]byte, int(amount))
	var n int
	n, err = lv.ReadAt(p, 0)
	Log.Debug("ReadAt", "lob", lv, "p", p, "n", n, "error", err)
	if n >= 0 && n < len(p) {
		p = p[:n]
	}
	return p, err
}

// WriteAt writes a value to the LOB variable at the specified offset; return the number of bytes written.
func (lv *ExternalLobVar) WriteAt(value []byte, off int64) (n int, err error) {
	// perform the write, if possible
	if err = lv.Verify(); err != nil {
		return 0, errgo.Mask(err)
	}
	return lv.lobVar.lobVarWrite(value, 0, off)
}

// Write writes the value to the LOB variable; return the number of bytes written.
func (lv *ExternalLobVar) Write(p []byte) (int, error) {
	if err := lv.Verify(); err != nil {
		return 0, errgo.Mask(err)
	}
	n, e := lv.WriteAt(p, lv.rwPos)
	if CTrace {
		ctrace("WriteAt %d => %d, %s => %d", lv.rwPos, n, e, lv.rwPos+int64(n))
	}
	lv.rwPos += int64(n)
	return n, e
}

// Trim the LOB variable to the specified length.
func (lv *ExternalLobVar) Trim(newSize int) error {
	var (
		err error
	)

	if err = lv.Verify(); err != nil {
		return errgo.Mask(

			// Py_BEGIN_ALLOW_THREADS
			err)
	}

	if CTrace {
		ctrace("OCILobTrim(conn=%p, lob=%x, newSize=%d)",
			lv.lobVar.connection.handle, lv.getHandleBytes(), newSize)
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILOBTRIM(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), C.LOB_LENGTH_TYPE(newSize)),
		"LobTrim",
	); err != nil {
		return errgo.Mask(

			// Py_END_ALLOW_THREADS
			err)
	}

	return nil
}

// Seek as os.File.Seek: sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the file
// 1 means relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (lv *ExternalLobVar) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case 0:
		lv.rwPos = offset
	case 1:
		lv.rwPos += offset
	case 2:
		var size C.LOB_LENGTH_TYPE
		if size, err = lv.internalSize(); err != nil {
			return lv.rwPos, err
		}
		lv.rwPos = int64(size) + offset
	default:
		return lv.rwPos, fmt.Errorf("bad whence %d", whence)
	}
	return lv.rwPos, nil
}

// GetChunkSize returns the chunk size that should be used when
// reading/writing the LOB in chunks.
func (lv *ExternalLobVar) GetChunkSize() (int, error) {
	var chunkSize C.ub4
	var err error

	if err = lv.Verify(); err != nil {
		return 0, errgo.Mask(err)
	}
	if CTrace {
		ctrace("OCILobGetChunk(conn=%p, lob=%x, &size=%p)",
			lv.lobVar.connection.handle, lv.getHandleBytes(), &chunkSize)
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobGetChunkSize(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), &chunkSize),
		"LobGetChunkSize"); err != nil {
		return 0, errgo.Mask(err)
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
		return false, errgo.Mask(

			// Py_BEGIN_ALLOW_THREADS
			err)
	}

	if CTrace {
		ctrace("OCILobIsOpen(conn=%p, lob=%x, &isOpen=%p)",
			lv.lobVar.connection.handle, lv.getHandleBytes(), &isOpen)
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobIsOpen(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), &isOpen),
		"LobIsOpen"); err != nil {
		return false, errgo.Mask(

			// Py_END_ALLOW_THREADS
			err)
	}

	return isOpen == C.TRUE, nil
}

// GetFileName returns the directory alias and file name for the BFILE lob.
func (lv *ExternalLobVar) GetFileName() (string, string, error) {
	var err error
	// determine the directory alias and name
	if err = lv.Verify(); err != nil {
		return "", "", errgo.Mask(err)
	}
	dirAliasB := make([]byte, 120)
	nameB := make([]byte, 1020)
	var dirAliasLength, nameLength C.ub2

	if CTrace {
		ctrace("OCILobGetFilename(conn=%p, lob=%x, &dirAlias=%p, &dirAliasLen=%p, &name=%p, &nameLen=%p)",
			lv.lobVar.connection.handle, lv.getHandleBytes(),
			&dirAliasB[0], &dirAliasLength,
			&nameB[0], &nameLength)
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileGetName(lv.lobVar.environment.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(),
			(*C.OraText)(&dirAliasB[0]), &dirAliasLength,
			(*C.OraText)(&nameB[0]), &nameLength),
		"LobFileGetName"); err != nil {
		return "", "", errgo.Mask(err)
	}

	return string(dirAliasB[:dirAliasLength]), string(nameB[:nameLength]), nil
}

// SetFileName sets the directory alias and file name for the BFILE lob.
func (lv *ExternalLobVar) SetFileName(dirAlias, name string) error {
	var err error
	// create a string for retrieving the value
	if err = lv.Verify(); err != nil {
		return errgo.Mask(err)
	}
	nameB := []byte(name)
	dirAliasB := []byte(dirAlias)

	if CTrace {
		ctrace("OCILobSetFilename(conn=%p, lob=%x, dirAlias=%s, dirAliasLen=%d, name=%s, nameLen=%d)",
			lv.lobVar.connection.handle, lv.getHandleBytes(),
			dirAliasB, len(dirAlias), nameB, len(nameB))
	}
	lob := lv.getHandle()
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileSetName(lv.lobVar.environment.handle,
			lv.lobVar.environment.errorHandle,
			&lob,
			(*C.OraText)(&dirAliasB[0]), C.ub2(len(dirAliasB)),
			(*C.OraText)(&nameB[0]), C.ub2(len(nameB))),
		"LobFileSetName"); err != nil {
		return errgo.Mask(err)
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
		return false, errgo.Mask(

			// Py_BEGIN_ALLOW_THREADS
			err)
	}

	if CTrace {
		ctrace("OCILobFileExists(conn=%p, lob=%x, &flag=%p",
			lv.lobVar.connection.handle, lv.getHandleBytes(), &flag)
	}
	if err = lv.lobVar.environment.CheckStatus(
		C.OCILobFileExists(lv.lobVar.connection.handle,
			lv.lobVar.environment.errorHandle,
			lv.getHandle(), &flag),
		"LobFileExists",
	); err != nil {
		return false, errgo.Mask(

			// Py_END_ALLOW_THREADS
			err)
	}

	return flag == C.TRUE, nil
}
