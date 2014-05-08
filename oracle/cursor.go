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
#cgo LDFLAGS: -lclntsh

#include <stdlib.h>
#include <oci.h>

const int sizeof_OraText = sizeof(OraText);
*/
import "C"

import (
	//"bytes"
	"fmt"
	"hash/fnv"
	"log"

	// "reflect"
	"io"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/juju/errgo"
)

// BypassMultipleArgs induces a bypass agains ORA-1008 when a keyword
// is used more than once in the statement
// It is false by default, as it makes Execute change the statement!
var BypassMultipleArgs = false

const usePrepare2 = true

// func init() {
// debug("bindInfo_elementSize=%d", C.bindInfo_elementSize)
// }

// Cursor holds the handles for a cursor
type Cursor struct {
	// private or unexported fields
	handle                                      *C.OCIStmt
	connection                                  *Connection
	environment                                 *Environment
	bindVarsArr                                 []*Variable
	bindVarsMap                                 map[string]*Variable
	fetchVariables                              []*Variable
	arraySize, bindArraySize, fetchArraySize    uint
	setInputSizes, outputSize, outputSizeColumn int
	rowCount, actualRows, rowNum                int
	statement                                   []byte
	statementTag                                []byte
	statementType                               int
	numbersAsStrings, isDML, isOpen, isOwned    bool
}

//DefaultArraySize is the default array (PL/SQL) size
var DefaultArraySize uint = 50
var (
	//CursorIsClosed prints cursor is closed
	CursorIsClosed = errgo.New("cursor is closed")
	//QueriesNotSupported prints queries not supported
	QueriesNotSupported = errgo.New("queries not supported: results undefined")
	//ListIsEmpty prints list is empty
	ListIsEmpty = errgo.New("list is empty")
)

//statement // statementTag // rowFactory // inputTypeHandler // outputTypeHandler

// allocateHandle allocates a new handle.
func (cur *Cursor) allocateHandle() error {
	cur.isOwned = true
	return ociHandleAlloc(unsafe.Pointer(cur.environment.handle),
		C.OCI_HTYPE_STMT,
		(*unsafe.Pointer)(unsafe.Pointer(&cur.handle)),
		"allocate statement handle")
}

//freeHandle frees the handle which may be reallocated if necessary.
func (cur *Cursor) freeHandle() error {
	if CTrace {
		ctrace("%s.freeHandle(%p)", cur, cur.handle)
	}
	if cur.handle == nil {
		return nil
	}
	//debug("freeing cursor handle %v", cur.handle)
	if !cur.isOwned && cur.connection.handle != nil &&
		cur.statementTag != nil && len(cur.statementTag) > 0 {
		if CTrace {
			ctrace("OCIStmtRelease(cur=%p, env=%p, stmtT=%q, len(stmtT)=%d)",
				cur.handle, cur.environment.errorHandle,
				cur.statementTag, len(cur.statementTag))
		}
		if err := cur.environment.CheckStatus(C.OCIStmtRelease(cur.handle,
			cur.environment.errorHandle, (*C.OraText)(&cur.statementTag[0]),
			C.ub4(len(cur.statementTag)), C.OCI_DEFAULT),
			"statement release"); err != nil {
			return errgo.Mask(err)
		}
	} else {
		if CTrace {
			ctrace("OCIHandleFree(cur=%p, htype_stmt)", cur.handle)
		}
		if err := cur.environment.CheckStatus(
			C.OCIHandleFree(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT),
			"freeCursor"); err != nil {
			return errgo.Mask(err)
		}
	}
	cur.handle = nil
	return nil
}

//FromOraText converts from C.OraText to a string
func FromOraText(textp *C.OraText, length int) string {
	/*
	   var theGoSlice []TheCType
	   sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&theGoSlice)))
	   sliceHeader.Cap = length
	   sliceHeader.Len = length
	   sliceHeader.Data = uintptr(unsafe.Pointer(&theCArray[0]))
	   // now theGoSlice is a normal Go slice backed by the C array
	*/

	return string(C.GoBytes(unsafe.Pointer(textp), C.int(C.sizeof_OraText*length)))
}

//IsOpen checks whether the cursor is open
func (cur *Cursor) IsOpen() bool {
	if CTrace {
		ctrace("%s.IsOpen")
	}
	if !cur.isOpen {
		return false
	}
	return cur.connection.IsConnected()
}

//NewCursor creates a new cursor
func NewCursor(conn *Connection) *Cursor {
	return &Cursor{connection: conn, environment: conn.environment,
		arraySize: DefaultArraySize, fetchArraySize: DefaultArraySize,
		bindArraySize: 1, statementType: -1, outputSize: -1, outputSizeColumn: -1,
		isOpen: true}
}

//String implements Stringer on Cursor
func (cur *Cursor) String() string {
	return fmt.Sprintf("<goracle.Cursor %x on %p>", cur.handle, cur.connection.handle)
}

//getBindNames returns a list of bind variable names. At this point the cursor must have
// already been prepared.
func (cur *Cursor) getBindNames() (names []string, err error) {
	// ensure that a statement has already been prepared
	if cur.statement == nil {
		err = errgo.New("statement must be prepared first!")
		return
	}

	if names, err = cur.getBindInfo(8); err != nil {
		if men, ok := errgo.Cause(err).(mismatchElementNum); ok {
			names, err = cur.getBindInfo(int(men))
		}
	}

	return names, err
}

// getBindInfo returns the bind information on the cursor
func (cur *Cursor) getBindInfo(numElements int) ([]string, error) {
	var foundElements C.sb4

	// avoid bus errors on 64-bit platforms
	// numElements = numElements + (sizeof(void*) - numElements % sizeof(void*));

	// initialize the buffers
	// buffer := make([]byte, numElements*int(C.bindInfo_elementSize))
	// bindNames := (**C.OraText)(unsafe.Pointer(&buffer[0]))
	bindNames := make([](*C.OraText), numElements)
	// bindNameLengths := (*C.ub1)(&buffer[0+numElements])
	bindNameLengths := make([]byte, numElements)
	// indicatorNames := (**C.OraText)(unsafe.Pointer(&buffer[1*numElements+numElements]))
	indicatorNames := make([](*C.OraText), numElements)
	// indicatorNameLengths := (*C.ub1)(&buffer[2*numElements+numElements])
	indicatorNameLengths := make([]byte, numElements)
	// duplicate := (*C.ub1)(unsafe.Pointer(&buffer[3*numElements+numElements]))
	duplicate := make([]byte, numElements)
	// bindHandles := (**C.OCIBind)(unsafe.Pointer(&buffer[4*numElements+numElements]))
	bindHandles := make([](*C.OCIBind), numElements)

	// get the bind information
	if CTrace {
		ctrace("OCIStmtGetBindInfo", cur.handle, cur.environment.errorHandle,
			numElements, 1, &foundElements, bindNames, bindNameLengths,
			indicatorNames, indicatorNameLengths,
			duplicate, bindHandles)
	}
	status := C.OCIStmtGetBindInfo(cur.handle,
		cur.environment.errorHandle, C.ub4(numElements), 1, &foundElements,
		(**C.OraText)(unsafe.Pointer(&bindNames[0])),
		(*C.ub1)(&bindNameLengths[0]),
		(**C.OraText)(&indicatorNames[0]), (*C.ub1)(&indicatorNameLengths[0]),
		(*C.ub1)(&duplicate[0]), (**C.OCIBind)(&bindHandles[0]))
	if status != C.OCI_NO_DATA {
		if e := cur.environment.CheckStatus(status, "GetBindInfo"); e != nil {
			return nil, e
		}
	}
	if foundElements < 0 {
		return nil, mismatchElementNum(-foundElements)
	}

	// create the list which is to be returned
	names := make([]string, 0, foundElements)
	// process the bind information returned
	for i := 0; i < int(foundElements); i++ {
		if duplicate[i] > 0 {
			continue
		}
		names = append(names, FromOraText(bindNames[i], int(bindNameLengths[i])))
	}
	return names, nil
}

// performDefine performs the defines for the cursor.
// At this point it is assumed that the statement being executed is in fact a query.
func (cur *Cursor) performDefine() error {
	var numParams uint
	var x = C.ub4(0)

	// determine number of items in select-list
	if CTrace {
		ctrace("performDefine.OCIAttrGet(cur=%p, HTYPE_STMT, numParams=%p, &x=%p, PARAM_COUNT, env=%p)",
			cur.handle, &numParams, &x, cur.environment.errorHandle)
	}
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle),
			C.OCI_HTYPE_STMT,
			unsafe.Pointer(&numParams), &x,
			C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
		"PerformDefine"); err != nil {
		return errgo.Mask(

			// debug("performDefine param count = %d", numParams)
			err)
	}

	// create a list corresponding to the number of items
	cur.fetchVariables = make([]*Variable, numParams)

	// define a variable for each select-item
	var v *Variable
	var e error
	cur.fetchArraySize = cur.arraySize
	for pos := uint(1); pos <= numParams; pos++ {
		v, e = cur.varDefine(cur.fetchArraySize, pos)
		// debug("varDefine[%d]: %s nil?%s", pos, e, e == nil)
		if e != nil {
			return errgo.Newf("error defining variable %d: %s", pos, e)
		}
		if v == nil {
			return errgo.Newf("empty variable on pos %d!", pos)
		}
		// debug("var %d=%v", pos, v)
		cur.fetchVariables[pos-1] = v
	}
	// debug("len(cur.fetchVariables)=%d", len(cur.fetchVariables))
	return nil
}

// setRowCount sets the rowcount variable.
func (cur *Cursor) setRowCount() error {
	if CTrace {
		ctrace("%s.setRowCount statementType=%d", cur, cur.statementType)
	}

	var rowCount, x C.ub4

	if cur.statementType == C.OCI_STMT_SELECT {
		cur.rowCount = 0
		cur.actualRows = -1
		cur.rowNum = 0
	} else if cur.statementType == C.OCI_STMT_INSERT ||
		cur.statementType == C.OCI_STMT_UPDATE ||
		cur.statementType == C.OCI_STMT_DELETE {
		if CTrace {
			ctrace("OCIAttrGet", cur.handle, "HTYPE_STMT", &rowCount, &x,
				"ATTR_ROW_COUNT", cur.environment.errorHandle)
		}
		if err := cur.environment.CheckStatus(
			C.OCIAttrGet(unsafe.Pointer(cur.handle),
				C.OCI_HTYPE_STMT, unsafe.Pointer(&rowCount), &x,
				C.OCI_ATTR_ROW_COUNT, cur.environment.errorHandle),
			"SetRowCount"); err != nil {
			return errgo.Mask(err)
		}
		cur.rowCount = int(rowCount)
	} else {
		cur.rowCount = -1
	}

	return nil
}

// GetRowCount returns the rowcount of the statement (0 for select, rows affected for DML)
func (cur Cursor) GetRowCount() int {
	return cur.rowCount
}

// // returns the bind variables array and map
// func (cur Cursor) GetBindVars() ([]*Variable, map[string]*Variable) {
// 	return cur.bindVarsArr, cur.bindVarsMap
// }

// getErrorOffset gets the error offset on the error object, if applicable.
func (cur *Cursor) getErrorOffset() int {
	var offset, x C.ub4
	if CTrace {
		ctrace("getErrorOffset.OCIAttrGet", cur.handle, "HTYPE_STMT", &offset, &x,
			"ATTR_PARSE_ERROR_OFFSET", cur.environment.errorHandle)
	}
	C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		unsafe.Pointer(&offset), &x,
		C.OCI_ATTR_PARSE_ERROR_OFFSET, cur.environment.errorHandle)
	return int(offset)
}

// setErrorOffset sets the error offset (if applicable)
func (cur *Cursor) setErrorOffset(err error) {
	if err == nil {
		return
	}
	if x, ok := errgo.Cause(err).(*Error); ok {
		x.Offset = cur.getErrorOffset()
	}
}

// internalExecute performs the work of executing a cursor and
// sets the rowcount appropriately regardless of whether an error takes place.
func (cur *Cursor) internalExecute(numIters uint) error {
	if CTrace {
		ctrace("%s.internalExecute(%d)", cur, numIters)
	}

	var mode C.ub4

	if cur.connection.autocommit {
		mode = C.OCI_COMMIT_ON_SUCCESS
	} else {
		mode = C.OCI_DEFAULT
	}

	// Py_BEGIN_ALLOW_THREADS
	debug("%p.StmtExecute(%s, mode=%d) in internalExecute", cur,
		cur.statement, mode)
	if CTrace {
		ctrace("internalExecute.OCIStmtExecute(conn=%p, cur=%p, env=%p,"+
			" iters=%d, rowOff=%d, mode=%d)",
			cur.connection.handle, cur.handle,
			cur.environment.errorHandle, numIters, 0, mode)
	}
	if err := cur.environment.CheckStatus(
		C.OCIStmtExecute(cur.connection.handle,
			cur.handle, cur.environment.errorHandle,
			C.ub4(numIters), 0, // iters, rowOff
			nil, nil, // snapIn, snapOut
			mode),
		"internalExecute"); err != nil {
		cur.setErrorOffset(err)
		return errgo.Mask(err)
	}
	return errgo.Mask(cur.setRowCount())
}

// getStatementType determines if the cursor is executing a select statement.
func (cur *Cursor) getStatementType() error {
	var statementType C.ub2
	var vsize C.ub4
	if CTrace {
		ctrace("getStatementType.OCIAttrGet(%p, HTYPE_STMT, &stt=%p, &vsize=%p, ATTR_SMT_TYPE, errh=%p)",
			cur.handle, &statementType, &vsize, cur.environment.errorHandle)
	}
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			unsafe.Pointer(&statementType), &vsize, C.OCI_ATTR_STMT_TYPE,
			cur.environment.errorHandle),
		"getStatementType"); err != nil {
		return errgo.Mask(err)
	}
	cur.statementType = int(statementType)
	if CTrace {
		ctrace("statement type is %d", cur.statementType)
	}
	if cur.fetchVariables != nil {
		cur.fetchVariables = nil
	}

	return nil
}

// fixupBoundCursor fixes a cursor so that fetching and returning cursor
// descriptions are successful after binding a cursor to another cursor.
func (cur *Cursor) fixupBoundCursor() error {
	if cur.handle != nil && cur.statementType < 0 {
		if err := cur.getStatementType(); err != nil {
			return errgo.Mask(err)
		}
		if cur.statementType == C.OCI_STMT_SELECT {
			if err := cur.performDefine(); err != nil {
				return errgo.Mask(err)
			}
		}
		if err := cur.setRowCount(); err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

// itemDecriptionHelper is a helper for Cursor_ItemDescription() used
// so that it is not necessary to constantly free the descriptor
// when an error takes place.
func (cur *Cursor) itemDescriptionHelper(pos uint, param *C.OCIParam) (desc VariableDescription, err error) {
	var (
		internalSize, charSize C.ub2
		varType                *VariableType
		displaySize            int
		precision              C.sb2
		nullOk                 C.ub1
		scale                  C.ub1
	)

	// logPrefix := fmt.Sprintf("iDH(%d, %v) ", pos, param)
	logg := func(format string, args ...interface{}) {
		// debug(logPrefix+format, args...)
	}
	// acquire usable type of intem
	if varType, err = cur.environment.varTypeByOracleDescriptor(param); err != nil {
		err = errgo.Mask(err)
		return
	}
	logg("varType=%s", varType)

	// acquire internal size of item
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_DATA_SIZE, unsafe.Pointer(&internalSize),
		"itemDescription: internal size"); err != nil {
		err = errgo.Mask(err)
		return
	}
	logg("internalSize=%d", internalSize)

	// acquire character size of item
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_CHAR_SIZE, unsafe.Pointer(&charSize),
		"itemDescription(): character size"); err != nil {
		err = errgo.Mask(err)
		return
	}
	logg("charSize=%d", charSize)

	var name []byte
	// aquire name of item
	if name, err = cur.environment.AttrGetName(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_NAME, "itemDescription(): name"); err != nil {
		err = errgo.Mask(err)
		return
	}
	logg("name=%s", name)

	// lookup precision and scale
	if varType.IsNumber() {
		if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_SCALE, unsafe.Pointer(&scale),
			"itemDescription(): scale"); err != nil {
			err = errgo.Mask(err)
			return
		}
		logg("scale=%d", scale)
		if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_PRECISION, unsafe.Pointer(&precision),
			"itemDescription(): precision"); err != nil {
			err = errgo.Mask(err)
			return
		}
		logg("precision=%d", precision)
	}

	// lookup whether null is permitted for the attribute
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_IS_NULL, unsafe.Pointer(&nullOk),
		"itemDescription(): nullable"); err != nil {
		err = errgo.Mask(err)
		return
	}
	logg("nullOk=%d", nullOk)

	// set display size based on data type
	switch {
	case varType.IsString():
		displaySize = int(charSize)
	case varType.IsBinary():
		displaySize = int(internalSize)
	// case variable.IsFixed():
	case varType.IsNumber():
		if precision > 0 {
			displaySize = int(precision + 1)
			if scale > 0 {
				displaySize += int(scale + 1)
			}
		} else {
			displaySize = 127
		}
	case varType.IsDate():
		displaySize = 23
	default:
		displaySize = -1
	}

	logg("name=%s env=%v", name, cur.environment)
	logg("name2=%s", cur.environment.FromEncodedString(name))
	desc = VariableDescription{
		Name:        cur.environment.FromEncodedString(name),
		Type:        int(varType.oracleType),
		DisplaySize: displaySize, InternalSize: int(internalSize),
		Precision: int(precision), Scale: int(scale),
		NullOk: nullOk != 0,
	}
	return
}

// itemDescription returns a tuple describing the item at the given position.
func (cur *Cursor) itemDescription(pos uint) (desc VariableDescription, err error) {
	var param *C.OCIParam

	// acquire parameter descriptor
	if CTrace {
		ctrace("OCIParamGet", cur.handle, "HTYPE_STMT", cur.environment.errorHandle,
			&param, pos)
	}
	if err = cur.environment.CheckStatus(
		C.OCIParamGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			cur.environment.errorHandle,
			(*unsafe.Pointer)(unsafe.Pointer(&param)), C.ub4(pos)),
		"itemDescription(): parameter"); err != nil {
		return
	}

	// use helper routine to get tuple
	desc, err = cur.itemDescriptionHelper(pos, param)
	if CTrace {
		ctrace("OCIDescriptorFree", param, "DTYPE_PARAM")
	}
	C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)
	return
}

// GetDescription returns a list of 7-tuples consisting of the
// description of the define variables.
func (cur *Cursor) GetDescription() (descs []VariableDescription, err error) {
	var numItems int

	// make sure the cursor is open
	if !cur.isOpen {
		err = CursorIsClosed
		return
	}

	// fixup bound cursor, if necessary
	if err = cur.fixupBoundCursor(); err != nil {
		return
	}

	// if not a query, return None
	if cur.statementType != C.OCI_STMT_SELECT {
		return
	}

	// determine number of items in select-list
	if _, err = cur.environment.AttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		C.OCI_ATTR_PARAM_COUNT, unsafe.Pointer(&numItems),
		"GetDescription()"); err != nil {
		return
	}

	// create a list of the required length
	descs = make([]VariableDescription, numItems)

	// create tuples corresponding to the select-items
	for index := uint(0); index < uint(numItems); index++ {
		if descs[int(index)], err = cur.itemDescription(index + 1); err != nil {
			return
		}
	}

	return
}

// Close the cursor.
func (cur *Cursor) Close() {
	if CTrace {
		ctrace("%s.Close", cur)
	}
	// make sure we are actually open
	if !cur.isOpen {
		return
	}
	// close the cursor
	cur.freeHandle() // no error checking?

	cur.isOpen = false
}

// setBindVariableHelper is a helper for setting a bind variable.
func (cur *Cursor) setBindVariableHelper(numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool, // defer type assignment if null?
	value interface{}, // value to bind
	origVar *Variable, // original variable bound
) (newVar *Variable, err error) {
	if CTrace {
		ctrace("%s.setBindVariableHelper", cur)
	}

	var isValueVar bool

	// initialization
	newVar = nil
	isValueVar = isVariable(value) //FIXME

	// handle case where variable is already bound
	debug("origVar=%#v value=%#v (%T)", origVar, value, value)
	if origVar != nil {

		// if the value is a variable object, rebind it if necessary
		if isValueVar {
			if origVar != value {
				newVar = value.(*Variable)
			}

			// if the number of elements has changed, create a new variable
			// this is only necessary for executemany() since execute() always
			// passes a value of 1 for the number of elements
		} else {
			newTyp, _, _, e := VarTypeByValue(value)
			if e != nil {
				err = e
				return
			}
			if newTyp != origVar.typ {
				origVar = nil
			} else {
				if numElements > origVar.allocatedElements {
					if newVar, err = cur.NewVariable(numElements, origVar.typ,
						origVar.size); err != nil {
						return
					}
					if err = newVar.SetValue(arrayPos, value); err != nil {
						return
					}

					// otherwise, attempt to set the value
				} else if origVar.SetValue(arrayPos, value); err != nil {

					// executemany() should simply fail after the first element
					if arrayPos > 0 {
						return
					}

					// anything other than index error or type error should fail
					/*
					   if (!PyErr_ExceptionMatches(PyExc_IndexError) &&
					           !PyErr_ExceptionMatches(PyExc_TypeError))
					       return -1;
					*/
					// return

					// clear the exception and try to create a new variable
					origVar = nil
				}
			}
		}
	}

	// if no original variable used, create a new one
	if origVar == nil {
		debug("origVar is Nil, isValueVar? %b", isValueVar)

		// if the value is a variable object, bind it directly
		if isValueVar && value != nil && value.(*Variable) != nil {
			debug("A")
			newVar = value.(*Variable)
			debug("newVar=%#v typ.Name=%s", newVar, string(newVar.typ.Name))
			debug("newVar.typ=%#v", newVar.typ)
			newVar.boundPos = 0
			newVar.boundName = ""

			// otherwise, create a new variable, unless the value is None and
			// we wish to defer type assignment
		} else if value != nil || !deferTypeAssignment {
			if newVar, err = cur.NewVariableByValue(value, numElements); err != nil {
				return
			}
			if err = newVar.SetValue(arrayPos, value); err != nil {
				return
			}
			debug("%v.SetValue(%d, %v)", newVar, arrayPos, value)
		}

		if newVar.typ.Name == "" {
			log.Fatalf("uninitialized type for %s", newVar)
		}
	} else {
		if origVar.typ.Name == "" {
			log.Fatalf("uninitialized type for %s", origVar)
		}
	}

	return
}

// setBindVariablesByPos creates or sets bind variables by position.
func (cur *Cursor) setBindVariablesByPos(parameters []interface{}, // parameters to bind
	numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool) ( // defer type assignment if null?
	err error) {

	if CTrace {
		ctrace("%s.setBindVariablesByPos", cur)
	}

	var origNumParams int
	// PyObject *key, *value, *origVar;
	var origVar, newVar *Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return ListIsEmpty
	}
	if cur.bindVarsArr != nil {
		origNumParams = len(cur.bindVarsArr)
		newNumParams := len(parameters)
		for _, v := range cur.bindVarsArr {
			v.unbind()
		}
		if newNumParams < origNumParams {
			cur.bindVarsArr = cur.bindVarsArr[:newNumParams]
		}
	} else {
		cur.bindVarsArr = make([]*Variable, len(parameters))
	}
	if len(cur.bindVarsMap) > 0 {
		for k, v := range cur.bindVarsMap {
			delete(cur.bindVarsMap, k)
			v.unbind()
		}
	}

	// handle positional binds
	for i, v := range parameters {
		origVar = nil
		if i < origNumParams {
			origVar = cur.bindVarsArr[i]
		}
		if newVar, err = cur.setBindVariableHelper(numElements, arrayPos, deferTypeAssignment, v, origVar); err != nil {
			return errgo.Mask(err)
		}
		if newVar != nil {
			if i < len(cur.bindVarsArr) {
				cur.bindVarsArr[i] = newVar
			} else {
				cur.bindVarsArr = append(cur.bindVarsArr, newVar)
			}
		}
	}
	return
}

// setBindVariablesByName creates or sets bind variables by name (nap).
func (cur *Cursor) setBindVariablesByName(parameters map[string]interface{}, // parameters to bind
	numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool, // defer type assignment if null?
) (err error) {
	// PyObject *key, *value, *origVar;
	var origVar, newVar *Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return ListIsEmpty
	}
	if cur.bindVarsMap == nil {
		cur.bindVarsMap = make(map[string]*Variable, len(parameters))
	} else if len(cur.bindVarsMap) > 0 {
		for k, v := range cur.bindVarsMap {
			delete(cur.bindVarsMap, k)
			v.unbind()
		}
	}
	if len(cur.bindVarsArr) > 0 {
		for _, v := range cur.bindVarsArr {
			v.unbind()
		}
		cur.bindVarsArr = cur.bindVarsArr[:0]
	}

	// handle named binds
	for k, v := range parameters {
		origVar = cur.bindVarsMap[k]
		if newVar, err = cur.setBindVariableHelper(numElements, arrayPos, deferTypeAssignment,
			v, origVar); err != nil {
			return errgo.Mask(err)
		}
		if newVar != nil {
			cur.bindVarsMap[k] = newVar
		}
	}

	return
}

// performBind performs the binds on the cursor.
func (cur *Cursor) performBind() (err error) {
	if CTrace {
		ctrace("%s.performBind", cur)
	}
	// PyObject *key, *var;
	// Py_ssize_t pos;
	// ub2 i;

	// ensure that input sizes are reset
	// this is done before binding is attempted so that if binding fails and
	// a new statement is prepared, the bind variables will be reset and
	// spurious errors will not occur
	cur.setInputSizes = 0

	if CTrace {
		ctrace("bindVarsArr=%v", cur.bindVarsArr)
		ctrace("bindVarsMap=%v", cur.bindVarsMap)
	}
	// set values and perform binds for all bind variables
	if cur.bindVarsMap != nil {
		for k, v := range cur.bindVarsMap {
			if err = v.Bind(cur, k, 1); err != nil {
				return errgo.Mask(err)
			}
		}
		/*
			log.Printf("statementVars: %v", FindStatementVars(string(cur.statement)))
			for k, num := range FindStatementVars(string(cur.statement)) {
				if num <= 1 {
					continue
				}
				for i := 1; i < num; i++ {
					if err = cur.bindVarsMap[k].Bind(cur, k, uint(i+1)); err != nil {
						return err
					}
				}
			}
		*/
	} else if cur.bindVarsArr != nil {
		for i, v := range cur.bindVarsArr {
			if err = v.Bind(cur, "", uint(i+1)); err != nil {
				return errgo.Mask(err)
			}
		}
	}
	return nil
}

// createRow creates an object for the row. The object created is a tuple unless a row
// factory function has been defined in which case it is the result of the
// row factory function called with the argument tuple that would otherwise be
// returned.
func (cur *Cursor) createRow() ([]interface{}, error) {
	var err error
	// create a new tuple
	numItems := len(cur.fetchVariables)
	row := make([]interface{}, numItems)

	// acquire the value for each item
	for pos, v := range cur.fetchVariables {
		if row[pos], err = v.GetValue(uint(cur.rowNum)); err != nil {
			return nil, errgo.Mask(err)
		}
	}

	// increment row counters
	cur.rowNum++
	cur.rowCount++

	/*
	   // if a row factory is defined, call it
	   if (self->rowFactory && self->rowFactory != Py_None) {
	       result = PyObject_CallObject(self->rowFactory, tuple);
	       Py_DECREF(tuple);
	       return result;
	   }

	   return tuple;
	*/
	return row, nil
}

// fetchInto fetches current row's columns into the given pointers
func (cur *Cursor) fetchInto(row ...interface{}) error {
	var err error
	// create a new tuple
	numItems := len(cur.fetchVariables)
	if numItems != len(row) {
		return errgo.Newf("colnum mismatch: got %d, have %d", len(row), numItems)
	}

	// acquire the value for each item
	var (
		ok bool
		x  *interface{}
		v  *Variable
	)
	for pos := 0; pos < len(row); pos++ {
		if x, ok = row[pos].(*interface{}); !ok {
			// return fmt.Errorf("awaited *interface{}, got %T (%+v) at pos %d (row=%+v)",
			// 	dst, dst, pos, row)
			x = &row[pos]
		}
		v = cur.fetchVariables[pos]
		if err = v.GetValueInto(x, uint(cur.rowNum)); err != nil {
			return errgo.Mask(

				// row[pos] = *x
				err)
		}

	}

	// increment row counters
	cur.rowNum++
	cur.rowCount++
	// debug("fetchInto rn=%d rc=%d row=%+v", cur.rowNum, cur.rowCount, row)

	return nil
}

// IsDDL checks whether the cursor is of Data Definition Language
func (cur *Cursor) IsDDL() bool {
	return cur.statementType == C.OCI_STMT_CREATE ||
		cur.statementType == C.OCI_STMT_DROP ||
		cur.statementType == C.OCI_STMT_ALTER
}

// internalPrepare is an internal method for preparing a statement for execution.
func (cur *Cursor) internalPrepare(statement string, statementTag string) error {
	// make sure we don't get a situation where nothing is to be executed
	if statement == "" && cur.statement == nil {
		return ProgrammingError("no statement specified and no prior statement prepared")
	}

	if CTrace {
		ctrace("%s.internalPrepare(%q)", cur, statement)
	}

	// but go ahead and prepare anyway for create, alter and drop statments
	if statement == "" || statement == string(cur.statement) {
		// FIXME why would double prepare be good??
		if statementTag == "" || statementTag == string(cur.statementTag) {
			return nil
		}
		if !cur.IsDDL() {
			return nil
		}
		if statement == "" {
			statement = string(cur.statement)
		}
	}
	// keep track of the statement
	cur.statement = []byte(statement)
	if statementTag == "" {
		cur.statementTag = hashTag(cur.statement)
	} else {
		cur.statementTag = []byte(statementTag)
	}
	// release existing statement, if necessary
	if err := cur.freeHandle(); err != nil {
		return errgo.Mask(

			// prepare statement
			err)
	}

	cur.isOwned = false
	if usePrepare2 {
		debug(`%p.Prepare2 for "%s" [%x]`, cur, cur.statement, cur.statementTag)
		// Py_BEGIN_ALLOW_THREADS
		if CTrace {
			ctrace("internalPrepare.OCIStmtPrepare2(conn=%p, &cur=%p, env=%p, "+
				"statement=%q, len=%d, tag=%v, tagLen=%d, NTV_SYNTAX, DEFAULT)",
				cur.connection.handle, &cur.handle, cur.environment.errorHandle,
				cur.statement, len(cur.statement), cur.statementTag, len(cur.statementTag))
		}
		if err := cur.environment.CheckStatus(
			C.OCIStmtPrepare2(cur.connection.handle, &cur.handle,
				cur.environment.errorHandle,
				(*C.OraText)(unsafe.Pointer(&cur.statement[0])), C.ub4(len(cur.statement)),
				(*C.OraText)(unsafe.Pointer(&cur.statementTag[0])), C.ub4(len(cur.statementTag)),
				C.OCI_NTV_SYNTAX, C.OCI_DEFAULT),
			"internalPrepare"); err != nil {
			// Py_END_ALLOW_THREADS
			// this is needed to avoid "invalid handle" errors since Oracle doesn't
			// seem to leave the pointer alone when an error is raised but the
			// resulting handle is still invalid
			cur.handle = nil
			return err
		}
	} else {
		if cur.handle == nil {
			if err := cur.allocateHandle(); err != nil {
				return errgo.Mask(err)
			}
		}
		debug(`%p.Prepare for "%s"`, cur)
		// Py_BEGIN_ALLOW_THREADS
		if CTrace {
			ctrace("internalPrepare.OCIStmtPrepare(conn=%p, &cur=%p, env=%p, "+
				"statement=%q, len=%d, NTV_SYNTAX, DEFAULT)",
				cur.connection.handle, &cur.handle, cur.environment.errorHandle,
				cur.statement, len(cur.statement))
		}
		if err := cur.environment.CheckStatus(
			C.OCIStmtPrepare(cur.handle,
				cur.environment.errorHandle,
				(*C.OraText)(unsafe.Pointer(&cur.statement[0])), C.ub4(len(cur.statement)),
				C.OCI_NTV_SYNTAX, C.OCI_DEFAULT),
			"internalPrepare"); err != nil {
			// Py_END_ALLOW_THREADS
			// this is needed to avoid "invalid handle" errors since Oracle doesn't
			// seem to leave the pointer alone when an error is raised but the
			// resulting handle is still invalid
			cur.handle = nil
			return err
		}
	}
	if CTrace {
		ctrace("internalPrepare done, cur.handle=%x", cur.handle)
	}
	// debug("prepared")

	// clear bind variables, if applicable
	if cur.setInputSizes <= 1 {
		cur.bindVarsArr = nil
		cur.bindVarsMap = nil
	}

	// clear row factory, if applicable
	// cur.rowFactory = nil

	// determine if statement is a query
	if err := cur.getStatementType(); err != nil {
		return errgo.Mask(err)
	}

	return nil
}

// Parse the statement without executing it. This also retrieves information
// about the select list for select statements.
func (cur *Cursor) Parse(statement string) error {
	var mode C.ub4

	// statement text is expected
	if statement == "" {
		return nil
	}

	// make sure the cursor is open
	if !cur.isOpen {
		return nil
	}

	// prepare the statement
	if err := cur.internalPrepare(statement, ""); err != nil {
		return errgo.Mask(

			// parse the statement
			err)
	}

	if cur.statementType == C.OCI_STMT_SELECT {
		mode = C.OCI_DESCRIBE_ONLY
	} else {
		mode = C.OCI_PARSE_ONLY
	}
	// Py_BEGIN_ALLOW_THREADS
	log.Printf("%p.StmtExecute(%s, mode=%d) in Parse", cur, cur.statement, mode)
	if CTrace {
		ctrace("OCIStmtExecute", cur.connection.handle, cur.handle,
			cur.environment.errorHandle, 0, 0, nil, nil, mode)
	}
	if err := cur.environment.CheckStatus(
		C.OCIStmtExecute(cur.connection.handle, cur.handle,
			cur.environment.errorHandle,
			0, 0, //iters, rowoff
			nil, nil, //snapIn, snapOut
			mode),
		"parse"); err != nil {
		// Py_END_ALLOW_THREADS
		return errgo.Mask(err)
	}

	return nil
}

// Prepare the statement for execution. statementTag is optional
func (cur *Cursor) Prepare(statement, statementTag string) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return nil
	}

	// prepare the statement
	if err := cur.internalPrepare(statement, statementTag); err != nil {
		return errgo.Mask(err)
	}
	return nil
}

// Calculate the size of the statement that is to be executed.
func (cur *Cursor) callCalculateSize(
	name string, // name of procedure/function to call
	returnValue *Variable, // return value variable (optional)
	listOfArguments []interface{}, // list of positional arguments
	keywordArguments map[string]interface{}, // dictionary of keyword arguments
) (size int, err error) { // statement size (OUT)

	// set base size without any arguments
	size = 17

	// add any additional space required to handle the return value
	if returnValue != nil {
		size += 6
	}

	// assume up to 9 characters for each positional argument
	// this allows up to four digits for the placeholder if the bind variale
	// is a boolean value
	if listOfArguments != nil {
		if len(listOfArguments) == 0 {
			return -1, ListIsEmpty
		}
		size += len(listOfArguments) * 9
	}

	// assume up to 15 characters for each keyword argument
	// this allows up to four digits for the placeholder if the bind variable
	// is a boolean value
	if keywordArguments != nil {
		if len(keywordArguments) == 0 {
			return -1, ListIsEmpty
		}
		size += len(keywordArguments) * 15
	}

	return size, nil
}

// Determine the statement and the bind variables to bind to the statement
// that is created for calling a stored procedure or function.
func (cur *Cursor) callBuildStatement(
	name string, // name of procedure/function to call
	returnValue *Variable, // return value variable (optional)
	listOfArguments []interface{}, // arguments
	keywordArguments map[string]interface{}, // keyword arguments
) (statement string, // statement string
	bindVarsArr []interface{}, // variables to bind
	err error) {
	// initialize the bind variables to the list of positional arguments
	allArgNum := 0
	if listOfArguments != nil {
		allArgNum += len(listOfArguments)
	}
	if keywordArguments != nil {
		allArgNum += len(keywordArguments)
	}
	if returnValue != nil {
		allArgNum++
	}

	bindVarsArr = make([]interface{}, allArgNum)

	// begin building the statement
	argNum := 1
	argChunksOffset := 0
	chunks := make([]string, 1, 32)
	chunks[0] = "begin "
	if returnValue != nil {
		chunks = append(chunks, ":1 := ")
		// insert the return variable
		bindVarsArr[0] = returnValue
		argChunksOffset = 1 // argNum=1 is used for the return
		argNum++
	}
	chunks = append(chunks, name, "(")

	// include any positional arguments first
	argchunks := make([]string, allArgNum-(argNum-1))
	if listOfArguments != nil && len(listOfArguments) > 0 {
		plus := ""
		for _, arg := range listOfArguments {
			if _, ok := arg.(bool); ok {
				plus = " = 1"
			} else {
				plus = ""
			}
			argchunks[argNum-1-argChunksOffset] = ":" + strconv.Itoa(argNum) + plus
			bindVarsArr[argNum-1] = arg
			argNum++
		}
	}

	// next append any keyword arguments
	if keywordArguments != nil && len(keywordArguments) > 0 {
		plus := ""
		for k, arg := range keywordArguments {
			if _, ok := arg.(bool); ok {
				plus = " = 1"
			} else {
				plus = ""
			}
			argchunks[argNum-1-argChunksOffset] = k + "=>:" + strconv.Itoa(argNum) + plus
			bindVarsArr[argNum-1] = arg
			argNum++
		}
	}

	// create statement object
	statement = strings.Join(chunks, "") + strings.Join(argchunks, ", ") + "); end;"
	err = nil
	return
}

// Call a stored procedure or function.
func (cur *Cursor) call( // cursor to call procedure/function
	returnValue *Variable, // return value variable (optional
	name string, // name of procedure/function to call
	listOfArguments []interface{}, // arguments
	keywordArguments map[string]interface{}, // keyword arguments
) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	// determine the statement size
	_, err := cur.callCalculateSize(name, returnValue, listOfArguments,
		keywordArguments)
	if err != nil {
		return errgo.Mask(

			// determine the statement to execute and the argument to pass
			err)
	}

	statement, bindVarArrs, e := cur.callBuildStatement(name, returnValue, listOfArguments,
		keywordArguments)
	if e != nil {
		return e
	}

	// execute the statement on the cursor
	return cur.Execute(statement, bindVarArrs, nil)
}

// CallFunc calls a stored function and return the return value of the function.
func (cur *Cursor) CallFunc(
	name string,
	returnType VariableType,
	parameters []interface{},
	keywordParameters map[string]interface{}) (interface{}, error) {

	// create the return variable
	variable, err := returnType.NewVariable(cur, 0, 0)
	if err != nil {
		return nil, errgo.Mask(

			// call the function
			err)
	}

	if err = cur.call(variable, name, parameters, keywordParameters); err != nil {
		return nil, errgo.Mask(

			// determine the results
			err)
	}

	return variable.GetValue(0)
}

// CallProc calls a stored procedure and return the (possibly modified) arguments.
func (cur *Cursor) CallProc(name string,
	parameters []interface{}, keywordParameters map[string]interface{}) (
	results []interface{}, err error) {
	// call the stored procedure
	if err = cur.call(nil, name, parameters, keywordParameters); err != nil {
		return
	}

	// create the return value
	numArgs := len(cur.bindVarsArr) + len(cur.bindVarsMap)
	results = make([]interface{}, numArgs)
	var val interface{}
	i := 0
	for _, v := range cur.bindVarsArr {
		if val, err = v.GetValue(0); err != nil {
			return
		}
		results[i] = val
		i++
	}
	/*
	   for _, v := range cur.bindVarsMap {
	   	if val, err = v.GetValue(); err != nil {
	   		return err
	   	}
	   	results[i] = val
	   	i++
	   }
	*/

	return
}

// Execute the statement.
// For pointer-backed Variables, get their values, too.
func (cur *Cursor) Execute(statement string,
	listArgs []interface{}, keywordArgs map[string]interface{}) error {

	if CTrace {
		ctrace("%s.Execute; IsOpen? %t", cur, cur.isOpen)
	}

	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	if BypassMultipleArgs && len(listArgs) == 0 && len(keywordArgs) > 0 {
		// make bind names unique (against ORA-1008)
		toBeChanged := make(map[int][2]string, 8)
		positions := make([]int, 0, 8)
		for k, pos := range FindStatementVars(statement) {
			if len(pos) <= 1 {
				continue
			}
			for i, p := range pos[1:] {
				positions = append(positions, p)
				toBeChanged[p] = [2]string{k, k + "##" + strconv.Itoa(i+1)}
			}
		}
		if len(positions) > 0 {
			sort.Ints(positions)
			for i := len(positions) - 1; i >= 0; i-- {
				p := positions[i]
				log.Printf("changing %q to %q at %d", toBeChanged[p][0], toBeChanged[p][1], p)
				statement = statement[:p+1] + toBeChanged[p][1] + statement[p+1+len(toBeChanged[p][0]):]
				keywordArgs[toBeChanged[p][1]] = keywordArgs[toBeChanged[p][0]]
			}
			log.Printf("statement=%q", statement)
		}
	}

	var err error
	// prepare the statement, if applicable
	if err = cur.internalPrepare(statement, ""); err != nil {
		return errgo.Mask(

			// debug("internalPrepare done, performing binds")
			// perform binds
			err)
	}

	if listArgs != nil && len(listArgs) > 0 {
		if err = cur.setBindVariablesByPos(listArgs, 1, 0, false); err != nil {
			return errgo.Mask(err)
		}
	} else if keywordArgs != nil && len(keywordArgs) > 0 {
		if err = cur.setBindVariablesByName(keywordArgs, 1, 0, false); err != nil {
			return errgo.Mask(err)
		}
	}
	if err = cur.performBind(); err != nil {
		return errgo.Mask(err)
	}
	debug("bind done, executing statement listArgs=%v", listArgs)

	// execute the statement
	isQuery := cur.statementType == C.OCI_STMT_SELECT
	numIters := uint(1)
	if isQuery {
		numIters = 0
	}
	if err = cur.internalExecute(numIters); err != nil {
		err = errgo.Mask(err)
		if oraErr, ok := errgo.Cause(err).(*Error); ok && oraErr.Code == 1008 { // ORA-1008: not all variables bound
			inStmt := CountStatementVars(statement)
			unnecessary := make([]string, 0, len(listArgs)+len(keywordArgs))
			morethanonce := make([]string, 0, len(inStmt))
			if len(listArgs) > 0 {
				for i := range listArgs {
					k := strconv.Itoa(i + 1)
					if _, ok := inStmt[k]; ok {
						delete(inStmt, k)
					} else {
						unnecessary = append(unnecessary, k)
					}
				}
			} else {
				for k := range keywordArgs {
					if i, ok := inStmt[k]; ok {
						delete(inStmt, k)
						if i > 1 {
							morethanonce = append(morethanonce, k)
						}
					} else {
						unnecessary = append(unnecessary, k)
					}
				}
			}
			missing := make([]string, len(inStmt))
			i := 0
			for k := range inStmt {
				missing[i] = k
				i++
			}
			sort.Strings(missing)
			sort.Strings(unnecessary)
			sort.Strings(morethanonce)
			oraErr.Message = (oraErr.Message +
				"\nmissing var: " + strings.Join(missing, ",") +
				"\nunnecessary: " + strings.Join(unnecessary, ",") +
				"\nmorethanonce: " + strings.Join(morethanonce, ",") +
				fmt.Sprintf("\nqry=%q\narr=%s, map=%s", statement, cur.bindVarsArr, cur.bindVarsMap))
			//err = oraErr
		}
		return err // already masked
	}
	// debug("executed, calling performDefine")

	// perform defines, if necessary
	if isQuery && cur.fetchVariables == nil {
		if err = cur.performDefine(); err != nil {
			return errgo.Mask(err)
		}
	}

	// reset the values of setoutputsize()
	cur.outputSize = -1
	cur.outputSizeColumn = -1

	if !isQuery {
		return errgo.Mask(cur.getPtrValues())
	}

	return nil
}

// CountStatementVars returns a mapping of the variable names found in statement,
// with the number of occurence as the map value
func CountStatementVars(statement string) map[string]int {
	positions := FindStatementVars(statement)
	inStmt := make(map[string]int, len(positions))
	for k, pos := range positions {
		inStmt[k] = len(pos)
	}
	return inStmt
}

// FindStatementVars returns a mapping of the variable names found in statement,
// with the number of occurence as the map value
func FindStatementVars(statement string) map[string][]int {
	inStmt := make(map[string][]int, 16)
	state := 0
	nm := make([]rune, 0, 30)
	for i := 0; i < len(statement); i++ {
		r := rune(statement[i])
		switch {
		case state == 0 && r == ':':
			state++
		case state == 1:
			if '0' <= r && r <= '9' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || r == '_' || r == '#' {
				nm = append(nm, r)
			} else {
				if len(nm) > 0 {
					name := string(nm)
					inStmt[name] = append(inStmt[name], i-1-len(name))
					nm = nm[:0]
				}
				state = 0
			}
		}
	}
	return inStmt
}

// ExecuteMany executes the statement many times.
// The number of times is equivalent to the number of elements in the array of dictionaries.
func (cur *Cursor) ExecuteMany(statement string, params []map[string]interface{}) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	var err error
	// prepare the statement
	if err = cur.internalPrepare(statement, ""); err != nil {
		return errgo.Mask(

			// queries are not supported as the result is undefined
			err)
	}

	if cur.statementType == C.OCI_STMT_SELECT {
		return QueriesNotSupported
	}

	// perform binds
	numRows := len(params)
	for i, arguments := range params {
		if err = cur.setBindVariablesByName(arguments, uint(numRows), uint(i),
			(i < numRows-1)); err != nil {
			return errgo.Mask(err)
		}
	}
	if err = cur.performBind(); err != nil {
		return errgo.Mask(

			// execute the statement, but only if the number of rows is greater than
			// zero since Oracle raises an error otherwise
			err)
	}

	if numRows > 0 {
		if err = cur.internalExecute(uint(numRows)); err != nil {
			return errgo.Mask(err)
		}
	}

	return nil
}

// ExecuteManyPrepared executes the prepared statement the number of times requested.
// At this point, the statement must have been already prepared and the bind variables
// must have their values set.
func (cur *Cursor) ExecuteManyPrepared(numIters uint) error {
	if numIters > cur.bindArraySize {
		return errgo.Newf("iterations exceed bind array size")
	}

	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	// queries are not supported as the result is undefined
	if cur.statementType == C.OCI_STMT_SELECT {
		return QueriesNotSupported
	}

	var err error
	// perform binds
	if err = cur.performBind(); err != nil {
		return errgo.Mask(

			// execute the statement
			err)
	}

	return errgo.Mask(cur.internalExecute(numIters))
}

// Verify that fetching may happen from this cursor.
func (cur *Cursor) verifyFetch() error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	// fixup bound cursor, if necessary
	if err := cur.fixupBoundCursor(); err != nil {
		return errgo.Mask(

			// make sure the cursor is for a query
			err)
	}

	if cur.statementType != C.OCI_STMT_SELECT {
		return errgo.New("not a query")
	}

	return nil
}

// Performs the actual fetch from Oracle.
func (cur *Cursor) internalFetch(numRows uint) error {
	if cur.fetchVariables == nil {
		return errgo.New("query not executed")
	}
	debug("fetchVars=%v", cur.fetchVariables)
	var err error
	for _, v := range cur.fetchVariables {
		if CTrace {
			ctrace("fetchvar %d=%s", v.internalFetchNum, v)
			ctrace("typ=%v", v.typ)
		}
		v.internalFetchNum++
		// debug("typ=%s", v.typ)
		// debug("preFetch=%s", v.typ.preFetch)
		if v.typ.preFetch != nil {
			if err = v.typ.preFetch(v); err != nil {
				return errgo.Mask(err)
			}
		}
	}
	// debug("StmtFetch numRows=%d", numRows)
	// Py_BEGIN_ALLOW_THREADS
	if CTrace {
		ctrace("OCIStmtFetch(cur=%p, env=%p, numRows=%d, FETCH_NEXT, DEFAULT)",
			cur.handle, cur.environment.errorHandle, numRows)
	}
	if err = cur.environment.CheckStatus(
		C.OCIStmtFetch(cur.handle, cur.environment.errorHandle,
			C.ub4(numRows), C.OCI_FETCH_NEXT, C.OCI_DEFAULT),
		"internalFetch(): fetch"); err != nil && err != NoDataFound {
		return err
	}
	// debug("fetched, getting row count")
	var rowCount int
	if _, err = cur.environment.AttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		C.OCI_ATTR_ROW_COUNT, unsafe.Pointer(&rowCount),
		"internalFetch(): row count"); err != nil {
		return errgo.Mask(

			// debug("row count = %d", rowCount)
			err)
	}

	cur.actualRows = rowCount - cur.rowCount
	cur.rowNum = 0
	return nil
}

// Returns an integer indicating if more rows can be retrieved from the
// cursor.
func (cur *Cursor) moreRows() (bool, error) {
	// debug("moreRows rowNum=%d actualRows=%d", cur.rowNum, cur.actualRows)
	if cur.rowNum >= cur.actualRows {
		if cur.actualRows < 0 || uint(cur.actualRows) == cur.fetchArraySize {
			if err := cur.internalFetch(cur.fetchArraySize); err != nil {
				return false, errgo.Mask(err)
			}
		}
		if cur.rowNum >= cur.actualRows {
			return false, nil
		}
	}
	return true, nil
}

// Return a list consisting of the remaining rows up to the given row limit
// (if specified).
func (cur *Cursor) multiFetch(rowLimit int) (results [][]interface{}, err error) {
	var ok bool
	var row []interface{}
	// create an empty list
	results = make([][]interface{}, 0, 2)

	// fetch as many rows as possible
	for rowNum := 0; rowLimit == 0 || rowNum < rowLimit; rowNum++ {
		if ok, err = cur.moreRows(); err != nil {
			return
		} else if !ok {
			break
		}
		if row, err = cur.createRow(); err != nil {
			return
		}
		results = append(results, row)
	}

	return
}

// FetchOne fetches a single row from the cursor.
func (cur *Cursor) FetchOne() (row []interface{}, err error) {
	// verify fetch can be performed
	if err = cur.verifyFetch(); err != nil {
		return
	}

	// setup return value
	if _, err = cur.moreRows(); err != nil {
		return
	}
	return cur.createRow()
}

// FetchOneInto fetches a single row from the cursor into the given column pointers
func (cur *Cursor) FetchOneInto(row ...interface{}) (err error) {
	// verify fetch can be performed
	if err = cur.verifyFetch(); err != nil {
		return
	}

	// setup return value
	var ok bool
	if ok, err = cur.moreRows(); err != nil {
		return
	} else if !ok {
		return io.EOF
	}
	err = cur.fetchInto(row...)
	debug("FetchOneInto result row=%v", row)
	return
}

// FetchMany fetches multiple rows from the cursor based on the arraysize.
// for default (arraySize) row limit, use negative rowLimit
func (cur *Cursor) FetchMany(rowLimit int) ([][]interface{}, error) {
	// parse arguments -- optional rowlimit expected
	if rowLimit < 0 {
		rowLimit = int(cur.arraySize)
	}

	// verify fetch can be performed
	if err := cur.verifyFetch(); err != nil {
		return nil, errgo.Mask(err)
	}

	return cur.multiFetch(rowLimit)
}

// FetchAll fetches all remaining rows from the cursor.
func (cur *Cursor) FetchAll() ([][]interface{}, error) {
	if err := cur.verifyFetch(); err != nil {
		return nil, errgo.Mask(err)
	}
	return cur.multiFetch(0)
}

// fetchRaw performs a raw fetch on the cursor; return the actual number of rows fetched.
func (cur *Cursor) fetchRaw(numRows uint) (int, error) {
	if numRows > cur.fetchArraySize {
		return -1, errgo.New("rows to fetch exceeds array size")
	}

	// do not attempt to perform fetch if no more rows to fetch
	if 0 < cur.actualRows && uint(cur.actualRows) < cur.fetchArraySize {
		return 0, nil
	}

	// perform internal fetch
	if err := cur.internalFetch(numRows); err != nil {
		return -1, errgo.Mask(err)
	}

	cur.rowCount += cur.actualRows
	numRowsFetched := cur.actualRows
	if cur.actualRows == int(numRows) {
		cur.actualRows = -1
	}
	return numRowsFetched, nil
}

// SetInputSizesByPos sets the sizes of the bind variables by position (array).
func (cur *Cursor) SetInputSizesByPos(types []VariableType) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	// eliminate existing bind variables
	if cur.bindVarsArr == nil {
		cur.bindVarsArr = make([]*Variable, 0, len(types))
	} else {
		cur.bindVarsArr = cur.bindVarsArr[:0]
	}
	cur.setInputSizes = 1

	// process each input
	var nv *Variable
	var err error
	for _, t := range types {
		if nv, err = t.NewVariable(cur, 0, 0); err != nil {
			return errgo.Mask(err)
		}
		cur.bindVarsArr = append(cur.bindVarsArr, nv)
	}
	return nil
}

// SetInputSizesByName sets the sizes of the bind variables by name (map).
func (cur *Cursor) SetInputSizesByName(types map[string]VariableType) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	// eliminate existing bind variables
	if cur.bindVarsMap == nil || len(cur.bindVarsMap) > 0 {
		cur.bindVarsMap = make(map[string]*Variable, len(types))
	}
	cur.setInputSizes = 1

	var err error
	// process each input
	for k, t := range types {
		if cur.bindVarsMap[k], err = t.NewVariable(cur, 0, 0); err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

// SetOutputSize sets the size of all of the long columns or just one of them.
// use -1 for outputSizeColumn if set outputSize for all columns!
func (cur *Cursor) SetOutputSize(outputSize, outputSizeColumn int) {
	cur.outputSize = outputSize
	cur.outputSizeColumn = outputSizeColumn
}

// GetBindNames returns a list of bind variable names.
func (cur *Cursor) GetBindNames() ([]string, error) {
	// make sure the cursor is open
	if !cur.isOpen {
		return nil, CursorIsClosed
	}

	// return result
	return cur.getBindNames()
}

/*
//-----------------------------------------------------------------------------
// Cursor_GetIter()
//   Return a reference to the cursor which supports the iterator protocol.
//-----------------------------------------------------------------------------
static PyObject *Cursor_GetIter(
    udt_Cursor *self)                   // cursor
{
    if (Cursor_VerifyFetch(self) < 0)
        return NULL;
    Py_INCREF(self);
    return (PyObject*) self;
}
*/

// getNext returns a reference to the cursor which supports the iterator protocol.
func (cur *Cursor) getNext() error {
	if err := cur.verifyFetch(); err != nil {
		return errgo.Mask(err)
	}
	if more, err := cur.moreRows(); err != nil {
		return errgo.Mask(err)
	} else if more {
		_, err = cur.createRow()
		return err
	}
	return io.EOF
}

var statementTagHash = fnv.New64a()

func hashTag(tag []byte) []byte {
	statementTagHash.Reset()
	statementTagHash.Write(tag)
	// hsh := statementTagHash.Sum(nil)
	// debug("hashTag(%s[%d])=%s[%d]", tag, len(tag), hsh, len(hsh))
	// return hsh
	return statementTagHash.Sum(make([]byte, 0, 8))
}
