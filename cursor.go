// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const int sizeof_OraText = sizeof(OraText);
*/
import "C"

import (
	"errors"
	"fmt"
	// "log"
	// "reflect"
	"unsafe"
)

// func init() {
// log.Printf("bindInfo_elementSize=%d", C.bindInfo_elementSize)
// }

type Cursor struct {
	handle                                      *C.OCIStmt
	connection                                  *Connection
	environment                                 *Environment
	bindVarsArr                                 []*Variable
	bindVarsMap                                 map[string]*Variable
	fetchVariables                              []*Variable
	arraySize, bindArraySize, fetchArraySize    int
	setInputSizes, outputSize, outputSizeColumn int
	rowCount, actualRows, rowNum                int
	statement                                   []byte
	statementTag                                []byte
	statementType                               int
	numbersAsStrings, isDML, isOpen, isOwned    bool
}

var DefaultArraySize int = 50
var (
	CursorIsClosed      = errors.New("cursor is closed")
	QueriesNotSupported = errors.New("queries not supported: results undefined")
)

//statement // statementTag // rowFactory // inputTypeHandler // outputTypeHandler

//   Allocate a new handle.
func (cur *Cursor) allocateHandle(typ C.ub4) *Error {
	cur.isOwned = true
	return ociHandleAlloc(unsafe.Pointer(cur.environment.handle),
		C.OCI_HTYPE_STMT,
		(*unsafe.Pointer)(unsafe.Pointer(&cur.handle)),
		"allocate statement handle")
}

//   Free the handle which may be reallocated if necessary.
func (cur *Cursor) freeHandle() *Error {
	if cur.handle == nil {
		return nil
	}
	if cur.isOwned {
		return cur.environment.CheckStatus(
			C.OCIHandleFree(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT),
			"freeCursor")
	} else if cur.connection.handle != nil {
		return cur.environment.CheckStatus(C.OCIStmtRelease(cur.handle,
			cur.environment.errorHandle, (*C.OraText)(&cur.statementTag[0]),
			C.ub4(len(cur.statementTag)), C.OCI_DEFAULT),
			"statement release")
	}
	cur.handle = nil
	return nil
}

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

func (cur *Cursor) IsOpen() bool {
	if !cur.isOpen {
		return false
	}
	return cur.connection.IsConnected()
}

// Creates new cursor
func NewCursor(conn *Connection) *Cursor {
	return &Cursor{connection: conn, environment: conn.environment,
		arraySize: DefaultArraySize, fetchArraySize: DefaultArraySize,
		bindArraySize: 1, statementType: -1, outputSize: -1, outputSizeColumn: -1,
		isOpen: true}
}

func (cur Cursor) String() string {
	return fmt.Sprintf("<goracle.Cursor on %s>", cur.connection)
}

//   Return a list of bind variable names. At this point the cursor must have
// already been prepared.
func (cur *Cursor) getBindNames() (names []string, err error) {
	// ensure that a statement has already been prepared
	if cur.statement == nil {
		err = errors.New("statement must be prepared first!")
		return
	}

	if names, err = cur.getBindInfo(8); err != nil {
		if men, ok := err.(mismatchElementNum); ok {
			names, err = cur.getBindInfo(int(men))
		}
	}

	return names, err
}

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

// Perform the defines for the cursor. At this point it is assumed that the
// statement being executed is in fact a query.
func (cur *Cursor) performDefine() error {
	var numParams int
	var x C.ub4 = 0

	// determine number of items in select-list
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle),
			C.OCI_HTYPE_STMT,
			unsafe.Pointer(&numParams), &x,
			C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
		"PerformDefine"); err != nil {
		return err
	}

	// create a list corresponding to the number of items
	cur.fetchVariables = make([]*Variable, numParams)

	// define a variable for each select-item
	cur.fetchArraySize = cur.arraySize
	for pos := 1; pos <= numParams; pos++ {
		// FIXME defineVariable
		// v, e := defineVariable(cur, cur.fetchArraySize, pos)
		var v *Variable
		var e error
		v, e = nil, nil
		if e != nil {
			return e
		}
		cur.fetchVariables[pos-1] = v
	}
	return nil
}

// Set the rowcount variable.
func (cur *Cursor) setRowCount() error {
	var rowCount, x C.ub4

	if cur.statementType == C.OCI_STMT_SELECT {
		cur.rowCount = 0
		cur.actualRows = -1
		cur.rowNum = 0
	} else if cur.statementType == C.OCI_STMT_INSERT ||
		cur.statementType == C.OCI_STMT_UPDATE ||
		cur.statementType == C.OCI_STMT_DELETE {
		if err := cur.environment.CheckStatus(
			C.OCIAttrGet(unsafe.Pointer(cur.handle),
				C.OCI_HTYPE_STMT, unsafe.Pointer(&rowCount), &x,
				C.OCI_ATTR_ROW_COUNT, cur.environment.errorHandle),
			"SetRowCount"); err != nil {
			return err
		}
		cur.rowCount = int(rowCount)
	} else {
		cur.rowCount = -1
	}

	return nil
}

// Get the error offset on the error object, if applicable.
func (cur *Cursor) getErrorOffset() int {
	var offset, x C.ub4
	C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		unsafe.Pointer(&offset), &x,
		C.OCI_ATTR_PARSE_ERROR_OFFSET, cur.environment.errorHandle)
	return int(offset)
}

func (cur *Cursor) setErrorOffset(err *Error) {
	err.Offset = cur.getErrorOffset()
}

// Perform the work of executing a cursor and set the rowcount appropriately
// regardless of whether an error takes place.
func (cur *Cursor) internalExecute(numIters int) error {
	var mode C.ub4

	if cur.connection.autocommit {
		mode = C.OCI_COMMIT_ON_SUCCESS
	} else {
		mode = C.OCI_DEFAULT
	}

	// Py_BEGIN_ALLOW_THREADS
	if err := cur.environment.CheckStatus(
		C.OCIStmtExecute(cur.connection.handle,
			cur.handle, cur.environment.errorHandle,
			C.ub4(numIters), 0, 0, 0, mode),
		"internalExecute"); err != nil {
		cur.setErrorOffset(err)
		return err
	}
	return cur.setRowCount()
}

// Determine if the cursor is executing a select statement.
func (cur *Cursor) getStatementType() error {
	var statementType C.ub2
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			&statementType, 0, C.OCI_ATTR_STMT_TYPE,
			cur.environment.errorHandle),
		"getStatementType"); err != nil {
		return err
	}
	cur.statementType = statementType
	if cur.fetchVariables != nil {
		cur.fetchVariables = nil
	}

	return nil
}

// Fixup a cursor so that fetching and returning cursor descriptions are
// successful after binding a cursor to another cursor.
func (cur *Cursor) fixupBoundCursor() error {
	if cur.handle != nil && cur.statementType < 0 {
		if err := cur.getStatementType(); err != nil {
			return err
		}
		if cur.statementType == C.OCI_STMT_SELECT {
			if err := cur.performDefine(); err != nil {
				return err
			}
		}
		if err := cur.setRowCount(self); err != nil {
			return err
		}
	}
	return nil
}

// Helper for Cursor_ItemDescription() used so that it is not necessary to
// constantly free the descriptor when an error takes place.
func (cur *Cursor) itemDescriptionHelper(pos uint, param *C.OCIParam) (desc VariableDescription, err error) {
	var (
		internalSize, charSize C.ub2
		variable               Variable
		displaySize, index     int
		nameLength             C.ub4
		precision              C.sb2
		// ub1 nullOk;
		// sb1 scale;
	)

	// acquire usable type of item
	if variable, err = cur.environment.variableByOracleDescriptor(param); err != nil {
		return
	}

	// acquire internal size of item
	if err = cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			&internalSize, 0,
			C.OCI_ATTR_DATA_SIZE, cur.environment.errorHandle),
		"itemDescription: internal size"); err != nil {
		return
	}

	// acquire character size of item
	if err = cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			&charSize, 0,
			C.OCI_ATTR_CHAR_SIZE, cur.environment.errorHandle),
		"itemDescription(): character size"); err != nil {
		return
	}

	// aquire name of item
	if err = cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			&name,
			&nameLength, C.OCI_ATTR_NAME, cur.environment.errorHandle),
		"itemDescription(): name"); err != nil {
		return
	}

	// lookup precision and scale
	if variable.IsNumber() {
		if err = cur.environment.CheckStatus(
			C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
				&scale, 0,
				C.OCI_ATTR_SCALE, cur.environment.errorHandle),
			"itemDescription(): scale"); err != nil {
			return
		}
		if err = cur.environment.CheckStatus(
			C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
				&precision, 0,
				C.OCI_ATTR_PRECISION, cur.environment.errorHandle),
			"itemDescription(): precision"); err != nil {
			return
		}
	}

	// lookup whether null is permitted for the attribute
	if err = cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			&nullOk, 0,
			C.OCI_ATTR_IS_NULL, cur.environment.errorHandle),
		"itemDescription(): nullable"); err != nil {
		return
	}

	// set display size based on data type
	switch {
	case variable.IsString():
		displaySie = charSize
	case variable.IsBinary():
		displaySize = internalSize
	// case variable.IsFixed():
	case variable.IsNumber():
		if precision > 0 {
			displaySize = precision + 1
			if scale > 0 {
				displaySize += scale + 1
			}
		} else {
			displaySize = 127
		}
	case variable.IsDate():
		displaySize = 23
	default:
		displaySize = -1
	}

	desc = VariableDescription{Name: cur.environment.FromEncodedString(name, nameLength),
		Type:        -1,                        //FIXME
		DisplaySize: displaySize, InternalSize: internalSize,
		Precision: precision, Scale: scale, NullOk: nullOk != 0,
	}
	return
}

// Return a tuple describing the item at the given position.
func (cur *Cursor) itemDescription(pos uint) (VariableDescription, error) {
	var param *C.OCIParam

	// acquire parameter descriptor
	if err = cur.environment.CheckStatus(
		C.OCIParamGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			cur.environment.errorHandle,
			(*unsafe.Pointer)(unsafe.Pointer(&param)), pos),
		"itemDescription(): parameter"); err != nil {
		return
	}

	// use helper routine to get tuple
	desc, e := cur.itemDescriptionHelper(pos, param)
	err = e
	C.OCIDescriptorFree(param, C.OCI_DTYPE_PARAM)
	return
}

// Return a list of 7-tuples consisting of the description of the define
// variables.
func (cur *Cursor) GetDescription() (descs []VariableDescription, err error) {
	var numItems int

	// make sure the cursor is open
	if !cur.IsOpen() {
		err = ClosedCursor
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
	if err = cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(handle), C.OCI_HTYPE_STMT,
			&numItems, 0,
			C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
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
	// make sure we are actually open
	if !cur.IsOpen() {
		return
	}
	// close the cursor
	cur.freeHandle() // no error checking?

	cur.isOpen = false
}

// Helper for setting a bind variable.
func (cur *Cursor) setBindVariableHelper(numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool, // defer type assignment if null?
	value interface{}, // value to bind
	origVar *Variable, // original variable bound
) (newVar *Variable, err error) {
	var isValueVar bool

	// initialization
	newVar = nil
	isValueVar = isVariable(value) //FIXME

	// handle case where variable is already bound
	if origVar != nil {

		// if the value is a variable object, rebind it if necessary
		if isValueVar {
			if origVar != value {
				newVar = value.(Variable)
			}

			// if the number of elements has changed, create a new variable
			// this is only necessary for executemany() since execute() always
			// passes a value of 1 for the number of elements
		} else if numElements > origVar.allocatedElements {
			if newVar, err = NewVariable(cur, numElements, origVar.typ,
				origVar.size); err != nil {
				return
			}
			if err = newVar.setValue(arrayPos, value); err != nil {
				return
			}

			// otherwise, attempt to set the value
		} else if origVar.setValue(arrayPos, value); err != nil {

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
			return err

			// clear the exception and try to create a new variable
			origVar = nil
		}

	}

	// if no original variable used, create a new one
	if origVar == nil {

		// if the value is a variable object, bind it directly
		if isValueVar {
			newVar = value.(*Variable)
			newVar.boundPos = 0
			newVar.boundName = ""

			// otherwise, create a new variable, unless the value is None and
			// we wish to defer type assignment
		} else if value != nil || !deferTypeAssignment {
			if newVar, err = NewVariableByValue(value, numElements); err != nil {
				return
			}
			if err = newVar.setValue(arrayPos, value); err != nil {
				return
			}
		}

	}

	return
}

// Create or set bind variables by position.
func (cur *Cursor) setBindVariablesByPos(parameters []interface{}, // parameters to bind
	numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool) ( // defer type assignment if null?
	err error) {
	var origBoundByPos, origNumParams, boundByPos, numParams int
	// PyObject *key, *value, *origVar;
	newVar * Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return EmptyList
	}
	if cur.bindVarsArr != nil {
		origNumParams = len(cur.bindVarsArr)
	} else {
		cur.bindVarsArr = make([]interface{}, len(parameters))
	}

	// handle positional binds
	for i, v := range parameters {
		if i < origNumParams {
			origVar = cur.bindVarsArr[i]
		} else {
			origVar = nil
		}
		if newVar, err = cur.setBindVariableHelper(numElements, arrayPos, deferTypeAssignment, v, origVar); err != nil {
			return err
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

// Create or set bind variables by position.
func (cur *Cursor) setBindVariablesByName(parameters map[string]interface{}, // parameters to bind
	numElements, // number of elements to create
	arrayPos uint, // array position to set
	deferTypeAssignment bool, // defer type assignment if null?
) (err error) {
	var origBoundByPos, origNumParams, boundByPos, numParams int
	// PyObject *key, *value, *origVar;
	newVar * Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return EmptyList
	}
	if cur.bindVarsMap != nil {
		origNumParams = len(cur.bindVarsMap)
	} else {
		cur.bindVarsMap = make(map[string]interface{}, len(parameters))
	}

	// handle named binds
	for k, v := range parameters {
		origVar, _ = cur.bindVarsMap[k]
		if newVar, err = cur.setBindVariableHelper(numElements, arrayPos, deferTypeAssignment,
			v, origVar); err != nil {
			return err
		}
		if newVar != nil {
			cur.bindVarsMap[k] = newVar
		}
	}

	return
}

// Perform the binds on the cursor.
func (cur *Cursor) performBind() error {
	// PyObject *key, *var;
	// Py_ssize_t pos;
	// ub2 i;

	// ensure that input sizes are reset
	// this is done before binding is attempted so that if binding fails and
	// a new statement is prepared, the bind variables will be reset and
	// spurious errors will not occur
	cur.setInputSizes = 0

	// set values and perform binds for all bind variables
	if cur.bindVarsMap != nil {
		for k, v := range cur.bindVarsMap {
			if err = v.bind(cur, k, 0); err != nil {
				return err
			}
		}
	} else if cur.bindVarsArr != nil {
		for i, v := range cur.bindVarsArr {
			if err = v.bind(cur, "", i+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// Create an object for the row. The object created is a tuple unless a row
// factory function has been defined in which case it is the result of the
// row factory function called with the argument tuple that would otherwise be
// returned.
func (cur *Cursor) createRow() ([]interface{}, error) {
	// create a new tuple
	numItems := len(cur.fetchVariables)
	row := make([]interface{}, numItems)

	// acquire the value for each item
	for pos, v := range cur.fetchVariables {
		row[pos] = v.getValue(cur.rowNum)
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

// Internal method for preparing a statement for execution.
func (cur *Cursor) internalPrepare(statement string, statementTag string) error {
	// make sure we don't get a situation where nothing is to be executed
	if statement == "" && cur.statement == nil {
		return ProgrammingError("no statement specified and no prior statement prepared")
	}

	// nothing to do if the statement is identical to the one already stored
	// but go ahead and prepare anyway for create, alter and drop statments
	if statement == "" || statement == string(cur.statement) {
		if cur.statementType != c.OCI_STMT_CREATE &&
			cur.statementType != C.OCI_STMT_DROP &&
			cur.statementType != C.OCI_STMT_ALTER {
			return nil
		}
		statement = string(cur.statement)
	}

	// keep track of the statement
	cur.statement = []byte(statement)

	// release existing statement, if necessary
	cur.statementTag = []byte(statementTag)
	if err := cur.freeHandle(); err != nil {
		return err
	}

	// prepare statement
	cur.isOwned = false
	// Py_BEGIN_ALLOW_THREADS
	if err := cur.environment.CheckStatus(
		C.OCIStmtPrepare2(cur.connection.handle, &cur.handle,
			cur.environment.errorHandle,
			(*C.text)(unsafe.Pointer(&cur.statement[0])), len(cur.statement),
			(*C.text)(unsafe.Pointer(&cur.statementTag[0])), len(cur.statementTag),
			C.OCI_NTV_SYNTAX, C.OCI_DEFAULT),
		"internalPrepare"); err != nil {
		// Py_END_ALLOW_THREADS
		// this is needed to avoid "invalid handle" errors since Oracle doesn't
		// seem to leave the pointer alone when an error is raised but the
		// resulting handle is still invalid
		cur.handle = nil
		return err
	}

	// clear bind variables, if applicable
	if cur.setInputSizes == nil {
		cur.bindVariables = nil
	}

	// clear row factory, if applicable
	// cur.rowFactory = nil

	// determine if statement is a query
	if _, err := cur.getStatementType(); err != nil {
		return err
	}

	return nil
}

// Parse the statement without executing it. This also retrieves information
// about the select list for select statements.
func (cur *Cursor) parse(statement string) error {
	var mode C.ub4

	// statement text is expected
	if statement == "" {
		return nil
	}

	// make sure the cursor is open
	if !cur.isOpen() {
		return nil
	}

	// prepare the statement
	if err := cur.internalPrepare(statement); err != nil {
		return err
	}

	// parse the statement
	if cur.statementType == C.OCI_STMT_SELECT {
		mode = C.OCI_DESCRIBE_ONLY
	} else {
		mode = C.OCI_PARSE_ONLY
	}
	// Py_BEGIN_ALLOW_THREADS
	if err := cur.environment.CheckStatus(
		C.OCIStmtExecute(cur.connection.handle, cur.handle,
			cur.environment.errorHandle, 0, 0, 0, 0, mode),
		"parse"); err != nil {
		// Py_END_ALLOW_THREADS
		return err
	}

	return nil
}

// Prepare the statement for execution. statementTag is optional
func (cur *Cursor) Prepare(statement, statemenetTag string) error {
	// make sure the cursor is open
	if !cur.isOpen() {
		return nil
	}

	// prepare the statement
	if err := cur.internalPrepare(statement, statementTag); err != nil {
		return err
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
			return nil, EmptyArgumentList
		}
		size += len(listOfArguments) * 9
	}

	// assume up to 15 characters for each keyword argument
	// this allows up to four digits for the placeholder if the bind variable
	// is a boolean value
	if keywordArguments != nil {
		if len(keywordArguments) == 0 {
			return nil, EmptyArgumentList
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
	chunks := make([]string, 1, 32)
	chunks[0] = "begin "
	if returnValue != nil {
		chunks = append(chunks, ":1 := ")
		// insert the return variable
		bindVarsArr[0] = returnValue
		argNum++
	}
	chunks = append(chunks, name, "(")

	// include any positional arguments first
	argchunks := make([]string, allArgNum-(argNum-1))
	if listOfArguments != nil && len(listOfArguments) > 0 {
		plus := ""
		for i, arg := range listOfArguments {
			if _, ok := arg.(bool); ok {
				plus = " = 1"
			} else {
				plus = ""
			}
			argchunks[argNum-1] = ":" + strconv.Itoa(argNum) + plus
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
			argchunks[argNum-1] = key + "=>:" + strconv.Itoa(argNum) + plus
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
	if !cur.isOpen() {
		return CursorIsClosed
	}

	// determine the statement size
	statementSize, err := cur.callCalculateSize(name, returnValue, listOfArguments,
		keywordArguments)
	if err != nil {
		return err
	}

	// determine the statement to execute and the argument to pass
	statement, bindVarArrs, e := cur.callBuildStatement(name, returnValue, listOfArguments,
		keywordArguments)
	if e != nil {
		return e
	}

	// execute the statement on the cursor
	return cur.execute(statement, bindVarArrs)
}

// Call a stored function and return the return value of the function.
func (cur *Cursor) CallFunc(
	name string,
	returnType VariableType,
	parameters []interface{},
	keywordParameters map[string]interface{}) (interface{}, error) {
	var variable *Variable

	// create the return variable
	variable = NewVariableByType(cur, returnType)

	// call the function
	if err := cur.call(variable, name, parameters, keywordParameters); err != nil {
		return err
	}

	// determine the results
	return variable.GetValue(0)
}

// Call a stored procedure and return the (possibly modified) arguments.
func (cur *Cursor) CallProc(name string,
	parameters []interface{}, keywordParamenters map[string]interface{}) (
	results []interface{}, err error) {
	// call the stored procedure
	if err = cur.call(0, name, parameters, keywordParameters); err != nil {
		return err
	}

	// create the return value
	numArgs := len(cur.bindVarsArr) + len(cur.bindVarsMap)
	results = make([]interface{}, numArgs)
	var val interface{}
	i := 0
	for _, v := range cur.bindVarsArr {
		if val, err = v.GetValue(0); err != nil {
			return err
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
func (cur *Cursor) Execute(statement string,
	listArgs []interface{}, keywordArgs map[string]interface{}) error {

	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	var err error
	// prepare the statement, if applicable
	if err = cur.internalPrepare(statement); err != nil {
		return
	}

	// perform binds
	if listArgs != nil && len(listArgs) > 0 {
		if err = cur.setBindVariablesByPos(listArgs, 1, 0, false); err != nil {
			return err
		}
	} else if keywordArgs != nil && len(keywordArgs) > 0 {
		if err = cur.setBindVariablesByName(keywordArgs, 1, 0, false); err != nil {
			return err
		}
	}
	if err = cur.performBind(); err != nil {
		return err
	}

	// execute the statement
	isQuery := cur.statementType == C.OCI_STMT_SELECT
	if err = cur.internalExecute(isQuery); err != nil {
		return err
	}

	// perform defines, if necessary
	if isQuery && cur.fetchVariables == nil {
		if err = cur.performDefine(); err != nil {
			return err
		}
	}

	// reset the values of setoutputsize()
	cur.outputSize = -1
	cur.outputSizeColumn = -1

	return nil
}

// Execute the statement many times. The number of times is equivalent to the
// number of elements in the array of dictionaries.
func (cur *Cursor) ExecuteMany(statement, params []map[string]interface{}) error {
	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	var err error
	// prepare the statement
	if err = cur.internalPrepare(statement); err != nil {
		return err
	}

	// queries are not supported as the result is undefined
	if cur.statementType == C.OCI_STMT_SELECT {
		return QueriesNotSupported
	}

	// perform binds
	numRows := len(params)
	for i, arguments := range params {
		if err = cur.setBindVariablesByName(arguments, numRows, i,
			(i < numRows-1)); err != nil {
			return err
		}
	}
	if err = cur.performBind(); err != nil {
		return err
	}

	// execute the statement, but only if the number of rows is greater than
	// zero since Oracle raises an error otherwise
	if numRows > 0 {
		if err = cur.internalExecute(numRows); err != nil {
			return err
		}
	}

	return nil
}

//-----------------------------------------------------------------------------
// Execute the prepared statement the number of times requested. At this
// point, the statement must have been already prepared and the bind variables
// must have their values set.
func (cur *Cursor) ExecuteManyPrepared(numIters int) error {
	if numIters > cur.bindArraySize {
		return fmt.Errorf("iterations exceed bind array size")
	}

	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	// queries are not supported as the result is undefined
	if cur.statementType == C.OCI_STMT_SELECT {
		return QueriesNotSupported
	}

	var err error
	// perform binds
	if err = cur.performBind(self); err != nil {
		return err
	}

	// execute the statement
	return cur.internalExecute(self, numIters)
}

// Verify that fetching may happen from this cursor.
func (cur *Cursor) verifyFetch() error {
	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	// fixup bound cursor, if necessary
	if err := cur.fixupBoundCursor(); err != nil {
		return err
	}

	// make sure the cursor is for a query
	if cur.statementType != C.OCI_STMT_SELECT {
		return errors.New("not a query")
	}

	return nil
}

// Performs the actual fetch from Oracle.
func (cur *Cursor) internalFetch(numRows int) error {
	if cur.fetchVariables == nil {
		return errors.New("query not executed")
	}
	for _, v := range cur.fetchVariables {
		v.internalFetchNum++
		if v.typ.preFetchProc != nil {
			if err := v.typ.preFetchProc(); err != nil {
				return err
			}
		}
	}
	// Py_BEGIN_ALLOW_THREADS
	if err := cur.environment.CheckStatus(
		C.OCIStmtFetch(unsafe.Pointer(cur.handle), cur.environment.errorHandle,
			numRows, C.OCI_FETCH_NEXT, C.OCI_DEFAULT),
		"internalFetch(): fetch"); err != nil {
		return err
	}
	var rowCount int
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT, &rowCount, 0,
			C.OCI_ATTR_ROW_COUNT, cur.environment.errorHandle),
		"internalFetch(): row count"); err != nil {
		return err
	}
	cur.actualRows = rowCount - cur.rowCount
	cur.rowNum = 0
	return nil
}

// Returns an integer indicating if more rows can be retrieved from the
// cursor.
func (cur *Cursor) moreRows() (bool, error) {
	if cur.rowNum >= cur.actualRows {
		if cur.actualRows < 0 || cur.actualRows == cur.fetchArraySize {
			if err := cur.internalFetch(self, cur.fetchArraySize); err != nil {
				return false, err
			}
		}
		if cur.rowNum >= cur.actualRows {
			return false
		}
	}
	return true
}

// Return a list consisting of the remaining rows up to the given row limit
// (if specified).
func (cur *Cursor) multiFetch(rowLimit int) (results [][]interface{}, err error) {
	// create an empty list
	results = make([]interface{}, 0, 2)

	// fetch as many rows as possible
	for rowNum := 0; rowLimit == 0 || rowNum < rowLimit; rowNum++ {
		if ok, err := cur.moreRows(); err != nil {
			return
		} else if !ok {
			break
		}
		row = cur.createRow()
		results = append(results, row)
	}

	return results
}

// Fetch a single row from the cursor.
func (cur *Cursor) FetchOne() error {
	// verify fetch can be performed
	if err := cur.verifyFetch(); err != nil {
		return err
	}

	// setup return value
	if ok, err := cur.moreRows(); err != nil {
		return err
	} else if ok {
		return cur.createRow()
	}

	return nil
}

// Fetch multiple rows from the cursor based on the arraysize.
// for default (arraySize) row limit, use negative rowLimit
func (cur *Cursor) FetchMany(numRows, rowLimit int) error {
	// parse arguments -- optional rowlimit expected
	if rowLimit < 0 {
		rowLimit = cur.arraySize
	}

	// verify fetch can be performed
	if err := cur.verifyFetch(); err != nil {
		return err
	}

	return cur.mutliFetch(rowLimit)
}

// Fetch all remaining rows from the cursor.
func (cur *Cursor) fetchAll() error {
	if err := cur.verifyFetch(); err != nil {
		return err
	}
	return cur.multiFetch(0)
}

// Perform raw fetch on the cursor; return the actual number of rows fetched.
func (cur *Cursor) fetcRaw(numRows int) (int, error) {
	if numRows > cur.fetchArraySize {
		return nil, errors.New("rows to fetch exceeds array size")
	}

	// do not attempt to perform fetch if no more rows to fetch
	if 0 < cur.actualRows && cur.actualRows < cur.fetchArraySize {
		return 0, nil
	}

	// perform internal fetch
	if err := cur.internalFetch(numRows); err != nil {
		return 0, err
	}

	cur.rowCount += cur.actualRows
	numRowsFetched := cur.actualRows
	if cur.actualRows == numRows {
		cur.actualRows = -1
	}
	return numRowsFetched
}

// Set the sizes of the bind variables by position (array).
func (cur *Cursor) SetInputSizesByPos(types []VariableType) error {
	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	// eliminate existing bind variables
	if cur.bindVarsArr == nil {
		cur.bindVarsArr = make([]*Variables, 0, len(types))
	} else {
		cur.bindVarsArr = cur.bindVarsArr[:0]
	}
	cur.setInputSizes = 1

	// process each input
	for _, t := range types {
		cur.bindVarsArr = append(cur.bindVarsArr, t.NewVariable(cur))
	}
	return nil
}

// Set the sizes of the bind variables by name (map).
func (cur *Cursor) SetInputSizesByName(types map[string]VariableType) error {
	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
	}

	// eliminate existing bind variables
	if cur.bindVarsMap == nil {
		cur.bindVarsMap = make(map[string]*Variables, 0, len(types))
	} else {
		for k, _ := range cur.bindVarsMap {
			delete(cur.bindVarsMap, k)
		}
	}
	cur.setInputSizes = 1

	// process each input
	for k, t := range types {
		cur.bindVarsMap[k] = t.NewVariable(cur)
	}
	return nil
}

// Set the size of all of the long columns or just one of them.
// use -1 for outputSizeColumn if set outputSize for all columns!
func (cur *Cursor) SetOutputSize(outputSize, outputSizeColumn int) {
	cur.outputSize = outputSize
	cur.outputSizeColumn = outputSizeColumn
}

// Create a bind variable and return it.
func (cur *Cursor) NewVar(varType VariableType, size int, arraySize int /*inconverter, outconverter, typename*/) *Variable {
	// determine the type of variable
	// varType = Variable_TypeByPythonType(self, type);
	if varType.variableLength && size == 0 {
		size = varType.size
	}
	/*
	   if (type == (PyObject*) &g_ObjectVarType && !typeNameObj) {
	       PyErr_SetString(PyExc_TypeError,
	               "expecting type name for object variables");
	       return NULL;
	   }
	*/

	// create the variable
	v := NewVariable(cur, arraySize, varType, size)
	/*
	   var->inConverter = inConverter;
	   var->outConverter = outConverter;
	*/

	// define the object type if needed
	/*
	   if (type == (PyObject*) &g_ObjectVarType) {
	       objectVar = (udt_ObjectVar*) var;
	       objectVar->objectType = ObjectType_NewByName(self->connection,
	               typeNameObj);
	       if (!objectVar->objectType) {
	           Py_DECREF(var);
	           return NULL;
	       }
	   }
	*/

	return v
}

// Create an array bind variable and return it.
func (cur *Cursor) ArrayVar(varType *VariableType, values []interface{}, size int) (*Variable, error) {
	if varType.variableLength && size == 0 {
		size = varType.size
	}

	// determine the number of elements to create
	numElements := len(value)

	// create the variable
	v := NewVariable(cur, numElements, varType, size)
	if err := v.MakeArray(); err != nil {
		return nil, err
	}

	// set the value, if applicable
	if err := v.SetArrayValue(value); err != nil {
		return nil, err
	}

	return v, nil
}

// Return a list of bind variable names.
func (cur *Cursor) bindNames() ([]string, error) {
	// make sure the cursor is open
	if !cur.isOpen() {
		return CursorIsClosed
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

// Return a reference to the cursor which supports the iterator protocol.
func (cur *Cursor) getNext() error {
	if err := cur.verifyFetch(); err != nil {
		return err
	}
	if more, err := cur.moreRows(); err != nil {
		return err
	} else if more {
		return cur.createRow()
	}
	return nil, io.EOF
}
