package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>

const int sizeof_OraText = sizeof(OraText);
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	// "reflect"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// func init() {
// debug("bindInfo_elementSize=%d", C.bindInfo_elementSize)
// }

type Cursor struct {
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

var DefaultArraySize uint = 50
var (
	CursorIsClosed      = errors.New("cursor is closed")
	QueriesNotSupported = errors.New("queries not supported: results undefined")
	ListIsEmpty         = errors.New("list is empty")
)

//statement // statementTag // rowFactory // inputTypeHandler // outputTypeHandler

//   Allocate a new handle.
func (cur *Cursor) allocateHandle() error {
	cur.isOwned = true
	return ociHandleAlloc(unsafe.Pointer(cur.environment.handle),
		C.OCI_HTYPE_STMT,
		(*unsafe.Pointer)(unsafe.Pointer(&cur.handle)),
		"allocate statement handle")
}

//   Free the handle which may be reallocated if necessary.
func (cur *Cursor) freeHandle() error {
	if cur.handle == nil {
		return nil
	}
	// debug("freeing cursor handle %v", cur.handle)
	if cur.isOwned {
		if CTrace {
			ctrace("OCIHandleFree", cur.handle, "htype_stmt")
		}
		return cur.environment.CheckStatus(
			C.OCIHandleFree(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT),
			"freeCursor")
	} else if cur.connection.handle != nil &&
		cur.statementTag != nil && len(cur.statementTag) > 0 {
		if CTrace {
			ctrace("OCIStmtRelease", cur.handle, cur.environment.errorHandle,
				cur.statementTag, len(cur.statementTag), "OCI_DEFAULT")
		}
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

// Perform the defines for the cursor. At this point it is assumed that the
// statement being executed is in fact a query.
func (cur *Cursor) performDefine() error {
	var numParams uint
	var x C.ub4 = 0

	// determine number of items in select-list
	if CTrace {
		ctrace("OCIAttrGet", cur.handle, "HTYPE_STMT", &numParams, &x,
			"PARAM_COUNT", cur.environment.errorHandle)
	}
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle),
			C.OCI_HTYPE_STMT,
			unsafe.Pointer(&numParams), &x,
			C.OCI_ATTR_PARAM_COUNT, cur.environment.errorHandle),
		"PerformDefine"); err != nil {
		return err
	}
	// debug("performDefine param count = %d", numParams)

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
			return fmt.Errorf("error defining variable %d: %s", pos, e)
		}
		if v == nil {
			return fmt.Errorf("empty variable on pos %d!", pos)
		}
		// debug("var %d=%v", pos, v)
		cur.fetchVariables[pos-1] = v
	}
	// debug("len(cur.fetchVariables)=%d", len(cur.fetchVariables))
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
		if CTrace {
			ctrace("OCIAttrGet", cur.handle, "HTYPE_STMT", &rowCount, &x,
				"ATTR_ROW_COUNT", cur.environment.errorHandle)
		}
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

// returns the rowcount of the statement (0 for select, rows affected for DML)
func (cur Cursor) GetRowCount() int {
	return cur.rowCount
}

// // returns the bind variables array and map
// func (cur Cursor) GetBindVars() ([]*Variable, map[string]*Variable) {
// 	return cur.bindVarsArr, cur.bindVarsMap
// }

// Get the error offset on the error object, if applicable.
func (cur *Cursor) getErrorOffset() int {
	var offset, x C.ub4
	if CTrace {
		ctrace("OCIAttrGet", cur.handle, "HTYPE_STMT", &offset, &x,
			"ATTR_PARSE_ERROR_OFFSET", cur.environment.errorHandle)
	}
	C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
		unsafe.Pointer(&offset), &x,
		C.OCI_ATTR_PARSE_ERROR_OFFSET, cur.environment.errorHandle)
	return int(offset)
}

func (cur *Cursor) setErrorOffset(err error) {
	if x, ok := err.(*Error); ok {
		x.Offset = cur.getErrorOffset()
	}
}

// Perform the work of executing a cursor and set the rowcount appropriately
// regardless of whether an error takes place.
func (cur *Cursor) internalExecute(numIters uint) error {
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
		ctrace("OCIStmtExecute", cur.connection.handle, cur.handle,
			cur.environment.errorHandle, numIters, 0, nil, nil, mode)
	}
	if err := cur.environment.CheckStatus(
		C.OCIStmtExecute(cur.connection.handle,
			cur.handle, cur.environment.errorHandle,
			C.ub4(numIters), 0, // iters, rowOff
			nil, nil, // snapIn, snapOut
			mode),
		"internalExecute"); err != nil {
		cur.setErrorOffset(err)
		return err
	}
	return cur.setRowCount()
}

// Determine if the cursor is executing a select statement.
func (cur *Cursor) getStatementType() error {
	var statementType C.ub2
	var vsize C.ub4
	if CTrace {
		ctrace("OCIAttrGet", cur.handle, "HTYPE_STMT", &statementType, &vsize,
			"ATTR_STMT_TYPE", cur.environment.errorHandle)
	}
	if err := cur.environment.CheckStatus(
		C.OCIAttrGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			unsafe.Pointer(&statementType), &vsize, C.OCI_ATTR_STMT_TYPE,
			cur.environment.errorHandle),
		"getStatementType"); err != nil {
		return err
	}
	cur.statementType = int(statementType)
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
		if err := cur.setRowCount(); err != nil {
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
		return
	}
	logg("varType=%s", varType)

	// acquire internal size of item
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_DATA_SIZE, unsafe.Pointer(&internalSize),
		"itemDescription: internal size"); err != nil {
		return
	}
	logg("internalSize=%d", internalSize)

	// acquire character size of item
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_CHAR_SIZE, unsafe.Pointer(&charSize),
		"itemDescription(): character size"); err != nil {
		return
	}
	logg("charSize=%d", charSize)

	var name []byte
	// aquire name of item
	if name, err = cur.environment.AttrGetName(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_NAME, "itemDescription(): name"); err != nil {
		return
	}
	logg("name=%s", name)

	// lookup precision and scale
	if varType.IsNumber() {
		if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_SCALE, unsafe.Pointer(&scale),
			"itemDescription(): scale"); err != nil {
			return
		}
		logg("scale=%d", scale)
		if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_PRECISION, unsafe.Pointer(&precision),
			"itemDescription(): precision"); err != nil {
			return
		}
		logg("precision=%d", precision)
	}

	// lookup whether null is permitted for the attribute
	if _, err = cur.environment.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_IS_NULL, unsafe.Pointer(&nullOk),
		"itemDescription(): nullable"); err != nil {
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

// Return a tuple describing the item at the given position.
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

// Return a list of 7-tuples consisting of the description of the define
// variables.
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
	// make sure we are actually open
	if !cur.isOpen {
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
	// debug("origVar=%s value=%s (%T)", origVar, value, value)
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
			debug("newVar=%v %T", newVar, newVar.typ.Name)
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
	var origNumParams int
	// PyObject *key, *value, *origVar;
	var origVar, newVar *Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return ListIsEmpty
	}
	if cur.bindVarsArr != nil {
		origNumParams = len(cur.bindVarsArr)
	} else {
		cur.bindVarsArr = make([]*Variable, len(parameters))
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
	// PyObject *key, *value, *origVar;
	var origVar, newVar *Variable // udt_Variable *newVar;

	// make sure positional and named binds are not being intermixed
	if parameters == nil || len(parameters) <= 0 {
		return ListIsEmpty
	}
	if cur.bindVarsMap == nil || len(cur.bindVarsMap) > 0 {
		cur.bindVarsMap = make(map[string]*Variable, len(parameters))
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
func (cur *Cursor) performBind() (err error) {
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
			if err = v.Bind(cur, k, 0); err != nil {
				return err
			}
		}
	} else if cur.bindVarsArr != nil {
		for i, v := range cur.bindVarsArr {
			if err = v.Bind(cur, "", uint(i+1)); err != nil {
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
	var err error
	// create a new tuple
	numItems := len(cur.fetchVariables)
	row := make([]interface{}, numItems)

	// acquire the value for each item
	for pos, v := range cur.fetchVariables {
		if row[pos], err = v.GetValue(uint(cur.rowNum)); err != nil {
			return nil, err
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

// Fetch current row's columns into the given pointers
func (cur *Cursor) fetchInto(row ...interface{}) error {
	var err error
	// create a new tuple
	numItems := len(cur.fetchVariables)
	if numItems != len(row) {
		return fmt.Errorf("colnum mismatch: got %d, have %d", len(row), numItems)
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
			return err
		}
		// row[pos] = *x
	}

	// increment row counters
	cur.rowNum++
	cur.rowCount++
	// debug("fetchInto rn=%d rc=%d row=%+v", cur.rowNum, cur.rowCount, row)

	return nil
}

func (cur *Cursor) IsDDL() bool {
	return cur.statementType == C.OCI_STMT_CREATE ||
		cur.statementType == C.OCI_STMT_DROP ||
		cur.statementType == C.OCI_STMT_ALTER
}

// Internal method for preparing a statement for execution.
func (cur *Cursor) internalPrepare(statement string, statementTag string) error {
	// make sure we don't get a situation where nothing is to be executed
	if statement == "" && cur.statement == nil {
		return ProgrammingError("no statement specified and no prior statement prepared")
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
		return err
	}

	// prepare statement
	cur.isOwned = false
	debug(`%p.Prepare2 for "%s" [%x]`, cur, cur.statement, cur.statementTag)
	// Py_BEGIN_ALLOW_THREADS
	if CTrace {
		ctrace("OCIStmtPrepare2", cur.connection.handle, &cur.handle, cur.environment.errorHandle,
			string(bytes.Replace(cur.statement, []byte{'\n'}, []byte("\\n"), -1)),
			len(cur.statement), cur.statementTag, len(cur.statementTag),
			"NTV_SYNTAX", "DEFAULT")
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
	// debug("prepared")

	// clear bind variables, if applicable
	if cur.setInputSizes < 0 {
		cur.bindVarsArr = nil
		cur.bindVarsMap = nil
	}

	// clear row factory, if applicable
	// cur.rowFactory = nil

	// determine if statement is a query
	if err := cur.getStatementType(); err != nil {
		return err
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
		return err
	}

	// parse the statement
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
		return err
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
		for _, arg := range listOfArguments {
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
			argchunks[argNum-1] = k + "=>:" + strconv.Itoa(argNum) + plus
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
		return err
	}

	// determine the statement to execute and the argument to pass
	statement, bindVarArrs, e := cur.callBuildStatement(name, returnValue, listOfArguments,
		keywordArguments)
	if e != nil {
		return e
	}

	// execute the statement on the cursor
	return cur.Execute(statement, bindVarArrs, nil)
}

// Call a stored function and return the return value of the function.
func (cur *Cursor) CallFunc(
	name string,
	returnType VariableType,
	parameters []interface{},
	keywordParameters map[string]interface{}) (interface{}, error) {

	// create the return variable
	variable, err := returnType.NewVariable(cur, 0, 0)
	if err != nil {
		return nil, err
	}

	// call the function
	if err = cur.call(variable, name, parameters, keywordParameters); err != nil {
		return nil, err
	}

	// determine the results
	return variable.GetValue(0)
}

// Call a stored procedure and return the (possibly modified) arguments.
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
func (cur *Cursor) Execute(statement string,
	listArgs []interface{}, keywordArgs map[string]interface{}) error {

	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	var err error
	// prepare the statement, if applicable
	if err = cur.internalPrepare(statement, ""); err != nil {
		return err
	}

	// debug("internalPrepare done, performing binds")
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
	// debug("bind done, executing statement")

	// execute the statement
	isQuery := cur.statementType == C.OCI_STMT_SELECT
	numIters := uint(1)
	if isQuery {
		numIters = 0
	}
	if err = cur.internalExecute(numIters); err != nil {
		return err
	}
	// debug("executed, calling performDefine")

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
func (cur *Cursor) ExecuteMany(statement string, params []map[string]interface{}) error {
	// make sure the cursor is open
	if !cur.isOpen {
		return CursorIsClosed
	}

	var err error
	// prepare the statement
	if err = cur.internalPrepare(statement, ""); err != nil {
		return err
	}

	// queries are not supported as the result is undefined
	if cur.statementType == C.OCI_STMT_SELECT {
		return QueriesNotSupported
	}

	// perform binds
	numRows := len(params)
	for i, arguments := range params {
		if err = cur.setBindVariablesByName(arguments, uint(numRows), uint(i),
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
		if err = cur.internalExecute(uint(numRows)); err != nil {
			return err
		}
	}

	return nil
}

// Execute the prepared statement the number of times requested. At this
// point, the statement must have been already prepared and the bind variables
// must have their values set.
func (cur *Cursor) ExecuteManyPrepared(numIters uint) error {
	if numIters > cur.bindArraySize {
		return fmt.Errorf("iterations exceed bind array size")
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
		return err
	}

	// execute the statement
	return cur.internalExecute(numIters)
}

// Verify that fetching may happen from this cursor.
func (cur *Cursor) verifyFetch() error {
	// make sure the cursor is open
	if !cur.isOpen {
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
func (cur *Cursor) internalFetch(numRows uint) error {
	if cur.fetchVariables == nil {
		return errors.New("query not executed")
	}
	// debug("fetchVars=%v", cur.fetchVariables)
	var err error
	for _, v := range cur.fetchVariables {
		// debug("fetchvar %d=%s", v.internalFetchNum, v)
		v.internalFetchNum++
		// debug("typ=%s", v.typ)
		// debug("preFetch=%s", v.typ.preFetch)
		if v.typ.preFetch != nil {
			if err = v.typ.preFetch(v); err != nil {
				return err
			}
		}
	}
	// debug("StmtFetch numRows=%d", numRows)
	// Py_BEGIN_ALLOW_THREADS
	if CTrace {
		ctrace("OCIStmtFetch", cur.handle, cur.environment.errorHandle,
			numRows, "FETCH_NEXT", "DEFAULT")
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
		return err
	}
	// debug("row count = %d", rowCount)
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
				return false, err
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

// Fetch a single row from the cursor.
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

// Fetch a single row from the cursor into the given column pointers
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

// Fetch multiple rows from the cursor based on the arraysize.
// for default (arraySize) row limit, use negative rowLimit
func (cur *Cursor) FetchMany(rowLimit int) ([][]interface{}, error) {
	// parse arguments -- optional rowlimit expected
	if rowLimit < 0 {
		rowLimit = int(cur.arraySize)
	}

	// verify fetch can be performed
	if err := cur.verifyFetch(); err != nil {
		return nil, err
	}

	return cur.multiFetch(rowLimit)
}

// Fetch all remaining rows from the cursor.
func (cur *Cursor) FetchAll() ([][]interface{}, error) {
	if err := cur.verifyFetch(); err != nil {
		return nil, err
	}
	return cur.multiFetch(0)
}

// Perform raw fetch on the cursor; return the actual number of rows fetched.
func (cur *Cursor) fetchRaw(numRows uint) (int, error) {
	if numRows > cur.fetchArraySize {
		return -1, errors.New("rows to fetch exceeds array size")
	}

	// do not attempt to perform fetch if no more rows to fetch
	if 0 < cur.actualRows && uint(cur.actualRows) < cur.fetchArraySize {
		return 0, nil
	}

	// perform internal fetch
	if err := cur.internalFetch(numRows); err != nil {
		return -1, err
	}

	cur.rowCount += cur.actualRows
	numRowsFetched := cur.actualRows
	if cur.actualRows == int(numRows) {
		cur.actualRows = -1
	}
	return numRowsFetched, nil
}

// Set the sizes of the bind variables by position (array).
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
			return err
		}
		cur.bindVarsArr = append(cur.bindVarsArr, nv)
	}
	return nil
}

// Set the sizes of the bind variables by name (map).
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
			return err
		}
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
func (cur *Cursor) NewVar(value interface{}, /*inconverter, outconverter, typename*/
) (v *Variable, err error) {
	// determine the type of variable
	// varType = Variable_TypeByPythonType(self, type);
	varType, size, numElements, err := VarTypeByValue(value)
	if err != nil {
		return nil, err
	}
	if varType.isVariableLength && size == 0 {
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
	v, err = cur.NewVariable(numElements, varType, size)
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

	return
}

// Create an array bind variable and return it.
func (cur *Cursor) NewArrayVar(varType *VariableType, values []interface{}, size uint) (v *Variable, err error) {
	if varType.isVariableLength && size == 0 {
		size = varType.size
	}

	// determine the number of elements to create
	numElements := len(values)

	// create the variable
	if v, err = cur.NewVariable(uint(numElements), varType, size); err != nil {
		return
	}
	if err = v.makeArray(); err != nil {
		return
	}

	// set the value, if applicable
	if err = v.setArrayValue(values); err != nil {
		return
	}

	return
}

// Return a list of bind variable names.
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

// Return a reference to the cursor which supports the iterator protocol.
func (cur *Cursor) getNext() error {
	if err := cur.verifyFetch(); err != nil {
		return err
	}
	if more, err := cur.moreRows(); err != nil {
		return err
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
