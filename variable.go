// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	// "unsafe"
	"errors"
)

var (
	NotImplemented = errors.New("not implemented")
	ArrayTooLarge  = errors.New("array too large")
)

type Variable struct {
	bindHandle        *C.OCIBind
	defineHandle      *C.OCIDefine
	boundCursorHandle *C.OCIStmt
	boundName         string
	//PyObject*inConverter;
	//PyObject*outConverter;
	typ                               *VariableType
	allocatedElements, actualElements int
	boundPos, internalFetchNum        int
	size, bufferSize                  int
	environment                       *Environment
	isArray, isAllocatedInternally    bool
	indicator                         []C.sb2
	returnCode, actualLength          []C.ub2
	data                              []byte
}

// allocate a new variable
func NewVariable(cur *Cursor, numElements uint, varType *VariableType, size int) (v *Variable, err error) {
	// perform basic initialization
	v.environment = cur.connection.environment
	if numElements < 1 {
		v.allocatedElements = 1
	} else {
		v.allocatedElements = int(numElements)
	}
	v.isAllocatedInternally = true
	v.typ = varType

	// set the maximum length of the variable, ensure that a minimum of
	// 2 bytes is allocated to ensure that the array size check works
	v.size = v.typ.size
	if v.typ.isVariableLength {
		if size < 2 {
			size = 2
		}
		v.size = size
	}

	// allocate the data for the variable
	if err = v.allocateData(); err != nil {
		return
	}

	// allocate the indicator for the variable
	v.indicator = make([]C.sb2, v.allocatedElements) //sizeof(sb2)

	// for variable length data, also allocate the return code
	if v.typ.isVariableLength {
		v.returnCode = make([]C.ub2, v.allocatedElements)
	}

	// perform extended initialization
	if v.typ.initialize != nil {
		v.typ.initialize(v, cur)
	}

	return v, nil
}

type VariableDescription struct {
	Name                                              string
	Type, InternalSize, DisplaySize, Precision, Scale int
	NullOk                                            bool
}

type VariableType struct {
	Id                        byte
	isVariableLength          bool
	size                      int
	canBeInArray, canBeCopied bool
	initialize                func(*Variable, *Cursor) error
	finalize                  func(*Variable) error
	preDefine                 func(*Variable, *C.OCIParam) error
	postDefine                func(*Variable) error
	isNull                    func(*Variable, uint) bool
	getValue                  func(*Variable, uint) (interface{}, error)
	setValue                  func(*Variable, uint, interface{}) (interface{}, error)
	preFetch                  func(*Variable) error
	getBufferSize             func() int
}

//   Returns a boolean indicating if the object is a variable.
func isVariable(value interface{}) bool {
	//FIXME
	if _, ok := value.(Variable); ok {
		return true
	}
	if _, ok := value.(*Variable); ok {
		return true
	}
	return false
	/*
		    return (Py_TYPE(object) == &g_CursorVarType ||
		            Py_TYPE(object) == &g_DateTimeVarType ||
		            Py_TYPE(object) == &g_BFILEVarType ||
		            Py_TYPE(object) == &g_BLOBVarType ||
		            Py_TYPE(object) == &g_CLOBVarType ||
		            Py_TYPE(object) == &g_LongStringVarType ||
		            Py_TYPE(object) == &g_LongBinaryVarType ||
		            Py_TYPE(object) == &g_NumberVarType ||
		            Py_TYPE(object) == &g_StringVarType ||
		            Py_TYPE(object) == &g_FixedCharVarType ||
		            Py_TYPE(object) == &g_NCLOBVarType ||
		#if PY_MAJOR_VERSION < 3
		            Py_TYPE(object) == &g_UnicodeVarType ||
		            Py_TYPE(object) == &g_FixedUnicodeVarType ||
		#endif
		            Py_TYPE(object) == &g_RowidVarType ||
		            Py_TYPE(object) == &g_BinaryVarType ||
		            Py_TYPE(object) == &g_TimestampVarType ||
		            Py_TYPE(object) == &g_IntervalVarType
		#ifdef SQLT_BFLOAT
		            || Py_TYPE(object) == &g_NativeFloatVarType
		#endif
		            );
	*/
}

func (t VariableType) IsNumber() bool {
	return false
}
func (t VariableType) IsBinary() bool {
	return false
}
func (t VariableType) IsString() bool {
	return false
}
func (t VariableType) IsDate() bool {
	return false
}

func (t *VariableType) NewVariable(cur *Cursor, numElements uint, size int) (*Variable, error) {
	return NewVariable(cur, numElements, t, size)
}

func (env *Environment) varTypeByOracleDescriptor(param *C.OCIParam) (*VariableType, error) {
	return nil, nil
}

// Allocate the data for the variable.
func (v *Variable) allocateData() error {
	// set the buffer size for the variable
	if v.typ.getBufferSize != nil {
		v.bufferSize = v.typ.getBufferSize()
	} else {
		v.bufferSize = v.size
	}

	// allocate the data as long as it is small enough
	dataLength := v.allocatedElements * v.bufferSize
	if dataLength > 1<<31-1 {
		return ArrayTooLarge
	}
	v.data = make([]byte, dataLength)

	return nil
}

// Free an existing variable.
func (v *Variable) Free() {
	if v.isAllocatedInternally {
		if v.typ.finalize != nil {
			v.typ.finalize(v)
		}
		v.indicator = nil
		v.data = nil
		v.actualLength = nil
		v.returnCode = nil
	}
	v.environment = nil
	v.boundName = ""
	// Py_CLEAR(self->inConverter);
	// Py_CLEAR(self->outConverter);
}

// Resize the variable.
func (v *Variable) resize(size uint) error {
	// allocate the data for the new array
	nsize := v.allocatedElements * int(size)
	if len(v.data) == nsize {
		return nil
	}
	v.bufferSize = int(size)
	if len(v.data) < nsize {
		v.data = v.data[:nsize]
	} else {
		v.data = append(v.data, make([]byte, nsize-len(v.data))...)
	}

	// force rebinding
	if v.boundName != "" || v.boundPos > 0 {
		return v.internalBind()
	}

	return nil
}

/*
//-----------------------------------------------------------------------------
// Variable_TypeByPythonType()
//   Return a variable type given a Python type object or NULL if the Python
// type does not have a corresponding variable type.
//-----------------------------------------------------------------------------
static udt_VariableType *Variable_TypeByPythonType(
    udt_Cursor* cursor,                 // cursor variable created for
    PyObject* type)                     // Python type
{
    if (type == (PyObject*) &g_StringVarType)
        return &vt_String;
    if (type == (PyObject*) cxString_Type)
        return &vt_String;
    if (type == (PyObject*) &g_FixedCharVarType)
        return &vt_FixedChar;
#if PY_MAJOR_VERSION < 3
    if (type == (PyObject*) &g_UnicodeVarType)
        return &vt_NationalCharString;
    if (type == (PyObject*) &PyUnicode_Type)
        return &vt_NationalCharString;
    if (type == (PyObject*) &g_FixedUnicodeVarType)
        return &vt_FixedNationalChar;
#endif
    if (type == (PyObject*) &g_NCLOBVarType)
        return &vt_NCLOB;
    if (type == (PyObject*) &g_RowidVarType)
        return &vt_Rowid;
    if (type == (PyObject*) &g_BinaryVarType)
        return &vt_Binary;
    if (type == (PyObject*) &cxBinary_Type)
        return &vt_Binary;
    if (type == (PyObject*) &g_LongStringVarType)
        return &vt_LongString;
    if (type == (PyObject*) &g_LongBinaryVarType)
        return &vt_LongBinary;
    if (type == (PyObject*) &g_BFILEVarType)
        return &vt_BFILE;
    if (type == (PyObject*) &g_BLOBVarType)
        return &vt_BLOB;
    if (type == (PyObject*) &g_CLOBVarType)
        return &vt_CLOB;
    if (type == (PyObject*) &g_NumberVarType) {
        if (cursor->numbersAsStrings)
            return &vt_NumberAsString;
        return &vt_Float;
    }
    if (type == (PyObject*) &PyFloat_Type)
        return &vt_Float;
#if PY_MAJOR_VERSION < 3
    if (type == (PyObject*) &PyInt_Type)
        return &vt_Integer;
#endif
    if (type == (PyObject*) &PyLong_Type)
        return &vt_LongInteger;
    if (type == (PyObject*) &PyBool_Type)
        return &vt_Boolean;
    if (type == (PyObject*) &g_DateTimeVarType)
        return &vt_DateTime;
    if (type == (PyObject*) PyDateTimeAPI->DateType)
        return &vt_Date;
    if (type == (PyObject*) PyDateTimeAPI->DateTimeType)
        return &vt_DateTime;
    if (type == (PyObject*) &g_IntervalVarType)
        return &vt_Interval;
    if (type == (PyObject*) PyDateTimeAPI->DeltaType)
        return &vt_Interval;
    if (type == (PyObject*) &g_TimestampVarType)
        return &vt_Timestamp;
    if (type == (PyObject*) &g_CursorVarType)
        return &vt_Cursor;
#ifdef SQLT_BFLOAT
    if (type == (PyObject*) &g_NativeFloatVarType)
        return &vt_NativeFloat;
#endif
    if (type == (PyObject*) &g_ObjectVarType)
        return &vt_Object;

    PyErr_SetString(g_NotSupportedErrorException,
            "Variable_TypeByPythonType(): unhandled data type");
    return NULL;
}

// Go => Oracle type conversion interface
type OraTyper interface {
    GetVarType() *VariableType
}
*/

// Return a variable type given a Go object or error if the Go
// value does not have a corresponding variable type.
func VarTypeByValue(data interface{}) (vt *VariableType, size int, numElements uint, err error) {
	if data == nil {
		return &StringVarType, 1, 0, nil
	}
	switch x := data.(type) {
	case VariableType:
		return &x, x.size, 0, nil
	case *VariableType:
		return x, x.size, 0, nil
	case Variable:
		return x.typ, x.typ.size, 0, nil
	case *Variable:
		return x.typ, x.typ.size, 0, nil
	case string:
		if len(x) > MAX_STRING_CHARS {
			return &LongStringVarType, len(x), 0, nil
		}
		return StringVarType, len(x), 0, nil
	case bool:
		return &BoolVarType, 0, 0, nil
	case int8, uint8, byte, int16, uint16, int, uint, int32, uint32:
		return &IntVarType, 0, 0, nil
	case int64, uint64:
		return &LongVarType, 0, 0, nil
	case float32, float64:
		return &FloatVarType, 0, 0, nil
	case time.Duration:
		return &IntervarlVarType, 0, 0, nil
	case time.Time:
		return &DateTimeVarType, 0, 0, nil
	case []byte:
		if len(x) > MAX_BINARY_BYTES {
			return &LongBinaryVarType, len(x), 0, nil
		}
		return &BinaryVarType, len(x), 0, nil
	case CursorType:
		return &CursorVarType, 0, 0, nil
	case []interface{}:
		numElements = len(x)
		if numElements == 0 {
			return nil, 0, 0, ListIsEmpty
		}
		vt, size, _, err = VarTypeByValue(x[0])
		return
	}

	if data.(OraTyper) {
		return data.GetVarType(), 0, 0, nil
	}

	return nil, 0, 0, fmt.Errorf("unhandled data type %T", data)
}

// Return a variable type given an Oracle data type or NULL if the Oracle
// data type does not have a corresponding variable type.
func varTypeByOraDataType(oracleDataType C.ub2, charsetForm C.ub1) (*VariableType, error) {
	switch oracleDataType {
	case C.SQLT_LNG:
		return &LongStringVarType, nil
	case C.SQLT_AFC:
		return &FixedCharVarType, nil
	case C.SQLT_CHR:
		return &StringVarType, nil
	case C.SQLT_RDD:
		return &RowidVarType, nil
	case C.SQLT_BIN:
		return &BinaryVarType, nil
	case C.SQLT_LBI:
		return &LongBinaryVarType, nil
	case C.SQLT_BFLOAT, C.SQLT_IBFLOAT, C.SQLT_BDOUBLE, C.SQLT_IBDOUBLE:
		fallthrough
	case C.SQLT_NUM, C.SQLT_VNU:
		return &FloatVarType, nil
	case C.SQLT_DAT, C.SQLT_ODT:
		fallthrough
	case C.SQLT_DATE, C.SQLT_TIMESTAMP, C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
		return &DateTimeVarType, nil
	case C.SQLT_INTERVAL_DS:
		return &IntervalVarType
	case C.SQLT_CLOB:
		if charsetForm == C.SQLCS_NCHAR {
			return &NClobVarType, nil
		}
		return &ClobVarType, nil
	case C.SQLT_BLOB:
		return &BlobVarType, nil
	case C.SQLT_BFILE:
		return &BFileVarType, nil
	case C.SQLT_RSET:
		return &CursorVarType, nil
		// case C.SQLT_NTY:
		//     return &vt_Object;
	}
	return nil, fmt.Errorf("TypeByOracleDataType: unhandled data type %d",
		oracleDataType)
}

// Return a variable type given an Oracle descriptor.
func varTypeByOracleDescriptor(param *C.OCIParam, environment *Environment) (*VariableType, error) {
	var dataType C.ub2

	// retrieve datatype of the parameter
	if _, err := environment.AttrGet(param, C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_DATA_TYPE, unsafe.Pointer(&dataType),
		"data type"); err != nil {
		return nil, err
	}

	var charsetForm C.ub1
	// retrieve character set form of the parameter
	if dataType != C.SQLT_CHR && dataType != C.SQLT_AFC &&
		dataType != C.SQLT_CLOB {
		charsetForm = C.SQLCS_IMPLICIT
	} else {
		if _, err := environment.AttrGet(param, C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_CHARSET_FORM, unsafe.Pointer(&charsetForm),
			"charset form"); err != nil {
			return
		}
	}

	return varTypeByOracleDataType(dataType, charsetForm)
}

// Make the variable an array, ensuring that the type supports arrays.
func (v *Variable) makeArray() error {
	if !v.typ.canBeInArray {
		return fmt.Errorf("type does not support arrays")
	}
	v.isArray = true
	return nil
}

// Default method for determining the type of variable to use for the data.
func NewVariableByValue(cur *Cursor, value interface{}, numElements uint) (v *Variable, err error) {
	var varType *VariableType
	if varType, size, numElements, err = VarTypeByValue(value); err != nil {
		return
	}
	if v, err = NewVariable(cur, numElements, varType, size); err != nil {
		return
	}
	if x, ok := value.([]interface{}); ok {
		err = v.makeArray()
	}
	return
}

/*
// Allocate a new variable by looking at the type of the data.
static udt_Variable *Variable_NewByInputTypeHandler(
    udt_Cursor *cursor,                 // cursor to associate variable with
    PyObject *inputTypeHandler,         // input type handler
    PyObject *value,                    // Python value to associate
    unsigned numElements)               // number of elements to allocate
{
    PyObject *var;

    var = PyObject_CallFunction(inputTypeHandler, "OOi", cursor, value,
            numElements);
    if (!var)
        return NULL;
    if (var != Py_None) {
        if (!Variable_Check(var)) {
            Py_DECREF(var);
            PyErr_SetString(PyExc_TypeError,
                    "expecting variable from input type handler");
            return NULL;
        }
        return (udt_Variable*) var;
    }
    Py_DECREF(var);
    return Variable_DefaultNewByValue(cursor, value, numElements);
}


// Allocate a new variable by looking at the type of the data.
func NewVariableByValue(cur *Cursor, value interface{}, numElements uint) (v *Variable, err error) {
    if cur.inputTypeHandler && cursor->inputTypeHandler != Py_None)
        return Variable_NewByInputTypeHandler(cursor, cursor->inputTypeHandler,
                value, numElements);
    if (cursor->connection->inputTypeHandler &&
            cursor->connection->inputTypeHandler != Py_None)
        return Variable_NewByInputTypeHandler(cursor,
                cursor->connection->inputTypeHandler, value, numElements);
    return Variable_DefaultNewByValue(cursor, value, numElements);
}
*/

// Allocate a new PL/SQL array by looking at the Python data type.
func NewVariableArrayByType(cur *Cursor, element interface{}, numElements uint) (*Variable, error) {
	varType, err := Variable_TypeByPythonType(cursor, typeObj)
	if err != nil {
		return nil, err
	}
	return NewVariable(cur, numElements, varType, varType.size)
}

/*
//-----------------------------------------------------------------------------
// Variable_NewByType()
//   Allocate a new variable by looking at the Python data type.
//-----------------------------------------------------------------------------
static udt_Variable *Variable_NewByType(
    udt_Cursor *cursor,                 // cursor to associate variable with
    PyObject *value,                    // Python data type to associate
    unsigned numElements)               // number of elements to allocate
{
    udt_VariableType *varType;
    int size;

    // passing an integer is assumed to be a string
    if (PyInt_Check(value)) {
        size = PyInt_AsLong(value);
        if (PyErr_Occurred())
            return NULL;
        if (size > MAX_STRING_CHARS)
            varType = &vt_LongString;
        else varType = &vt_String;
        return Variable_New(cursor, numElements, varType, size);
    }

    // passing an array of two elements to define an array
    if (PyList_Check(value))
        return Variable_NewArrayByType(cursor, value);

    // handle directly bound variables
    if (Variable_Check(value)) {
        Py_INCREF(value);
        return (udt_Variable*) value;
    }

    // everything else ought to be a Python type
    varType = Variable_TypeByPythonType(cursor, value);
    if (!varType)
        return NULL;
    return Variable_New(cursor, numElements, varType, varType->size);
}

//-----------------------------------------------------------------------------
// Variable_NewByOutputTypeHandler()
//   Create a new variable by calling the output type handler.
//-----------------------------------------------------------------------------
static udt_Variable *Variable_NewByOutputTypeHandler(
    udt_Cursor *cursor,                 // cursor to associate variable with
    OCIParam *param,                    // parameter descriptor
    PyObject *outputTypeHandler,        // method to call to get type
    udt_VariableType *varType,          // variable type already chosen
    ub4 size,                           // maximum size of variable
    unsigned numElements)               // number of elements
{
    udt_Variable *var;
    PyObject *result;
    ub4 nameLength;
    sb2 precision;
    sword status;
    char *name;
    sb1 scale;

    // determine name of variable
    status = OCIAttrGet(param, OCI_HTYPE_DESCRIBE, (dvoid*) &name,
            &nameLength, OCI_ATTR_NAME, cursor->environment->errorHandle);
    if (Environment_CheckForError(cursor->environment, status,
            "Variable_NewByOutputTypeHandler(): get name") < 0)
        return NULL;

    // retrieve scale and precision of the parameter, if applicable
    precision = scale = 0;
    if (varType->pythonType == &g_NumberVarType) {
        status = OCIAttrGet(param, OCI_HTYPE_DESCRIBE, (dvoid*) &scale, 0,
                OCI_ATTR_SCALE, cursor->environment->errorHandle);
        if (Environment_CheckForError(cursor->environment, status,
                "Variable_NewByOutputTypeHandler(): get scale") < 0)
            return NULL;
        status = OCIAttrGet(param, OCI_HTYPE_DESCRIBE, (dvoid*) &precision, 0,
                OCI_ATTR_PRECISION, cursor->environment->errorHandle);
        if (Environment_CheckForError(cursor->environment, status,
                "Variable_NewByOutputTypeHandler(): get precision") < 0)
            return NULL;
    }

    // call method, passing parameters
    result = PyObject_CallFunction(outputTypeHandler, "Os#Oiii", cursor, name,
            nameLength, varType->pythonType, size, precision, scale);
    if (!result)
        return NULL;

    // if result is None, assume default behavior
    if (result == Py_None) {
        Py_DECREF(result);
        return Variable_New(cursor, numElements, varType, size);
    }

    // otherwise, verify that the result is an actual variable
    if (!Variable_Check(result)) {
        Py_DECREF(result);
        PyErr_SetString(PyExc_TypeError,
                "expecting variable from output type handler");
        return NULL;
    }

    // verify that the array size is sufficient to handle the fetch
    var = (udt_Variable*) result;
    if (var->allocatedElements < cursor->fetchArraySize) {
        Py_DECREF(result);
        PyErr_SetString(PyExc_TypeError,
                "expecting variable with array size large enough for fetch");
        return NULL;
    }

    return var;
}
*/

// Helper routine for Variable_Define() used so that constant calls to
// OCIDescriptorFree() is not necessary.
func variableDefineHelper(cur *Cursor, param *C.OCIParam, position, numElements uint) (v *Variable, err error) {
	var size C.ub4

	// determine data type
	varType, e := varTypeByOracleDescriptor(param, cur.environment)
	if e != nil {
		err = e
		return
	}
	// if (cursor->numbersAsStrings && varType == &vt_Float)
	//     varType = &vt_NumberAsString;

	// retrieve size of the parameter
	size = varType.size
	if varType.isVariableLength {
		var sizeFromOracle C.ub2
		// determine the maximum length from Oracle
		if _, err = cur.environment.AttrGet(
			param, C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_DATA_SIZE, unsafe.Pointer(&sizeFromOracle),
			"data size"); err != nil {
			return err
		}

		// use the length from Oracle directly if available
		if sizeFromOracle > 0 {
			size = sizeFromOracle
		} else if cur.outputSize >= 0 {
			// otherwise, use the value set with the setoutputsize() parameter
			if cursor.outputSizeColumn < 0 ||
				int(position) == cur.outputSizeColumn {
				size = cur.outputSize
			}
		}
	}

	// create a variable of the correct type
	/*
	   if cur.outputTypeHandler && cursor->outputTypeHandler != Py_None)
	       var = Variable_NewByOutputTypeHandler(cursor, param,
	               cursor->outputTypeHandler, varType, size, numElements);
	   else if (cursor->connection->outputTypeHandler &&
	           cursor->connection->outputTypeHandler != Py_None)
	       var = Variable_NewByOutputTypeHandler(cursor, param,
	               cursor->connection->outputTypeHandler, varType, size,
	               numElements);
	   else
	*/
	if v, err = NewVariable(cur, numElements, varType, size); err != nil {
		return
	}

	// call the procedure to set values prior to define
	if v.typ.preDefine != nil {
		if err = v.typ.preDefine(v, param); err != nil {
			return
		}
	}

	// perform the define
	if err = cur.environment.CheckStatus(
		C.OCIDefineByPos(cur.handle,
			(*unsafe.Pointer)(unsafe.Pointer(&v.defineHandle)),
			v.environment.errorHandle, position, v.data,
			v.bufferSize, v.typ.oracleType, v.indicator,
			v.actualLength, v.returnCode, C.OCI_DEFAULT),
		"define"); err != nil {
		return
	}

	// call the procedure to set values after define
	if v.typ.postDefine != nil {
		if err = v.typ.postDefine(v); err != nil {
			return
		}
	}

	return
}

// Allocate a variable and define it for the given statement.
func varDefine(cur *Cursor, numElements, position uint) (v *Variable, err error) {
	var param *C.OCIParam
	// retrieve parameter descriptor
	if err = cur.environment.CheckStatus(
		C.OCIParamGet(cur.handle, C.OCI_HTYPE_STMT,
			cur.environment.errorHandle,
			(*unsafe.Pointer)(unsafe.Pointer(&param)), position),
		"parameter"); err != nil {
		return
	}

	// call the helper to do the actual work
	v, err = variableDefineHelper(cur, param, position, numElements)
	C.OCIDescriptorFree(param, C.OCI_DTYPE_PARAM)
	return
}

// Allocate a variable and bind it to the given statement.
func (v *Variable) internalBind() (err error) {
	var status sword
	// perform the bind
	if v.boundName != "" {
		bname := []byte(v.boundName)
		if v.isArray {
			status = C.OCIBindByName(v.boundCursorHandle,
				&v.bindHandle,
				v.environment.errorHandle,
				(*C.OraText)(&bname[0]), len(bname),
				v.data, v.bufferSize,
				v.typ.oracleType, v.indicator, v.actualLength,
				v.returnCode, v.allocatedElements,
				&v.actualElements, C.OCI_DEFAULT)
		} else {
			status = C.OCIBindByName(v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle,
				(*C.OraText)(&bname[0]), len(bname),
				v.data, v.bufferSize,
				v.typ.oracleType, v.indicator, v.actualLength,
				v.returnCode, 0, nil, C.OCI_DEFAULT)
		}
	} else {
		if v.isArray {
			status = C.OCIBindByPos(v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle, v.boundPos, v.data,
				v.bufferSize, v.typ.oracleType, v.indicator,
				v.actualLength, v.returnCode,
				v.allocatedElements, &v.actualElements,
				C.OCI_DEFAULT)
		} else {
			status = C.OCIBindByPos(v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle, v.boundPos, v.data,
				v.bufferSize, v.typ.oracleType, v.indicator,
				v.actualLength, v.returnCode,
				0, nil,
				C.OCI_DEFAULT)
		}
	}
	if err = v.environment.CheckStatus(status); err != nil {
		return
	}

	// set the max data size for strings
	if (v.typ == &StringVarType || v.typ == &FixedCharVarType) &&
		v.size > v.typ.size {
		err = v.environment.AttrSet(v.bindHandle, C.OCI_HTYPE_BIND,
			C.OCI_ATTR_MAXDATA_SIZE, unsafe.Pointer(&v.typ.size))
	}

	return
}

// Allocate a variable and bind it to the given statement.
// bind to name or pos
func (v *Variable) Bind(cur *Cursor, name string, pos uint) error {
	// nothing to do if already bound
	if v.bindHandle != nil && name == v.boundName && pos == v.boundPos {
		return nil
	}

	// set the instance variables specific for binding
	v.boundPos = pos
	v.boundCursorHandle = cur.handle
	v.boundName = name

	// perform the bind
	return v.internalBind()
}

// Verifies that truncation or other problems did not take place on retrieve.
func (v *Variable) verifyFetch(arrayPos uint) error {
	if v.typ.isVariableLength {
		if code := v.returnCode[arrayPos]; code != 0 {
			err := NewError(code,
				fmt.Sprintf("column at array pos %d fetched with error: %d)",
					arrayPos, code))
			err.At = "verifyFetch"
			return err
		}
	}
	return nil
}

// Return the value of the variable at the given position.
func (v *Variable) getSingleValue(arrayPos uint) (interface{}, error) {
	var isNull bool

	// ensure we do not exceed the number of allocated elements
	if arrayPos >= v.allocatedElements {
		return nil, errors.New("Variable_GetSingleValue: array size exceeded")
	}

	// check for a NULL value
	if v.typ.isNull != nil {
		isNull = v.typ.isNull(v, arrayPos)
	} else {
		isNull = v.indicator[arrayPos] == C.OCI_IND_NULL
	}
	if isNull {
		return nil, nil
	}

	// check for truncation or other problems on retrieve
	if err := v.verifyFetch(arrayPos); err != nil {
		return nil, err
	}

	// calculate value to return
	return v.typ.getValue(v, arrayPos)
	/*
	   if value != nil && v->outConverter && var->outConverter != Py_None) {
	       result = PyObject_CallFunctionObjArgs(var->outConverter, value, NULL);
	       Py_DECREF(value);
	       return result;
	   }
	*/
}

// Return the value of the variable as an array.
func (v *Variable) getArrayvalue(numElements uint) (interface{}, error) {
	value := make([]interface{}, numElements)
	var singeValue interface{}
	var err error

	for i := 0; i < numElements; i++ {
		if singleValue, err = v.getSingleValue(i); err != nil {
			return err
		}
		value[i] = singleValue
	}

	return value, nil
}

// Return the value of the variable.
func (v *Variable) GetValue(arrayPos uint) (interface{}, error) {
	if v.isArray {
		return v.getArrayValue(v.actualElements)
	}
	return v.getSingleValue(arrayPos)
}

// Set a single value in the variable.
func (v *Variable) setSingleValue(arrayPos uint, value interface{}) error {
	// ensure we do not exceed the number of allocated elements
	if arrayPos >= v.allocatedElements {
		return errors.New("Variable_SetSingleValue: array size exceeded")
	}

	// convert value, if necessary
	/*
	   if (var->inConverter && var->inConverter != Py_None) {
	       convertedValue = PyObject_CallFunctionObjArgs(var->inConverter, value,
	               NULL);
	       if (!convertedValue)
	           return -1;
	       value = convertedValue;
	   }
	*/

	// check for a NULL value
	if value == nil {
		v.indicator[arrayPos] = C.OCI_IND_NULL
		return nil
	}

	v.indicator[arrayPos] = C.OCI_IND_NOTNULL
	if v.typ.isVariableLength {
		v.returnCode[arrayPos] = 0
	}
	return v.typ.setValue(v, arrayPos, value)
}

// Set all of the array values for the variable.
func (v *Variable) setArrayValue(value []interface{}) error {
	// ensure we haven't exceeded the number of allocated elements
	numElements := len(value)
	if numElements > v.allocatedElements {
		return errors.New("Variable_SetArrayValue: array size exceeded")
	}

	// set all of the values
	v.actualElements = numElements
	var err error
	for i, elt := range value {
		if err = v.setSingleValue(i, elt); err != nil {
			return err
		}
	}
	return nil
}

// Set the value of the variable.
func (v *Variable) SetValue(arrayPos uint, value interface{}) error {
	if v.isArray {
		if arrayPos > 0 {
			return errors.New("arrays of arrays are not supported by the OCI")
		}
		if x, ok := value.([]interface{}); !ok {
			return errors.New("array required!")
		} else {
			return v.setArrayValue(arrayPos, x)
		}
	}
	return v.setSingleValue(arrayPos, value)
}

// Copy the contents of the source variable to the destination variable.
func (targetVar *Variable) externalCopy(sourceVar *Variable, sourcePost, targetPos uint) error {
	if !sourceVar.typ.canBeCopied {
		return errors.New("variable does not support copying")
	}

	// ensure array positions are not violated
	if sourcePos >= sourceVar.allocatedElements {
		return errors.New("Variable_ExternalCopy: source array size exceeded")
	}
	if targetPos >= targetVar.allocatedElements {
		return errors.News("Variable_ExternalCopy: target array size exceeded")
	}

	// ensure target can support amount data from the source
	if targetVar.bufferSize < sourceVar.bufferSize {
		return errors.New("target variable has insufficient space to copy source data")
	}

	// handle null case directly
	if sourceVar.indicator[sourcePos] == C.OCI_IND_NULL {
		targetVar.indicator[targetPos] = C.OCI_IND_NULL
	} else { // otherwise, copy data
		targetVar.indicator[targetPos] = C.OCI_IND_NOTNULL
		var err error
		if err = sourceVar.verifyFetch(sourcePos); err != nil {
			return err
		}
		if targetVar.actualLength > 0 {
			targetVar.actualLength[targetPos] =
				sourceVar.actualLength[sourcePos]
		}
		if targetVar.returnCode != 0 {
			targetVar.returnCode[targetPos] =
				sourceVar.returnCode[sourcePos]
		}
		return copy(targetVar.data[targetPos*targetVar.bufferSize:],
			sourceVar.data[sourcePos*sourceVar.bufferSize:(sourcePos+1)*sourceVar.bufferSize])
	}

	return nil
}

// Set the value of the variable at the given position.
func (v *Variable) externalSetValue(pos uint, value interface{}) error {
	return v.setValue(pos, value)
}

// Return the value of the variable at the given position.
func (v *Variable) externalGetValue(pos uint) (interface{}, error) {
	return v.getValue(pos)
}

/*
//-----------------------------------------------------------------------------
// Variable_Repr()
//   Return a string representation of the variable.
//-----------------------------------------------------------------------------
static PyObject *Variable_Repr(
    udt_Variable *var)                  // variable to return the string for
{
    PyObject *valueRepr, *value, *module, *name, *result, *format, *formatArgs;

    if (var->isArray)
        value = Variable_GetArrayValue(var, var->actualElements);
    else if (var->allocatedElements == 1)
        value = Variable_GetSingleValue(var, 0);
    else value = Variable_GetArrayValue(var, var->allocatedElements);
    if (!value)
        return NULL;
    valueRepr = PyObject_Repr(value);
    Py_DECREF(value);
    if (!valueRepr)
        return NULL;
    format = cxString_FromAscii("<%s.%s with value %s>");
    if (!format) {
        Py_DECREF(valueRepr);
        return NULL;
    }
    if (GetModuleAndName(Py_TYPE(var), &module, &name) < 0) {
        Py_DECREF(valueRepr);
        Py_DECREF(format);
        return NULL;
    }
    formatArgs = PyTuple_Pack(3, module, name, valueRepr);
    Py_DECREF(module);
    Py_DECREF(name);
    Py_DECREF(valueRepr);
    if (!formatArgs) {
        Py_DECREF(format);
        return NULL;
    }
    result = cxString_Format(format, formatArgs);
    Py_DECREF(format);
    Py_DECREF(formatArgs);
    return result;
}
*/
