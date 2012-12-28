package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	"time"
	"unsafe"
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
	connection                        *Connection //for LOBs
	typ                               *VariableType
	allocatedElements, actualElements uint
	boundPos, internalFetchNum        uint
	size, bufferSize                  uint
	environment                       *Environment
	isArray, isAllocatedInternally    bool
	indicator                         []C.sb2
	returnCode, actualLength          []C.ub2
	dataBytes                         []byte
	dataInts                          []int64
	dataFloats                        []float64
	cursors                           []*Cursor
}

// allocate a new variable
func NewVariable(cur *Cursor, numElements uint, varType *VariableType, size uint) (v *Variable, err error) {
	// log.Printf("cur=%+v varType=%+v", cur, varType)
	// perform basic initialization
	if numElements < 1 {
		numElements = 1
	}
	v = &Variable{typ: varType, environment: cur.connection.environment,
		isAllocatedInternally: true, allocatedElements: numElements,
		size:      varType.size,
		indicator: make([]C.sb2, numElements), //sizeof(sb2)
		// returnCode:   make([]C.ub2, numElements),
		// actualLength: make([]C.ub2, numElements),
	}

	// set the maximum length of the variable, ensure that a minimum of
	// 2 bytes is allocated to ensure that the array size check works
	if v.typ.isVariableLength {
		if size < 2 {
			size = 2
		}
		v.size = size
	}

	// allocate the data for the variable
	log.Printf("allocate data for the variable")
	if err = v.allocateData(); err != nil {
		return
	}

	// for variable length data, also allocate the return code
	if v.typ.isVariableLength {
		v.returnCode = make([]C.ub2, v.allocatedElements)
		v.actualLength = make([]C.ub2, v.allocatedElements)
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
	Name                         string
	isVariableLength, isCharData bool
	size                         uint
	canBeInArray, canBeCopied    bool
	charsetForm                  C.ub1
	oracleType                   C.ub2
	initialize                   func(*Variable, *Cursor) error
	finalize                     func(*Variable) error
	preDefine                    func(*Variable, *C.OCIParam) error
	postDefine                   func(*Variable) error
	isNull                       func(*Variable, uint) bool
	getValue                     func(*Variable, uint) (interface{}, error)
	setValue                     func(*Variable, uint, interface{}) error
	preFetch                     func(*Variable) error
	getBufferSize                func(*Variable) uint
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

func (t VariableType) IsBinary() bool {
	return false
}
func (t VariableType) IsString() bool {
	return false
}
func (t VariableType) IsDate() bool {
	return false
}

func (t *VariableType) NewVariable(cur *Cursor, numElements uint, size uint) (*Variable, error) {
	return NewVariable(cur, numElements, t, size)
}

func (t *VariableType) String() string {
	return fmt.Sprintf("<%s %d var?%s char?%s>", t.Name, t.oracleType,
		t.isVariableLength, t.isCharData)
}

func (env *Environment) varTypeByOracleDescriptor(param *C.OCIParam) (*VariableType, error) {
	return nil, nil
}

func (v *Variable) getDataArr() (p unsafe.Pointer) {
	defer func() {
		log.Printf("getDataArr(%d): %v", v.typ.oracleType, p)
		if p == nil {
			log.Panicf("getDataArr(%+v) returns nil pointer!", v)
		}
	}()

	if v.dataBytes != nil {
		log.Printf("getDataArr(%d) len=%d", v.typ.oracleType, len(v.dataBytes))
		return (unsafe.Pointer(&v.dataBytes[0]))
	} else if v.dataInts != nil {
		return (unsafe.Pointer(&v.dataInts[0]))
	} else {
		return (unsafe.Pointer(&v.dataFloats[0]))

	}
	log.Panicf("everything is nil!")
	return nil
}

// Allocate the data for the variable.
func (v *Variable) allocateData() error {
	// set the buffer size for the variable
	if v.typ.getBufferSize != nil {
		v.bufferSize = v.typ.getBufferSize(v)
	} else {
		v.bufferSize = v.size
	}
	if v.bufferSize%2 > 0 {
		v.bufferSize++
	}

	// allocate the data as long as it is small enough
	dataLength := v.allocatedElements * v.bufferSize
	if dataLength > 1<<31-1 {
		return ArrayTooLarge
	}
	log.Printf("%s bufsize=%d dataLength=%d", v.typ, v.bufferSize, dataLength)
	v.dataFloats = nil
	v.dataInts = nil
	v.dataBytes = nil
	if false && v.typ.IsNumber() && !v.typ.isCharData &&
		(v.typ == NativeFloatVarType || v.typ.IsInteger()) {
		if v.typ == NativeFloatVarType {
			v.dataFloats = make([]float64, v.allocatedElements)
			log.Printf("floats=%v", unsafe.Pointer(&v.dataFloats[0]))
		} else {
			v.dataInts = make([]int64, v.allocatedElements)
			log.Printf("ints=%v", unsafe.Pointer(&v.dataInts[0]))
		}
	} else {
		v.dataBytes = make([]byte, dataLength)
		log.Printf("bytes=%v (%d)", unsafe.Pointer(&v.dataBytes[0]), len(v.dataBytes))
	}

	return nil
}

// Free an existing variable.
func (v *Variable) Free() {
	if v.isAllocatedInternally {
		if v.typ.finalize != nil {
			v.typ.finalize(v)
		}
		v.connection = nil
		v.cursors = nil
		v.indicator = nil
		v.dataBytes = nil
		v.dataInts = nil
		v.dataFloats = nil
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
	if v.dataBytes == nil {
		return nil
	}

	nsize := v.allocatedElements * size
	if len(v.dataBytes) == int(nsize) {
		return nil
	}
	v.bufferSize = size
	if len(v.dataBytes) < int(nsize) {
		v.dataBytes = v.dataBytes[:nsize]
	} else {
		v.dataBytes = append(v.dataBytes, make([]byte, nsize-uint(len(v.dataBytes)))...)
	}

	// force rebinding
	if v.boundName != "" || v.boundPos > 0 {
		return v.internalBind()
	}

	return nil
}

// Go => Oracle type conversion interface
type OraTyper interface {
	GetVarType() *VariableType
}

// Return a variable type given a Go object or error if the Go
// value does not have a corresponding variable type.
func VarTypeByValue(data interface{}) (vt *VariableType, size uint, numElements uint, err error) {
	if data == nil {
		return StringVarType, 1, 0, nil
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
			return LongStringVarType, uint(len(x)), 0, nil
		}
		return StringVarType, uint(len(x)), 0, nil
	case bool:
		return BooleanVarType, 0, 0, nil
	case int8, uint8, int16, uint16, int, uint, int32, uint32:
		return Int32VarType, 0, 0, nil
	case int64, uint64:
		return Int64VarType, 0, 0, nil
	case float32, float64:
		return FloatVarType, 0, 0, nil
	case time.Time:
		return DateTimeVarType, 0, 0, nil
	case time.Duration:
		return IntervalVarType, 0, 0, nil
	// case CursorVarType:
	// 	return CursorVarType, 0, 0, nil
	case []byte:
		if len(x) > MAX_BINARY_BYTES {
			return LongBinaryVarType, uint(len(x)), 0, nil
		}
		return BinaryVarType, uint(len(x)), 0, nil
	case []interface{}:
		numElements = uint(len(x))
		if numElements == 0 {
			return nil, 0, 0, ListIsEmpty
		}
		vt, size, _, err = VarTypeByValue(x[0])
		return
	}

	if x, ok := data.(OraTyper); ok {
		return x.GetVarType(), 0, 0, nil
	}

	return nil, 0, 0, fmt.Errorf("unhandled data type %T", data)
}

// Return a variable type given an Oracle data type or NULL if the Oracle
// data type does not have a corresponding variable type.
func varTypeByOraDataType(oracleDataType C.ub2, charsetForm C.ub1) (*VariableType, error) {
	switch oracleDataType {
	case C.SQLT_RDD:
		return RowidVarType, nil
	case C.SQLT_DAT, C.SQLT_ODT:
		fallthrough
	case C.SQLT_DATE, C.SQLT_TIMESTAMP, C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
		return DateTimeVarType, nil
	case C.SQLT_INTERVAL_DS:
		return IntervalVarType, nil
	case C.SQLT_LNG:
		return LongStringVarType, nil
	case C.SQLT_LBI:
		return LongBinaryVarType, nil
	case C.SQLT_RSET:
		return CursorVarType, nil
		// case C.SQLT_NTY:
		//     return &vt_Object;
	case C.SQLT_CLOB:
		if charsetForm == C.SQLCS_NCHAR {
			return NClobVarType, nil
		}
		return ClobVarType, nil
	case C.SQLT_BLOB:
		return BlobVarType, nil
	case C.SQLT_BFILE:
		return BFileVarType, nil
	case C.SQLT_AFC:
		return FixedCharVarType, nil
	case C.SQLT_CHR:
		// log.Printf("StringVarType=%v", StringVarType)
		return StringVarType, nil
	case C.SQLT_BIN:
		return BinaryVarType, nil
	case C.SQLT_BFLOAT, C.SQLT_IBFLOAT, C.SQLT_BDOUBLE, C.SQLT_IBDOUBLE:
		fallthrough
	case C.SQLT_NUM, C.SQLT_VNU:
		return FloatVarType, nil
	}
	log.Printf("unhandled data type: %d", oracleDataType)
	return nil, fmt.Errorf("TypeByOracleDataType: unhandled data type %d",
		oracleDataType)
}

// Return a variable type given an Oracle descriptor.
func varTypeByOracleDescriptor(param *C.OCIParam, environment *Environment) (*VariableType, error) {
	var dataType C.ub2

	// retrieve datatype of the parameter
	if _, err := environment.AttrGet(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_DATA_TYPE, unsafe.Pointer(&dataType),
		"data type"); err != nil {
		log.Printf("error with data type: %s", err)
		return nil, err
	}

	var charsetForm C.ub1
	// retrieve character set form of the parameter
	if dataType != C.SQLT_CHR && dataType != C.SQLT_AFC &&
		dataType != C.SQLT_CLOB {
		charsetForm = C.SQLCS_IMPLICIT
	} else {
		if _, err := environment.AttrGet(
			unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_CHARSET_FORM, unsafe.Pointer(&charsetForm),
			"charset form"); err != nil {
			log.Printf("error with charsetForm: %s", err)
			return nil, err
		}
	}

	return varTypeByOraDataType(dataType, charsetForm)
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
	var size uint
	if varType, size, numElements, err = VarTypeByValue(value); err != nil {
		return
	}
	if v, err = NewVariable(cur, numElements, varType, size); err != nil {
		return
	}
	if _, ok := value.([]interface{}); ok {
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

// Allocate a new PL/SQL array by looking at the data
func NewVariableArrayByValue(cur *Cursor, element interface{}, numElements uint) (*Variable, error) {
	varType, size, _, err := VarTypeByValue(element)
	if err != nil {
		return nil, err
	}
	return NewVariable(cur, numElements, varType, size)
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

func (v *Variable) aLrC() (aL, rC *C.ub2) {
	if v.actualLength != nil {
		aL = &v.actualLength[0]
		rC = &v.returnCode[0]
	}
	return aL, rC
}

// Helper routine for Variable_Define() used so that constant calls to
// OCIDescriptorFree() is not necessary.
func variableDefineHelper(cur *Cursor, param *C.OCIParam, position, numElements uint) (v *Variable, err error) {
	var size C.ub4
	var varType *VariableType

	// determine data type
	varType, err = varTypeByOracleDescriptor(param, cur.environment)
	if err != nil {
		log.Printf("error determining data type: %s", err)
		return nil, err
	}
	// if (cursor->numbersAsStrings && varType == &vt_Float)
	//     varType = &vt_NumberAsString;

	// retrieve size of the parameter
	size = C.ub4(varType.size)
	if varType.isVariableLength {
		var sizeFromOracle C.ub2
		// determine the maximum length from Oracle
		if _, err = cur.environment.AttrGet(
			unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_DATA_SIZE, unsafe.Pointer(&sizeFromOracle),
			"data size"); err != nil {
			log.Printf("error getting data size: %+v", err)
			return nil, err
		}
		log.Printf("size of %v @ %d: %d", param, position, sizeFromOracle)

		// use the length from Oracle directly if available
		if uint(sizeFromOracle) > 0 {
			size = C.ub4(sizeFromOracle)
		} else if cur.outputSize >= 0 {
			// otherwise, use the value set with the setoutputsize() parameter
			if cur.outputSizeColumn < 0 ||
				int(position) == cur.outputSizeColumn {
				size = C.ub4(cur.outputSize)
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
	v, err = NewVariable(cur, numElements, varType, uint(size))
	if err != nil {
		return nil, fmt.Errorf("error creating variable: %s", err)
	}

	// call the procedure to set values prior to define
	if v.typ.preDefine != nil {
		if err = v.typ.preDefine(v, param); err != nil {
			return nil, fmt.Errorf("error with preDefine(%s): %s", v, err)
		}
	}

	// perform the define
	aL, rC := v.aLrC()
	log.Printf("OCIDefineByPos(typ=%s, pos=%d, data=%v, size=%d, oracleType=%d, indicator=%v aL=%v rC=%v",
		v.typ, position, v.getDataArr(), v.bufferSize, v.typ.oracleType, v.indicator, aL, rC)
	if err = cur.environment.CheckStatus(
		C.OCIDefineByPos(cur.handle,
			&v.defineHandle,
			v.environment.errorHandle, C.ub4(position), v.getDataArr(),
			C.sb4(v.bufferSize), v.typ.oracleType,
			unsafe.Pointer(&v.indicator[0]),
			aL, rC, C.OCI_DEFAULT),
		"define"); err != nil {
		return nil, fmt.Errorf("error defining: %s", err)
	}

	// call the procedure to set values after define
	if v.typ.postDefine != nil {
		if err = v.typ.postDefine(v); err != nil {
			return nil, fmt.Errorf("error with postDefine(%s): %s", v, err)
		}
	}

	return v, nil
}

// Allocate a variable and define it for the given statement.
func varDefine(cur *Cursor, numElements, position uint) (*Variable, error) {
	var param *C.OCIParam
	// retrieve parameter descriptor
	if cur.handle == nil {
		log.Printf("WARN: nil cursor handle in varDefine!")
	}
	log.Printf("retrieve parameter descriptor cur.handle=%s pos=%d", cur.handle, position)
	if err := cur.environment.CheckStatus(
		C.OCIParamGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			cur.environment.errorHandle,
			(*unsafe.Pointer)(unsafe.Pointer(&param)), C.ub4(position)),
		"parameter"); err != nil {
		log.Printf("NO PARAM! %s", err)
		return nil, err
	}
	log.Printf("got param handle")

	// call the helper to do the actual work
	v, err := variableDefineHelper(cur, param, position, numElements)
	log.Printf("variable defined err=%s nil?%s", err, err == nil)
	C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)
	return v, err
}

// Allocate a variable and bind it to the given statement.
func (v *Variable) internalBind() (err error) {
	var status C.sword
	// perform the bind
	aL, rC := v.aLrC()
	if v.boundName != "" {
		bname := []byte(v.boundName)
		if v.isArray {
			actElts := C.ub4(0)
			status = C.OCIBindByName(v.boundCursorHandle,
				&v.bindHandle,
				v.environment.errorHandle,
				(*C.OraText)(&bname[0]), C.sb4(len(bname)),
				v.getDataArr(), C.sb4(v.bufferSize),
				v.typ.oracleType, unsafe.Pointer(&v.indicator[0]),
				aL, rC,
				C.ub4(v.allocatedElements),
				&actElts, C.OCI_DEFAULT)
			v.actualElements = uint(actElts)
		} else {
			status = C.OCIBindByName(v.boundCursorHandle,
				&v.bindHandle,
				v.environment.errorHandle,
				(*C.OraText)(&bname[0]), C.sb4(len(bname)),
				v.getDataArr(), C.sb4(v.bufferSize),
				v.typ.oracleType, unsafe.Pointer(&v.indicator[0]),
				aL, rC,
				0, nil, C.OCI_DEFAULT)
		}
	} else {
		if v.isArray {
			actElts := C.ub4(0)
			status = C.OCIBindByPos(v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle, C.ub4(v.boundPos), v.getDataArr(),
				C.sb4(v.bufferSize), v.typ.oracleType,
				unsafe.Pointer(&v.indicator[0]),
				aL, rC,
				C.ub4(v.allocatedElements), &actElts, C.OCI_DEFAULT)
			v.actualElements = uint(actElts)
		} else {
			status = C.OCIBindByPos(v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle, C.ub4(v.boundPos), v.getDataArr(),
				C.sb4(v.bufferSize), v.typ.oracleType,
				unsafe.Pointer(&v.indicator[0]),
				aL, rC,
				0, nil, C.OCI_DEFAULT)
		}
	}
	if err = v.environment.CheckStatus(status, "BindBy"); err != nil {
		return
	}

	// set the max data size for strings
	if (v.typ == StringVarType || v.typ == FixedCharVarType) &&
		v.size > v.typ.size {
		err = v.environment.AttrSet(
			unsafe.Pointer(v.bindHandle), C.OCI_HTYPE_BIND,
			C.OCI_ATTR_MAXDATA_SIZE, unsafe.Pointer(&v.typ.size),
			C.sizeof_ub4)
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
			err := NewError(int(code),
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
func (v *Variable) getArrayValue(numElements uint) (interface{}, error) {
	value := make([]interface{}, numElements)
	var singleValue interface{}
	var err error

	for i := 0; i < int(numElements); i++ {
		if singleValue, err = v.getSingleValue(uint(i)); err != nil {
			return nil, err
		}
		value[i] = singleValue
	}

	return value, nil
}

// Return the value of the variable.
func (v *Variable) GetValue(arrayPos uint) (interface{}, error) {
	if v.isArray {
		return v.getArrayValue(uint(v.actualElements))
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
	numElements := uint(len(value))
	if numElements > v.allocatedElements {
		return errors.New("Variable_SetArrayValue: array size exceeded")
	}

	// set all of the values
	v.actualElements = numElements
	var err error
	for i, elt := range value {
		if err = v.setSingleValue(uint(i), elt); err != nil {
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
			return v.setArrayValue(x)
		}
	}
	log.Printf("calling %s.setValue(%d, %v (%T))", v.typ, arrayPos, value, value)
	return v.setSingleValue(arrayPos, value)
}

// Copy the contents of the source variable to the destination variable.
func (targetVar *Variable) externalCopy(sourceVar *Variable, sourcePos, targetPos uint) error {
	if !sourceVar.typ.canBeCopied {
		return errors.New("variable does not support copying")
	}

	// ensure array positions are not violated
	if sourcePos >= sourceVar.allocatedElements {
		return errors.New("Variable_ExternalCopy: source array size exceeded")
	}
	if targetPos >= targetVar.allocatedElements {
		return errors.New("Variable_ExternalCopy: target array size exceeded")
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
		if targetVar.actualLength[targetPos] > 0 {
			targetVar.actualLength[targetPos] =
				sourceVar.actualLength[sourcePos]
		}
		if targetVar.returnCode[targetPos] != 0 {
			targetVar.returnCode[targetPos] =
				sourceVar.returnCode[sourcePos]
		}

		dp := targetPos * targetVar.bufferSize
		sp := sourcePos * sourceVar.bufferSize
		sq := (sourcePos + 1) * sourceVar.bufferSize
		switch {
		case sourceVar.dataFloats != nil:
			copy(targetVar.dataFloats[dp:], sourceVar.dataFloats[sp:sq])
		case sourceVar.dataInts != nil:
			copy(targetVar.dataInts[dp:], sourceVar.dataInts[sp:sq])
		default:
			copy(targetVar.dataBytes[dp:], sourceVar.dataBytes[sp:sq])
		}
		return nil
	}

	return nil
}

// Set the value of the variable at the given position.
func (v *Variable) externalSetValue(pos uint, value interface{}) error {
	return v.SetValue(pos, value)
}

// Return the value of the variable at the given position.
func (v *Variable) externalGetValue(pos uint) (interface{}, error) {
	return v.GetValue(pos)
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
