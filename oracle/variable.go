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
#include <string.h>
*/
import "C"

import (
	"fmt"
	"log"
	"reflect"
	"time"
	"unsafe"

	"github.com/juju/errgo"
)

var (
	//NotImplemented prints not implemented
	NotImplemented = errgo.New("not implemented")
	//ArrayTooLarge prints array too large
	ArrayTooLarge = errgo.New("array too large")
)

//Variable holds the handles for a variable
type Variable struct {
	// private or unexported fields
	bindHandle        *C.OCIBind
	defineHandle      *C.OCIDefine
	boundCursorHandle *C.OCIStmt
	boundName         string
	//PyObject*inConverter;
	//PyObject*outConverter;
	connection                     *Connection //for LOBs
	typ                            *VariableType
	allocatedElements              uint
	actualElements                 C.ub4
	boundPos, internalFetchNum     uint
	size, bufferSize               uint
	environment                    *Environment
	isArray, isAllocatedInternally bool
	indicator                      []C.sb2
	returnCode, actualLength       []C.ub2
	dataBytes                      []byte
	dataInts                       []int64
	dataFloats                     []float64
	cursors                        []*Cursor
	destination                    reflect.Value
}

// NewVariable allocates a new variable
func (cur *Cursor) NewVariable(numElements uint, varType *VariableType, size uint) (v *Variable, err error) {
	// log.Printf("cur=%+v varType=%+v", cur, varType)
	// perform basic initialization
	isArray := numElements > 0
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
	if isArray {
		v.makeArray()
	}

	// log.Printf("NewVariable(elts=%d typ=%s)", numElements, varType)

	// set the maximum length of the variable, ensure that a minimum of
	// 2 bytes is allocated to ensure that the array size check works
	if v.typ.isVariableLength {
		if size < 2 {
			size = 2
		}
		v.size = size
	}

	// allocate the data for the variable
	// log.Printf("allocate data for the variable")
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
		if err = v.typ.initialize(v, cur); err != nil {
			err = errgo.Newf("error initializing %s: %s", v, err)
			return
		}
	}

	return v, nil
}

// String returns a (short) representation of the variable
func (v *Variable) String() string {
	return fmt.Sprintf("<Variable %s of %p>", v.typ.Name, v.boundCursorHandle)
}

// VariableDescription holds the description of a variable
type VariableDescription struct {
	Name                                              string
	Type, InternalSize, DisplaySize, Precision, Scale int
	NullOk                                            bool
}

// VariableType holds data for a variable
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

// FIXME: proper Into, not just this dummy
// getValueInto fetches value into the dest pointer
func (t *VariableType) getValueInto(dest interface{}, v *Variable, pos uint) error {
	rval := reflect.ValueOf(dest)
	if rval.Kind() != reflect.Ptr {
		return errgo.Newf("%s.getValueInto: a pointer is needed, got %T", v, dest)
	}
	val, err := t.getValue(v, pos)
	if err != nil {
		return errgo.Mask(err)
	}
	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(val))
	// log.Printf("%s.getValueInto dest=%+v err=%s", t, *dest, err)
	return nil
}

// isVariable returns a boolean indicating if the object is a variable.
func isVariable(value interface{}) bool {
	//FIXME
	if _, ok := value.(Variable); ok {
		return true
	}
	if _, ok := value.(*Variable); ok {
		return true
	}
	return false
}

// NewVariable returns a new Variable of the given VariableType
func (t *VariableType) NewVariable(cur *Cursor, numElements uint, size uint) (*Variable, error) {
	return cur.NewVariable(numElements, t, size)
}

// NewVar creates a bind variable and returns it.
// If value is a pointer, then after cur.Execute, data will be returned into it
// automatically, no need to call v.GetValue.
func (cur *Cursor) NewVar(value interface{}, /*inconverter, outconverter, typename*/
) (v *Variable, err error) {
	// determine the type of variable
	// varType = Variable_TypeByPythonType(self, type);
	rval := reflect.ValueOf(value)
	val := value
	if rval.Kind() == reflect.Ptr {
		val = rval.Elem().Interface()
	}
	varType, size, numElements, err := VarTypeByValue(val)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if varType.isVariableLength && size == 0 {
		size = varType.size
	}
	log.Printf("varType=%v size=%d numElements=%d", varType, size, numElements)
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

	// set the value, if applicable
	err = v.SetValue(0, value)
	return
}

// NewArrayVar creates an array bind variable and return it.
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
	err = v.setArrayValue(values)
	return
}

// String returns a string representation of the VariableType
func (t *VariableType) String() string {
	return fmt.Sprintf("<%s %d var?%t char?%t>", t.Name, t.oracleType,
		t.isVariableLength, t.isCharData)
}

func (env *Environment) varTypeByOracleDescriptor(param *C.OCIParam) (*VariableType, error) {
	var (
		charsetForm C.ub1
		dataType    C.ub2
	)

	// retrieve datatype of the parameter
	if _, err := env.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_DATA_TYPE, unsafe.Pointer(&dataType),
		"param dataType"); err != nil {
		return nil, errgo.Mask(

			// retrieve character set form of the parameter
			err)
	}

	if dataType != C.SQLT_CHR && dataType != C.SQLT_AFC &&
		dataType != C.SQLT_CLOB {
		charsetForm = C.SQLCS_IMPLICIT
	} else {
		if _, err := env.AttrGet(unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
			C.OCI_ATTR_CHARSET_FORM, unsafe.Pointer(&charsetForm),
			"param charsetForm"); err != nil {
			return nil, errgo.Mask(err)
		}
	}

	return varTypeByOraDataType(dataType, charsetForm)
}

func (v *Variable) getDataArr() (p unsafe.Pointer) {
	defer func() {
		// log.Printf("getDataArr(%d): %v", v.typ.oracleType, p)
		if p == nil {
			log.Panicf("getDataArr(%+v) returns nil pointer!", v)
		}
	}()

	if len(v.dataBytes) > 0 {
		// log.Printf("getDataArr(%d) len=%d", v.typ.oracleType, len(v.dataBytes))
		return (unsafe.Pointer(&v.dataBytes[0]))
	} else if len(v.dataInts) > 0 {
		return (unsafe.Pointer(&v.dataInts[0]))
	}
	return (unsafe.Pointer(&v.dataFloats[0]))
}

// ArrayLength returns the number of allocated elements (array length for arrays)
func (v Variable) ArrayLength() uint {
	//log.Printf("actualElements=%d allocatedElements=%d", v.actualElements, v.allocatedElements)
	return uint(v.actualElements)
}

// allocateData allocates the data for the variable.
func (v *Variable) allocateData() error {
	if CTrace {
		ctrace("%s.allocateData", v)
	}
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
	//log.Printf("%s bufsize=%d dataLength=%d", v.typ, v.bufferSize, dataLength)
	v.dataFloats = nil
	v.dataInts = nil
	v.dataBytes = nil
	if v.typ.IsNumber() && !v.typ.isCharData && v.typ.oracleType != C.SQLT_VNU {
		//(v.typ == NativeFloatVarType || v.typ.IsInteger()) {
		if v.typ == NativeFloatVarType {
			v.dataFloats = make([]float64, v.allocatedElements)
			//log.Printf("floats=%v", unsafe.Pointer(&v.dataFloats[0]))
		} else {
			v.dataInts = make([]int64, v.allocatedElements)
			//log.Printf("ints=%v", unsafe.Pointer(&v.dataInts[0]))
		}
	} else {
		v.dataBytes = make([]byte, dataLength)
		// log.Printf("bytes=%v (%d)", unsafe.Pointer(&v.dataBytes[0]), len(v.dataBytes))
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

	v.dataBytes = v.dataBytes[:cap(v.dataBytes)]
	nsize := v.allocatedElements * size
	olen := len(v.dataBytes)
	if olen == int(nsize) {
		return nil
	}
	v.bufferSize = size
	if olen > int(nsize) {
		// log.Printf("olen=%d > nsize=%d", olen, nsize)
		v.dataBytes = v.dataBytes[:nsize]
	} else {
		// log.Printf("olen=%d < nsize=%d", olen, nsize)
		v.dataBytes = append(v.dataBytes, make([]byte, nsize-uint(olen))...)
	}

	// force rebinding
	if v.boundName != "" || v.boundPos > 0 {
		return v.internalBind()
	}

	return nil
}

// OraTyper is a Go => Oracle type conversion interface
type OraTyper interface {
	GetVarType() *VariableType
}

// VarTypeByValue returns a variable type given a Go object or error if the Go
// value does not have a corresponding variable type.
func VarTypeByValue(data interface{}) (vt *VariableType, size uint, numElements uint, err error) {
	// defer func() {
	// 	log.Printf("VarTypeByValue(%T) => %s", data, vt)
	// }()

	if data == nil {
		return StringVarType, 1, 0, nil
	}
	switch x := data.(type) {
	case VariableType:
		return &x, x.size, 0, nil
	case *VariableType:
		if x == nil {
			return StringVarType, 1, 0, nil
		}
		return x, x.size, 0, nil
	case Variable:
		return x.typ, x.typ.size, 0, nil
	case *Variable:
		if x == nil {
			return StringVarType, 1, 0, nil
		}
		return x.typ, x.typ.size, 0, nil

	case string:
		if len(x) > MaxStringChars {
			return LongStringVarType, uint(len(x)), 0, nil
		}
		return StringVarType, uint(len(x)), 0, nil
	case *string:
		return VarTypeByValue(*x)
	case []string:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue("")
		for _, y := range x {
			size = maxUint(size, uint(len(y)+1))
		}
		return
	case bool:
		return BooleanVarType, 0, 0, nil
	case *bool:
		return VarTypeByValue(*x)

	case int8, uint8, int16, uint16, int, uint, int32, uint32:
		return Int32VarType, 0, 0, nil
	case []int32:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue(int32(0))
		return

	case int64, uint64:
		return Int64VarType, 0, 0, nil
	case []int64:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue(int64(0))
		return

	case float32, float64:
		return FloatVarType, 0, 0, nil
	case []float32:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue(float32(0))
		return
	case []float64:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue(float64(0))
		return

	case time.Time:
		return DateTimeVarType, 0, 0, nil
	case []time.Time:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue(time.Time{})
		return
	case *time.Time:
		return VarTypeByValue(*x)

	case time.Duration:
		return IntervalVarType, 0, 0, nil

	case []byte:
		if len(x) > MaxBinaryBytes {
			return LongBinaryVarType, uint(len(x)), 0, nil
		}
		return BinaryVarType, uint(len(x)), 0, nil
	case [][]byte:
		numElements = uint(len(x))
		vt, size, _, err = VarTypeByValue([]byte{})
		return

	case []interface{}:
		numElements = uint(len(x))
		if numElements == 0 {
			return nil, 0, 0, ListIsEmpty
		}
		vt, size, _, err = VarTypeByValue(x[0])
		return

	case *Cursor:
		return CursorVarType, 0, 0, nil
	}

	if x, ok := data.(OraTyper); ok {
		return x.GetVarType(), 0, 0, nil
	}

	return nil, 0, 0, errgo.Newf("unhandled data type %T", data)
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
	return nil, errgo.Newf("TypeByOracleDataType: unhandled data type %d",
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
		return errgo.Newf("type does not support arrays")
	}
	v.isArray = true
	return nil
}

// NewVariableByValue is the default method for determining the
// type of variable to use for the data.
func (cur *Cursor) NewVariableByValue(value interface{}, numElements uint) (v *Variable, err error) {
	var varType *VariableType
	var size uint
	if varType, size, numElements, err = VarTypeByValue(value); err != nil {
		return
	}
	if v, err = cur.NewVariable(numElements, varType, size); err != nil {
		return
	}
	//log.Printf("NewVariableByValue(%v, %d) isArray? %s", value, numElements,
	//	reflect.TypeOf(value).Kind() == reflect.Slice)
	//if reflect.TypeOf(value).Kind() == reflect.Slice {
	//	err = v.makeArray()
	//}
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

// NewVariableArrayByValue allocates a new PL/SQL array by looking at the data
func (cur *Cursor) NewVariableArrayByValue(element interface{}, numElements uint) (*Variable, error) {
	varType, size, _, err := VarTypeByValue(element)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	v, err := cur.NewVariable(numElements, varType, size)
	if err != nil {
		return nil, errgo.Mask(

			//v.makeArray()
			err)
	}

	return v, nil
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
        if (size > MaxStringChars)
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

func (v *Variable) aLrC() (indic unsafe.Pointer, aL, rC *C.ub2) {
	indic = unsafe.Pointer(&v.indicator[0])
	if len(v.actualLength) > 0 && len(v.returnCode) > 0 {
		aL = &v.actualLength[0]
		rC = &v.returnCode[0]
	}
	return
}

// Helper routine for Variable_Define() used so that constant calls to
// OCIDescriptorFree() is not necessary.
func (cur *Cursor) variableDefineHelper(param *C.OCIParam, position, numElements uint) (v *Variable, err error) {
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
		if CTrace {
			log.Printf("size of %p[%s] @ %d: %d", param, varType, position, sizeFromOracle)
		}

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
	//log.Printf("varType=%#v size=%d", varType, size)
	v, err = cur.NewVariable(numElements, varType, uint(size))
	if err != nil {
		return nil, errgo.Newf("error creating variable: %s", err)
	}

	// call the procedure to set values prior to define
	if v.typ.preDefine != nil {
		if err = v.typ.preDefine(v, param); err != nil {
			return nil, errgo.Newf("error with preDefine(%s): %s", v, err)
		}
	}

	// perform the define
	//log.Printf("v=%#v", v)
	indic, aL, rC := v.aLrC()
	if CTrace {
		ctrace("OCIDefineByPos(cur=%p, defineHandle=%p, env=%p, position=%d, dataArr=%v, bufferSize=%d, oracleType=%d indicator=%v, aL=%v rC=%v, DEFAULT)",
			cur.handle, &v.defineHandle, v.environment.errorHandle,
			position, v.getDataArr(), v.bufferSize, v.typ.oracleType, v.indicator,
			aL, rC)
	}
	if err = cur.environment.CheckStatus(
		C.OCIDefineByPos(cur.handle,
			&v.defineHandle,
			v.environment.errorHandle, C.ub4(position), v.getDataArr(),
			C.sb4(v.bufferSize), v.typ.oracleType,
			indic, aL, rC, C.OCI_DEFAULT),
		"define"); err != nil {
		return nil, errgo.Newf("error defining: %s", err)
	}

	// call the procedure to set values after define
	if v.typ.postDefine != nil {
		if err = v.typ.postDefine(v); err != nil {
			return nil, errgo.Newf("error with postDefine(%s): %s", v, err)
		}
	}

	return v, nil
}

// Allocate a variable and define it for the given statement.
func (cur *Cursor) varDefine(numElements, position uint) (*Variable, error) {
	var param *C.OCIParam
	// retrieve parameter descriptor
	if cur.handle == nil {
		log.Printf("WARN: nil cursor handle in varDefine!")
	}
	// log.Printf("retrieve parameter descriptor cur.handle=%s pos=%d", cur.handle, position)
	if CTrace {
		ctrace("OCIParamGet(cur=%p, HTYPE_STMT, env=%p, param=%p, position=%d)",
			cur.handle, "HTYPE_STMT", cur.environment.errorHandle,
			&param, position)
	}
	if err := cur.environment.CheckStatus(
		C.OCIParamGet(unsafe.Pointer(cur.handle), C.OCI_HTYPE_STMT,
			cur.environment.errorHandle,
			(*unsafe.Pointer)(unsafe.Pointer(&param)), C.ub4(position)),
		"parameter"); err != nil {
		log.Printf("NO PARAM! %s", err)
		return nil, err
	}
	// log.Printf("got param handle")

	// call the helper to do the actual work
	v, err := cur.variableDefineHelper(param, position, numElements)
	// log.Printf("variable defined err=%s nil?%s", err, err == nil)
	if CTrace {
		ctrace("OCIDescriptorFree(%p, DTYPE_PARAM)", param)
	}
	C.OCIDescriptorFree(unsafe.Pointer(param), C.OCI_DTYPE_PARAM)
	return v, err
}

// Allocate a variable and bind it to the given statement.
func (v *Variable) internalBind() (err error) {
	if CTrace {
		ctrace("%s.internalBind", v)
	}

	var status C.sword
	// perform the bind
	indic, aL, rC := v.aLrC()
	allElts := C.ub4(0)
	pActElts := &v.actualElements
	if v.isArray {
		allElts = C.ub4(v.allocatedElements)
	} else {
		pActElts = nil
	}
	//log.Printf("%v isArray? %b allElts=%d", v.typ.Name, v.isArray, allElts)
	var bindName string
	var bufSlice interface{}
	var m int
	if CTrace {
		m = int(v.bufferSize)
		if m > 40 {
			m = 40
		}
		switch {
		case v.dataInts != nil:
			if m > len(v.dataInts) {
				m = len(v.dataInts)
			}
			bufSlice = v.dataInts[:m]
		case v.dataFloats != nil:
			if m > len(v.dataFloats) {
				m = len(v.dataFloats)
			}
			bufSlice = v.dataFloats[:m]
		default:
			if m > len(v.dataBytes) {
				m = len(v.dataBytes)
			}
			bufSlice = v.dataBytes[:m]
		}
	}
	if v.boundName != "" {
		bname := []byte(v.boundName)
		if CTrace {
			ctrace("internalBind.OCIBindByName(cur=%p, bind=%p, env=%p, name=%q, bufferSize=%d, oracleType=%d, data[:%d]=%v, indicator=%v, aL=%v, rC=%v, allElts=%v, pActElts=%v, DEFAULT)",
				v.boundCursorHandle, &v.bindHandle,
				v.environment.errorHandle, bname,
				v.bufferSize, v.typ.oracleType, m, bufSlice,
				v.indicator, aL, rC,
				allElts, pActElts)
		}
		bindName = fmt.Sprintf("%q", bname)
		status = C.OCIBindByName(v.boundCursorHandle,
			&v.bindHandle,
			v.environment.errorHandle,
			(*C.OraText)(&bname[0]), C.sb4(len(bname)),
			v.getDataArr(), C.sb4(v.bufferSize), v.typ.oracleType,
			indic, aL, rC,
			allElts, pActElts, C.OCI_DEFAULT)
	} else {
		if CTrace {
			m := int(v.bufferSize)
			if m > 40 {
				m = 40
			}
			switch {
			case v.dataInts != nil:
				if m > len(v.dataInts) {
					m = len(v.dataInts)
				}
				bufSlice = v.dataInts[:m]
			case v.dataFloats != nil:
				if m > len(v.dataFloats) {
					m = len(v.dataFloats)
				}
				bufSlice = v.dataFloats[:m]
			default:
				if m > len(v.dataBytes) {
					m = len(v.dataBytes)
				}
				bufSlice = v.dataBytes[:m]
			}
			ctrace("internalBind.OCIBindByPos(cur=%p, boundPos=%d, data[:%d]=%v, bufSize=%d, oracleType=%d, indicator=%v, actLen=%v, rc=%p, allElts=%p pActElts=%p)",
				v.boundCursorHandle, v.boundPos, m, bufSlice,
				v.bufferSize, v.typ.oracleType, v.indicator, aL, rC,
				allElts, pActElts)
		}
		bindName = fmt.Sprintf("%d", v.boundPos)
		status = C.OCIBindByPos(v.boundCursorHandle, &v.bindHandle,
			v.environment.errorHandle, C.ub4(v.boundPos), v.getDataArr(),
			C.sb4(v.bufferSize), v.typ.oracleType,
			indic, aL, rC,
			allElts, pActElts, C.OCI_DEFAULT)
	}
	if err = v.environment.CheckStatus(status, fmt.Sprintf("BindBy(%s)", bindName)); err != nil {
		err = errgo.Mask(err)
		return
	}

	if v.typ.charsetForm != C.SQLCS_IMPLICIT {
		if err = v.environment.AttrSet(
			unsafe.Pointer(v.bindHandle), C.OCI_HTYPE_BIND,
			C.OCI_ATTR_CHARSET_FORM, unsafe.Pointer(&v.typ.size),
			C.sizeof_ub4); err != nil {
			err = errgo.Mask(err)
			return
		}
		// why do we set this here?
		if err = v.environment.AttrSet(
			unsafe.Pointer(v.bindHandle), C.OCI_HTYPE_BIND,
			C.OCI_ATTR_MAXDATA_SIZE, unsafe.Pointer(&v.bufferSize),
			C.sizeof_ub4); err != nil {
			err = errgo.Mask(err)
			return
		}
	}

	// set the max data size for strings
	if (v.typ == StringVarType || v.typ == FixedCharVarType) &&
		v.size > v.typ.size {
		err = v.environment.AttrSet(
			unsafe.Pointer(v.bindHandle), C.OCI_HTYPE_BIND,
			C.OCI_ATTR_MAXDATA_SIZE, unsafe.Pointer(&v.typ.size),
			C.sizeof_ub4)
	}
	if err != nil {
		err = errgo.Mask(err)
	}

	return
}

// unbind undoes the binding
func (v *Variable) unbind() {
	if v.bindHandle == nil {
		return
	}
	v.boundCursorHandle = nil
	v.boundPos = 0
	v.boundName = ""
}

// Bind allocates a variable and bind it to the given statement.
// bind to name or pos
func (v *Variable) Bind(cur *Cursor, name string, pos uint) error {
	if CTrace {
		ctrace("%s.Bind(%s, %s, %d)", v, cur, name, pos)
	}

	// nothing to do if already bound
	if v.boundCursorHandle == cur.handle && v.bindHandle != nil &&
		name == v.boundName && pos == v.boundPos {
		if CTrace {
			ctrace("already bound!")
		}
		return nil
	}
	if cur == nil {
		log.Fatalf("cur is nil")
	}
	if cur.handle == nil {
		log.Fatalf("cur=%v handle is nil", cur)
	}

	// set the instance variables specific for binding
	v.boundPos = pos
	v.boundCursorHandle = cur.handle
	v.boundName = name

	// perform the bind
	return errgo.Mask(v.internalBind())
}

// verifyFetch verifies that truncation or other problems did not take place on retrieve.
func (v *Variable) verifyFetch(arrayPos uint) error {
	if CTrace {
		ctrace("%s.verifyFetch(%d) varlength? %t", v, arrayPos, v.typ.isVariableLength)
	}

	if v.typ.isVariableLength {
		if code := v.returnCode[arrayPos]; code != 0 {
			err := NewErrorAt(
				int(code),
				fmt.Sprintf("column at array pos %d fetched with error: %d)", arrayPos, code),
				"verifyFetch")
			return errgo.Mask(err, errgo.Any)
		}
	}
	return nil
}

// getSingleValue returns the value of the variable at the given position.
func (v *Variable) getSingleValue(arrayPos uint) (interface{}, error) {
	if CTrace {
		ctrace("%s.getSingleValue(%d)", v, arrayPos)
	}
	var isNull bool

	// ensure we do not exceed the number of allocated elements
	if arrayPos >= v.allocatedElements {
		return nil, errgo.New("Variable_GetSingleValue: array size exceeded")
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
		return nil, errgo.Mask(err)
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

// getSingleValueInto inserts the value of the variable at the given position into the pointer
func (v *Variable) getSingleValueInto(dest *interface{}, arrayPos uint) error {
	var isNull bool

	// ensure we do not exceed the number of allocated elements
	if arrayPos >= v.allocatedElements {
		return errgo.New("Variable_GetSingleValue: array size exceeded")
	}

	// check for a NULL value
	if v.typ.isNull != nil {
		isNull = v.typ.isNull(v, arrayPos)
	} else {
		isNull = v.indicator[arrayPos] == C.OCI_IND_NULL
	}
	if isNull {
		switch v.typ {
		case DateTimeVarType:
			*dest = time.Time{}
		case IntervalVarType:
			*dest = time.Duration(0)
		case NumberAsStringVarType, LongIntegerVarType, Int64VarType, Int32VarType, FloatVarType, NativeFloatVarType:
			*dest = 0
		case BooleanVarType:
			*dest = false
		default:
			*dest = ""
		}
		return nil
	}

	// check for truncation or other problems on retrieve
	if err := v.verifyFetch(arrayPos); err != nil {
		return errgo.Mask(err)
	}

	// calculate value to return
	err := v.typ.getValueInto(dest, v, arrayPos)
	if err != nil {
		log.Printf("%s.getSingleValueInto dest=%T(%+v) err=%s", v.typ, *dest, *dest, errgo.Details(err))
		return errgo.Notef(err, "dest=%T(%#v)", *dest, *dest)
	}
	return nil
}

// getArrayValue returns the value of the variable as an array.
func (v *Variable) getArrayValue(numElements uint) (interface{}, error) {
	value := make([]interface{}, numElements)

	for i := uint(0); i < numElements; i++ {
		singleValue, err := v.getSingleValue(i)
		if err != nil {
			return nil, errgo.Mask(err)
		}
		value[i] = singleValue
	}

	return value, nil
}

// getArrayValueInto inserts the value of the variable as an array into the given pointer
func (v *Variable) getArrayValueInto(dest interface{}, numElements uint) error {
	var valp *[]interface{}
	valp, ok := dest.(*[]interface{})
	if !ok {
		val, ok := dest.([]interface{})
		if !ok {
			return errgo.Newf("getArrayValueInto requires *[]interface{}, got %T", dest)
		}
		valp = &val
	}
	*valp = (*valp)[:numElements]
	if missnum := numElements - uint(cap(*valp)); missnum > 0 {
		*valp = append(*valp, make([]interface{}, missnum)...)
	}

	for i := uint(0); i < numElements; i++ {
		err := v.getSingleValueInto(&(*valp)[i], i)
		if err != nil {
			return errgo.Mask(err)
		}
	}

	return nil
}

// Len returns the array's actual length
func (v *Variable) Len() int {
	return int(v.actualElements)
}

// GetValue returns the value of the variable.
func (v *Variable) GetValue(arrayPos uint) (interface{}, error) {
	if CTrace {
		ctrace("%s.GetValue(%d)", v, arrayPos)
	}
	//log.Printf("GetValue isArray? %b", v.isArray)
	//if v.isArray {
	//	return v.getArrayValue(uint(v.actualElements))
	//}
	val, err := v.getSingleValue(arrayPos)
	return val, errgo.Mask(err)
}

// GetValueInto inserts the value of the variable into the given pointer
func (v *Variable) GetValueInto(dest *interface{}, arrayPos uint) error {
	//if v.isArray {
	//	return v.getArrayValueInto(dest, uint(v.actualElements))
	//}
	return errgo.Mask(v.getSingleValueInto(dest, arrayPos))
}

// setSingleValue sets a single value in the variable.
func (v *Variable) setSingleValue(arrayPos uint, value interface{}) error {
	// ensure we do not exceed the number of allocated elements
	if arrayPos >= v.allocatedElements {
		return errgo.New("Variable_SetSingleValue: array size exceeded")
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
	return errgo.Mask(v.typ.setValue(v, arrayPos, value))
}

// setArrayValue sets all of the array values for the variable.
func (v *Variable) setArrayValue(value []interface{}) error {
	// ensure we haven't exceeded the number of allocated elements
	numElements := uint(len(value))
	if numElements > v.allocatedElements {
		return errgo.New("Variable_SetArrayValue: array size exceeded")
	}

	// set all of the values
	v.actualElements = C.ub4(numElements)
	var err error
	for i, elt := range value {
		if err = v.setSingleValue(uint(i), elt); err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

// setArrayReflectValue sets all of the array values for the variable, using reflection
func (v *Variable) setArrayReflectValue(value reflect.Value) error {
	if value.Kind() != reflect.Slice {
		return errgo.Newf("Variable_setArrayReflectValue needs slice, not %s!", value.Kind())
	}
	numElements := uint(value.Len())
	if numElements > v.allocatedElements {
		return errgo.New("Variable_setArrayReflectValue: array size exceeded")
	}

	// set all of the values
	v.actualElements = C.ub4(numElements)
	var err error
	for i := uint(0); i < numElements; i++ {
		//for i, elt := range value {
		if err = v.setSingleValue(i,
			value.Index(int(i)).Interface()); err != nil {
			return errgo.Mask(err)
		}
	}
	return nil
}

// SetValue sets the value of the variable.
// If value is a pointer, then v will put its gained data into it.
func (v *Variable) SetValue(arrayPos uint, value interface{}) error {
	if x, ok := value.(*Variable); ok && x != nil {
		*v = *x
		return nil
	}
	if v.isArray && arrayPos > 0 {
		return errgo.New("arrays of arrays are not supported by the OCI")
	}
	rval := reflect.ValueOf(value)
	if rval.Kind() == reflect.Ptr && !rval.IsNil() {
		v.destination = rval
		rval = rval.Elem()
		if rval.IsValid() {
			value = rval.Interface()
		}
	} else if v.destination.IsValid() {
		v.destination = reflect.ValueOf(nil)
	}
	if v.isArray {
		//if reflect.TypeOf(value).Kind == reflect.Slice {
		if x, ok := value.([]interface{}); ok {
			return v.setArrayValue(x)
		}
		return v.setArrayReflectValue(rval)
		//return fmt.Errorf("%v is %T, not array!", value, value)
	}
	debug("calling %s.setValue(%d, %v (%T))", v.typ, arrayPos, value, value)
	return v.setSingleValue(arrayPos, value)
}

// getPtrValues calls GetValue for each variable which has a proper (pointer) destination
func (cur *Cursor) getPtrValues() error {
	debug("getPtrValues %v %v", cur.bindVarsArr, cur.bindVarsMap)
	for _, v := range cur.bindVarsArr {
		if v.destination.IsValid() && !v.isArray {
			val, err := v.GetValue(0)
			debug("%s setting %v to %v %v", v, v.destination, val, err)
			if err != nil {
				return errgo.Notef(err, "error getting value of %s", v)
			}
			if val == nil {
				v.destination.Elem().Set(reflect.Zero(v.destination.Elem().Type()))
			} else {
				v.destination.Elem().Set(reflect.ValueOf(val))
			}
		}
	}
	for k, v := range cur.bindVarsMap {
		if v.destination.IsValid() && !v.isArray {
			val, err := v.GetValue(0)
			debug("%s setting %v to %v %v", v, v.destination, val, err)
			if err != nil {
				return errgo.Notef(err, "error getting value of %s(%s)", k, v)
			}
			if val == nil {
				v.destination.Elem().Set(reflect.Zero(v.destination.Elem().Type()))
			} else {
				v.destination.Elem().Set(reflect.ValueOf(val))
			}
		}
	}
	return nil
}

// externalCopy the contents of the source variable to the destination variable.
func (targetVar *Variable) externalCopy(sourceVar *Variable, sourcePos, targetPos uint) error {
	if !sourceVar.typ.canBeCopied {
		return errgo.New("variable does not support copying")
	}

	// ensure array positions are not violated
	if sourcePos >= sourceVar.allocatedElements {
		return errgo.New("Variable_ExternalCopy: source array size exceeded")
	}
	if targetPos >= targetVar.allocatedElements {
		return errgo.New("Variable_ExternalCopy: target array size exceeded")
	}

	// ensure target can support amount data from the source
	if targetVar.bufferSize < sourceVar.bufferSize {
		return errgo.New("target variable has insufficient space to copy source data")
	}

	// handle null case directly
	if sourceVar.indicator[sourcePos] == C.OCI_IND_NULL {
		targetVar.indicator[targetPos] = C.OCI_IND_NULL
	} else { // otherwise, copy data
		targetVar.indicator[targetPos] = C.OCI_IND_NOTNULL
		var err error
		if err = sourceVar.verifyFetch(sourcePos); err != nil {
			return errgo.Mask(err)
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
		case len(sourceVar.dataFloats) > 0:
			copy(targetVar.dataFloats[dp:], sourceVar.dataFloats[sp:sq])
		case len(sourceVar.dataInts) > 0:
			copy(targetVar.dataInts[dp:], sourceVar.dataInts[sp:sq])
		default:
			copy(targetVar.dataBytes[dp:], sourceVar.dataBytes[sp:sq])
		}
		return nil
	}

	return nil
}

// externalSetValue the value of the variable at the given position.
func (v *Variable) externalSetValue(pos uint, value interface{}) error {
	return v.SetValue(pos, value)
}

// externalGetValue returns the value of the variable at the given position.
func (v *Variable) externalGetValue(pos uint) (interface{}, error) {
	if CTrace {
		ctrace("%s.externalGetValue(%d)", v, pos)
	}
	return v.GetValue(pos)
}

func (v Variable) getHandle(pos uint) unsafe.Pointer {
	return unsafe.Pointer(&v.dataBytes[int(pos*v.typ.size)])
}
func (v Variable) setHandle(pos uint, val unsafe.Pointer) {
	//void* memcpy( void *dest, const void *src, size_t count );
	C.memcpy(unsafe.Pointer(&v.dataBytes[int(pos*v.typ.size)]),
		unsafe.Pointer(&val),
		C.size_t(v.typ.size))
}
func (v Variable) getHandleBytes(pos uint) []byte {
	return v.dataBytes[int(pos*v.typ.size):int((pos+1)*v.typ.size)]
}

func maxUint(a, b uint) uint {
	if a > b {
		return a
	}
	return b
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
