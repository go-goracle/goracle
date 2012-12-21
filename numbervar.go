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
	"log"
	// "bytes"
	// "encoding/binary"
	"fmt"
	"unsafe"
)

type Stringer interface {
	String() string
}

var (
	FloatVarType, NativeFloatVarType               *VariableType
	Int32VarType, Int64VarType, LongIntegerVarType *VariableType
	NumberAsStringVarType, BooleanVarType          *VariableType
)

func init() {
	FloatVarType = &VariableType{
		preDefine:        numberVar_PreDefine,
		setValue:         numberVar_SetValue,
		getValue:         numberVar_GetValue,
		oracleType:       C.SQLT_VNU,         // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,   // charset form
		size:             C.sizeof_OCINumber, // element length
		isCharData:       false,              // is character data
		isVariableLength: false,              // is variable length
		canBeCopied:      true,               // can be copied
		canBeInArray:     true,               // can be in array
	}

	NativeFloatVarType = &VariableType{
		setValue:         numberVar_SetValue,
		getValue:         numberVar_GetValue,
		oracleType:       C.SQLT_BDOUBLE,   // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             C.sizeof_double,  // element length
		isCharData:       false,            // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	Int32VarType = &VariableType{
		preDefine:    numberVar_PreDefine,
		setValue:     numberVar_SetValue,
		getValue:     numberVar_GetValue,
		oracleType:   C.SQLT_VNU,         // Oracle type
		charsetForm:  C.SQLCS_IMPLICIT,   // charset form
		size:         C.sizeof_OCINumber, // element length
		canBeCopied:  true,               // can be copied
		canBeInArray: true,               // can be in array
	}

	Int64VarType = &VariableType{
		preDefine:    numberVar_PreDefine,
		setValue:     numberVar_SetValue,
		getValue:     numberVar_GetValue,
		oracleType:   C.SQLT_VNU,         // Oracle type
		charsetForm:  C.SQLCS_IMPLICIT,   // charset form
		size:         C.sizeof_OCINumber, // element length
		canBeCopied:  true,               // can be copied
		canBeInArray: true,               // can be in array
	}

	LongIntegerVarType = &VariableType{
		preDefine:    numberVar_PreDefine,
		setValue:     numberVar_SetValue,
		getValue:     numberVar_GetValue,
		oracleType:   C.SQLT_VNU,         // Oracle type
		charsetForm:  C.SQLCS_IMPLICIT,   // charset form
		size:         C.sizeof_OCINumber, // element length
		canBeCopied:  true,               // can be copied
		canBeInArray: true,               // can be in array
	}

	NumberAsStringVarType = &VariableType{
		preDefine:    numberVar_PreDefine,
		setValue:     numberVar_SetValue,
		getValue:     numberVar_GetValue,
		oracleType:   C.SQLT_VNU,         // Oracle type
		charsetForm:  C.SQLCS_IMPLICIT,   // charset form
		size:         C.sizeof_OCINumber, // element length
		canBeCopied:  true,               // can be copied
		canBeInArray: true,               // can be in array
	}

	BooleanVarType = &VariableType{
		preDefine:    numberVar_PreDefine,
		setValue:     numberVar_SetValue,
		getValue:     numberVar_GetValue,
		oracleType:   C.SQLT_VNU,         // Oracle type
		charsetForm:  C.SQLCS_IMPLICIT,   // charset form
		size:         C.sizeof_OCINumber, // element length
		canBeCopied:  true,               // can be copied
		canBeInArray: true,               // can be in array
	}
}

func (t *VariableType) IsNumber() bool {
	switch t {
	case BooleanVarType, NumberAsStringVarType, LongIntegerVarType, Int64VarType, Int32VarType, FloatVarType, NativeFloatVarType:
		return true
	}
	return false
}

func (t *VariableType) IsFloat() bool {
	return t == NativeFloatVarType || t == FloatVarType
}

// Set the type of value (integer, float or string) that will be returned
// when values are fetched from this variable.
func numberVar_PreDefine(v *Variable, param *C.OCIParam) error {
	var precision C.sb2
	var scale C.sb1

	// if the return type has not already been specified, check to see if the
	// number can fit inside an integer by looking at the precision and scale
	if _, err := v.environment.AttrGet(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_SCALE, unsafe.Pointer(&scale),
		"numberVar_PreDefine: scale"); err != nil {
		return err
	}
	if _, err := v.environment.AttrGet(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_PRECISION, unsafe.Pointer(&precision),
		"numberVar_PreDefine(): precision"); err != nil {
		return err
	}
	log.Printf("numberVar_PreDefine typ=%s", v.typ)
	if v.typ == nil || v.typ == FloatVarType || v.typ == LongIntegerVarType {
		if scale == 0 || (scale == -127 && precision == 0) {
			if precision > 0 && precision < 10 {
				v.typ = Int32VarType
			} else if precision > 0 && precision < 19 {
				v.typ = Int64VarType
			} else {
				v.typ = LongIntegerVarType
			}
		}
	}
	log.Printf("numberVar_PreDefine ok")

	return nil
}

func (env *Environment) numberFromInt(value int64, dst unsafe.Pointer) error {
	return env.CheckStatus(
		C.OCINumberFromInt(env.errorHandle, unsafe.Pointer(&value),
			C.uword(unsafe.Sizeof(value)), C.OCI_NUMBER_SIGNED,
			(*C.OCINumber)(dst)),
		"numberFromInt")
}

func (env *Environment) numberFromFloat(value float64, dst unsafe.Pointer) error {
	return env.CheckStatus(
		C.OCINumberFromReal(env.errorHandle, unsafe.Pointer(&value),
			C.uword(unsafe.Sizeof(value)), (*C.OCINumber)(dst)),
		"numberFromReal")
}

func numberVar_formatForString(text string) string {
	format := make([]byte, len(text))
	rational := false
	if text[0] == '-' {
		format[0] = '-'
	}
	for i := 1; i < len(text); i++ {
		if !rational && text[i] == '.' {
			format[i] = '.'
			rational = true
		}
		format[i] = '9'
	}
	return string(format)
}

// Set the value of the variable from a Python decimal.Decimal object.
func (env *Environment) numberFromText(value string, dst unsafe.Pointer) error {
	valuebuf := []byte(value)
	formatbuf := []byte(numberVar_formatForString(value))
	return env.CheckStatus(
		C.OCINumberFromText(env.errorHandle,
			(*C.oratext)(&valuebuf[0]), C.ub4(len(valuebuf)),
			(*C.oratext)(&formatbuf[0]), C.ub4(len(formatbuf)),
			(*C.oratext)(&env.nlsNumericCharactersBuffer[0]),
			C.ub4(len(env.nlsNumericCharactersBuffer)),
			(*C.OCINumber)(dst)),
		"numberFromText")
}

func (env *Environment) numberToFloat(src unsafe.Pointer) (float64, error) {
	var floatVal float64
	err := env.CheckStatus(
		C.OCINumberToReal(env.errorHandle, (*C.OCINumber)(src), C.sizeof_double,
			unsafe.Pointer(&floatVal)),
		"numberToFloat")
	return floatVal, err
}

// Set the value of the variable.
func numberVar_SetValue(v *Variable, pos uint, value interface{}) error {
	var (
		intval int64
	)
	switch x := value.(type) {
	case bool:
		if x {
			intval = 1
		}
		return v.environment.numberFromInt(intval,
			unsafe.Pointer(&v.dataInts[pos]))
	case int, int32, uint, uint32, uint64:
		intval = int64(x.(int64))
		return v.environment.numberFromInt(intval,
			unsafe.Pointer(&v.dataInts[pos]))
	case float32, float64:
		return v.environment.numberFromFloat(x.(float64),
			unsafe.Pointer(&v.dataFloats[pos]))
	case string:
		return v.environment.numberFromText(x,
			unsafe.Pointer(&v.dataBytes[pos]))
	default:
		if x, ok := value.(Stringer); ok {
			return v.environment.numberFromText(x.String(),
				unsafe.Pointer(&v.dataBytes[pos]))
		}
		return fmt.Errorf("required some kind of int, got %T", value)
	}
	return nil
}

// Returns the value stored at the given array position.
func numberVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	switch v.typ {
	case Int32VarType, Int64VarType, BooleanVarType:
		intVal := int64(0)
		if err := v.environment.CheckStatus(
			C.OCINumberToInt(v.environment.errorHandle,
				(*C.OCINumber)(unsafe.Pointer(&v.dataInts[pos])),
				C.sizeof_long, C.OCI_NUMBER_SIGNED, unsafe.Pointer(&intVal)),
			"numberToInt"); err != nil {
			return -1, err
		}
		if v.typ == BooleanVarType {
			return intVal > 0, nil
		}
		return intVal, nil
	case NumberAsStringVarType:
		buf := make([]byte, 200)
		var size C.ub4
		if err := v.environment.CheckStatus(
			C.OCINumberToText(v.environment.errorHandle,
				(*C.OCINumber)(unsafe.Pointer(&v.dataBytes[pos])),
				(*C.oratext)(unsafe.Pointer(&v.environment.numberToStringFormatBuffer[0])),
				C.ub4(len(v.environment.numberToStringFormatBuffer)), nil, 0,
				&size, (*C.oratext)(&buf[0])),
			"NumberToText"); err != nil {
			return 0, err
		}
		return v.environment.FromEncodedString(buf[:int(size)]), nil
	case NativeFloatVarType:
		// var floatVal float64
		// if err := binary.Read(bytes.NewReader(v.data[pos:pos+C.sizeof_double]),
		// 	binary.LittleEndian, &floatVal); err != nil {
		// 	return nil, err
		// }
		// return floatVal, nil
		return v.dataFloats[pos], nil
	}

	return v.environment.numberToFloat(unsafe.Pointer(&v.dataBytes[pos]))
}
