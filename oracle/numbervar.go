package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	// "log"
	// "bytes"
	// "encoding/binary"
	"fmt"
	"unsafe"
)

var (
	FloatVarType, NativeFloatVarType               *VariableType
	Int32VarType, Int64VarType, LongIntegerVarType *VariableType
	NumberAsStringVarType, BooleanVarType          *VariableType
)

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

func (t *VariableType) IsInteger() bool {
	switch t {
	case BooleanVarType, LongIntegerVarType, Int64VarType, Int32VarType:
		return true
	}
	return false
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
	// log.Printf("numberVar_PreDefine typ=%s scale=%d precision=%d", v.typ,
	// 	scale, precision)
	if v.typ == nil {
		v.typ = FloatVarType
	}
	if v.typ == FloatVarType {
		if precision > 0 && (scale == -127 || scale == 0) { // int
			if precision > 0 && precision < 10 {
				v.typ = Int32VarType
			} else if precision > 0 && precision < 19 {
				v.typ = Int64VarType
			} else {
				v.typ = LongIntegerVarType
			}
		}
	}
	// log.Printf("numberVar_PreDefine ok")

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

// Set the value of the variable.
func numberVar_SetValue(v *Variable, pos uint, value interface{}) error {
	debug("numberVar_SetValue(typ=%s, pos=%d len=(%d), value=%+v (%T))", v.typ,
		pos, len(v.dataBytes), value, value)
	nfInt := func(intVal int64) error {
		if v.dataInts != nil {
			v.dataInts[pos] = intVal
			return nil
		}
		return v.environment.numberFromInt(intVal,
			unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCINumber]))
	}
	nfFloat := func(floatVal float64) error {
		if v.dataFloats != nil {
			v.dataFloats[pos] = floatVal
			return nil
		}
		return v.environment.numberFromFloat(floatVal,
			unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCINumber]))
	}
	var err error
	switch x := value.(type) {
	case bool:
		if x {
			return nfInt(1)
		}
		return nfInt(0)
	case int16:
		return nfInt(int64(x))
	case uint16:
		return nfInt(int64(x))
	case int32:
		return nfInt(int64(x))
	case []int32:
		for i := range x {
			if err = numberVar_SetValue(v, pos+uint(i), x[i]); err != nil {
				return err
			}
		}
		return err

	case int:
		return nfInt(int64(x))
	case uint:
		return nfInt(int64(x))
	case uint32:
		return nfInt(int64(x))
	case uint64:
		return nfInt(int64(x))
	case int64:
		return nfInt(int64(x))
	case []int64:
		for i := range x {
			if err = numberVar_SetValue(v, pos+uint(i), x[i]); err != nil {
				return err
			}
		}
		return err

	case float32:
		return nfFloat(float64(x))
	case []float32:
		for i := range x {
			if err = numberVar_SetValue(v, pos+uint(i), x[i]); err != nil {
				return err
			}
		}
		return err

	case float64:
		return nfFloat(x)
	case []float64:
		for i := range x {
			if err = numberVar_SetValue(v, pos+uint(i), x[i]); err != nil {
				return err
			}
		}
		return err

	case string:
		return v.environment.numberFromText(x,
			unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCINumber]))
	case []string:
		for i := range x {
			if err = numberVar_SetValue(v, pos+uint(i), x[i]); err != nil {
				return err
			}
		}
		return err

	default:
		if x, ok := value.(fmt.Stringer); ok {
			return v.environment.numberFromText(x.String(),
				unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCINumber]))
		}
		return fmt.Errorf("required some kind of int, got %T", value)
	}
	return nil
}

// Returns the value stored at the given array position.
func numberVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	if v.dataFloats != nil {
		// log.Printf("getting pos=%d from %+v", pos, v.dataFloats)
		return v.dataFloats[pos], nil
	}
	if v.dataInts != nil {
		// log.Printf("getting pos=%d from %+v", pos, v.dataInts)
		return v.dataInts[pos], nil
	}
	if v.typ == NumberAsStringVarType {
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
	}
	// log.Printf("v=%s IsInteger?%s", v.typ, v.typ.IsInteger())
	if v.typ.IsInteger() {
		intVal := int64(0)
		if err := v.environment.CheckStatus(
			C.OCINumberToInt(v.environment.errorHandle,
				(*C.OCINumber)(unsafe.Pointer(&v.dataBytes[pos*v.size])),
				C.sizeof_long, C.OCI_NUMBER_SIGNED, unsafe.Pointer(&intVal)),
			"numberToInt"); err != nil {
			return -1, err
		}
		if v.typ == BooleanVarType {
			return intVal > 0, nil
		}
		return intVal, nil
	}

	floatVal := float64(0)
	err := v.environment.CheckStatus(
		C.OCINumberToReal(v.environment.errorHandle,
			(*C.OCINumber)(unsafe.Pointer(&v.dataBytes[pos*v.size])),
			C.sizeof_double, unsafe.Pointer(&floatVal)),
		"numberToFloat")
	return floatVal, err
}

func init() {
	FloatVarType = &VariableType{
		Name:             "Float",
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
		Name:             "NativeFloat",
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
		Name:         "Int32",
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
		Name:         "Int64",
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
		Name:         "LongInteger",
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
		Name:         "NumberAsString",
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
		Name:         "Boolean",
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
