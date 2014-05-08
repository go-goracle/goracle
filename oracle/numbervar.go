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
*/
import "C"

import (
	"fmt"
	"log"
	"math/big"
	"strings"
	"unsafe"

	"github.com/juju/errgo"
)

var (
	//FloatVarType is a VariableType for floats
	FloatVarType *VariableType
	//NativeFloatVarType is a VariableType for native floatss
	NativeFloatVarType *VariableType
	//Int32VarType is a VariableType for int32s
	Int32VarType *VariableType
	//Int64VarType is a VariableType for int64s
	Int64VarType *VariableType
	//LongIntegerVarType is a VariableType for long ints
	LongIntegerVarType *VariableType
	//NumberAsStringVarType is a VariableType for numbers represented as strings
	NumberAsStringVarType *VariableType
	//BooleanVarType is a VariableType for bools
	BooleanVarType *VariableType
)

// IsNumber returns whether the VariableType represents a number
func (t *VariableType) IsNumber() bool {
	switch t {
	case BooleanVarType, NumberAsStringVarType, LongIntegerVarType, Int64VarType, Int32VarType, FloatVarType, NativeFloatVarType:
		return true
	}
	return false
}

// IsFloat returns whether the VariableType represents a float
func (t *VariableType) IsFloat() bool {
	return t == NativeFloatVarType || t == FloatVarType
}

// IsInteger returns whether the VariableType represents an integer
func (t *VariableType) IsInteger() bool {
	switch t {
	case BooleanVarType, LongIntegerVarType, Int64VarType, Int32VarType:
		return true
	}
	return false
}

// Set the type of value (integer, float or string) that will be returned
// when values are fetched from this variable.
func numberVarPreDefine(v *Variable, param *C.OCIParam) error {
	var precision C.sb2
	var scale C.sb1

	// if the return type has not already been specified, check to see if the
	// number can fit inside an integer by looking at the precision and scale
	if _, err := v.environment.AttrGet(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_SCALE, unsafe.Pointer(&scale),
		"numberVarPreDefine: scale"); err != nil {
		return errgo.Mask(err)
	}
	if _, err := v.environment.AttrGet(
		unsafe.Pointer(param), C.OCI_HTYPE_DESCRIBE,
		C.OCI_ATTR_PRECISION, unsafe.Pointer(&precision),
		"numberVarPreDefine(): precision"); err != nil {
		return errgo.Mask(

			// log.Printf("numberVarPreDefine typ=%s scale=%d precision=%d", v.typ,
			// 	scale, precision)
			err)
	}

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
	// log.Printf("numberVarPreDefine ok")

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

func numberVarformatForString(text string) string {
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
	formatbuf := []byte(numberVarformatForString(value))
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
func numberVarSetValue(v *Variable, pos uint, value interface{}) error {
	debug("numberVarSetValue(typ=%s, pos=%d len=(%d), value=%T(%+v))", v.typ,
		pos, len(v.dataBytes), value, value)
	nfInt := func(intVal int64) error {
		if v.dataInts != nil {
			v.dataInts[pos] = intVal
			return nil
		}
		return v.environment.numberFromInt(intVal,
			v.getHandle(pos))
	}
	nfFloat := func(floatVal float64) error {
		if v.dataFloats != nil {
			v.dataFloats[pos] = floatVal
			return nil
		}
		return v.environment.numberFromFloat(floatVal,
			v.getHandle(pos))
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
			if err = numberVarSetValue(v, pos+uint(i), x[i]); err != nil {
				return errgo.Mask(err)
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
			if err = numberVarSetValue(v, pos+uint(i), x[i]); err != nil {
				return errgo.Mask(err)
			}
		}
		return err
	case big.Int:
		return v.environment.numberFromText((&x).String(), v.getHandle(pos))
	case *big.Int:
		return v.environment.numberFromText(x.String(), v.getHandle(pos))

	case float32:
		return nfFloat(float64(x))
	case []float32:
		for i := range x {
			if err = numberVarSetValue(v, pos+uint(i), x[i]); err != nil {
				return errgo.Mask(err)
			}
		}
		return err

	case float64:
		return nfFloat(x)
	case []float64:
		for i := range x {
			if err = numberVarSetValue(v, pos+uint(i), x[i]); err != nil {
				return errgo.Mask(err)
			}
		}
		return err
	case big.Rat:
		return v.environment.numberFromText((&x).FloatString(22), v.getHandle(pos))
	case *big.Rat:
		return v.environment.numberFromText(x.FloatString(22), v.getHandle(pos))

	case string:
		return v.environment.numberFromText(x, v.getHandle(pos))
	case []string:
		for i := range x {
			if err = numberVarSetValue(v, pos+uint(i), x[i]); err != nil {
				return errgo.Mask(err)
			}
		}
		return err

	default:
		if x, ok := value.(fmt.Stringer); ok {
			return v.environment.numberFromText(x.String(),
				v.getHandle(pos))
		}
		return errgo.Newf("required some kind of int, got %T", value)
	}
}

// Returns the value stored at the given array position.
func numberVarGetValue(v *Variable, pos uint) (interface{}, error) {
	if v.dataFloats != nil {
		// log.Printf("getting pos=%d from %+v", pos, v.dataFloats)
		return v.dataFloats[pos], nil
	}
	if v.dataInts != nil {
		// log.Printf("getting pos=%d from %+v", pos, v.dataInts)
		return v.dataInts[pos], nil
	}

	num := (*C.OCINumber)(v.getHandle(pos))
	buf := make([]byte, 64)
	size := C.ub4(len(buf))
	if err := v.environment.CheckStatus(
		C.OCINumberToText(v.environment.errorHandle,
			num,
			(*C.oratext)(unsafe.Pointer(&v.environment.numberToStringFormatBuffer[0])),
			C.ub4(len(v.environment.numberToStringFormatBuffer)), nil, 0,
			&size, (*C.oratext)(&buf[0])),
		"NumberToText",
	); err != nil {
		var floatVal float64
		_ = v.environment.CheckStatus(
			C.OCINumberToReal(v.environment.errorHandle,
				num,
				C.sizeof_double, unsafe.Pointer(&floatVal)),
			"numberToFloat")
		log.Printf("floatVal=%f format=%q len=%d num=%p (%d) size=%p buf=%p",
			floatVal,
			v.environment.numberToStringFormatBuffer,
			len(v.environment.numberToStringFormatBuffer),
			num, *((*byte)(unsafe.Pointer(num))),
			&size, &buf[0])
		return 0, errgo.Notef(err, "want string (%f)", floatVal)
	}
	numS := strings.Replace(
		v.environment.FromEncodedString(buf[:int(size)]),
		",", ".", -1)
	if v.typ == NumberAsStringVarType {
		return numS, nil
	}

	if len(numS) <= 9 && strings.IndexByte(numS, '.') < 0 {
		intVal, err := numAsInt(v, num)
		return intVal, errgo.Mask(err)
	}
	return numS, nil
}

func numAsInt(v *Variable, num *C.OCINumber) (intVal int32, err error) {
	if err = v.environment.CheckStatus(
		C.OCINumberToInt(v.environment.errorHandle,
			//(*C.OCINumber)(v.getHandle(pos)),
			num,
			4, C.OCI_NUMBER_SIGNED, unsafe.Pointer(&intVal)),
		"numberToInt"); err != nil {
		return -1, errgo.Mask(err)
	}
	return
}

func init() {
	FloatVarType = &VariableType{
		Name:       "Float",
		preDefine:  numberVarPreDefine,
		setValue:   numberVarSetValue,
		getValue:   numberVarGetValue,
		oracleType: C.SQLT_VNU,
		//oracleType:       C.SQLT_FLT,         // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,   // charset form
		size:             C.sizeof_OCINumber, // element length
		isCharData:       false,              // is character data
		isVariableLength: false,              // is variable length
		canBeCopied:      true,               // can be copied
		canBeInArray:     true,               // can be in array
	}

	NativeFloatVarType = &VariableType{
		Name:             "NativeFloat",
		setValue:         numberVarSetValue,
		getValue:         numberVarGetValue,
		oracleType:       C.SQLT_BDOUBLE,   // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             C.sizeof_double,  // element length
		isCharData:       false,            // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	Int32VarType = &VariableType{
		Name:       "Int32",
		preDefine:  numberVarPreDefine,
		setValue:   numberVarSetValue,
		getValue:   numberVarGetValue,
		oracleType: C.SQLT_VNU,
		//oracleType:       C.SQLT_INT,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             4,                // element length
		isCharData:       false,            // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	Int64VarType = &VariableType{
		Name:       "Int64",
		preDefine:  numberVarPreDefine,
		setValue:   numberVarSetValue,
		getValue:   numberVarGetValue,
		oracleType: C.SQLT_VNU,
		//oracleType:       C.SQLT_INT,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             8,                // element length
		isCharData:       false,            // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	LongIntegerVarType = &VariableType{
		Name:       "LongInteger",
		preDefine:  numberVarPreDefine,
		setValue:   numberVarSetValue,
		getValue:   numberVarGetValue,
		oracleType: C.SQLT_VNU,
		//oracleType:       C.SQLT_NUM,         // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,   // charset form
		size:             C.sizeof_OCINumber, // element length
		isCharData:       false,              // is character data
		isVariableLength: false,              // is variable length
		canBeCopied:      true,               // can be copied
		canBeInArray:     true,               // can be in array
	}

	NumberAsStringVarType = &VariableType{
		Name:             "NumberAsString",
		preDefine:        numberVarPreDefine,
		setValue:         numberVarSetValue,
		getValue:         numberVarGetValue,
		oracleType:       C.SQLT_VNU,         // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,   // charset form
		size:             C.sizeof_OCINumber, // element length
		isCharData:       false,              // is character data
		isVariableLength: false,              // is variable length
		canBeCopied:      true,               // can be copied
		canBeInArray:     true,               // can be in array
	}

	BooleanVarType = &VariableType{
		Name:             "Boolean",
		preDefine:        numberVarPreDefine,
		setValue:         numberVarSetValue,
		getValue:         numberVarGetValue,
		oracleType:       C.SQLT_VNU,         // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,   // charset form
		size:             C.sizeof_OCINumber, // element length
		isCharData:       false,              // is character data
		isVariableLength: false,              // is variable length
		canBeCopied:      true,               // can be copied
		canBeInArray:     true,               // can be in array
	}
}
