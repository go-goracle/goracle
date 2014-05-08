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
*/
import "C"

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errgo"
)

var (
	//LongStringVarType is a VariableType for LONG strings
	LongStringVarType *VariableType
	//LongBinaryVarType is a VariableType for RAW strings
	LongBinaryVarType *VariableType
)

func init() {
	LongStringVarType = &VariableType{
		Name:             "long",
		setValue:         longVarSetValue,
		getValue:         longVarGetValue,
		getBufferSize:    longVarGetBufferSize,
		oracleType:       C.SQLT_LVC,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             128 * 1024,       // element length (default)
		isCharData:       true,             // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     false,            // can be in array
	}

	LongBinaryVarType = &VariableType{
		Name:             "long_raw",
		setValue:         longVarSetValue,
		getValue:         longVarGetValue,
		getBufferSize:    longVarGetBufferSize,
		oracleType:       C.SQLT_LVB,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             128 * 1024,       // element length (default)
		isCharData:       false,            // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     false,            // can be in array
	}
}

// longVarSetValue sets the value of the variable.
func longVarSetValue(v *Variable, pos uint, value interface{}) error {
	var x []byte
	if y, ok := value.(string); !ok {
		z, ok := value.([]byte)
		if !ok {
			return errgo.Newf("awaited string or []byte, got %T", value)
		}
		x = z
	} else {
		x = []byte(y)
	}
	// verify there is enough space to store the value
	length := uint(len(x) + 4)
	if uint(len(v.dataBytes)) < length {
		if err := v.resize(length); err != nil {
			return errgo.Mask(err)
		}
	}

	p := v.bufferSize * pos
	if err := binary.Write(bytes.NewBuffer(v.dataBytes[p:p+4]),
		binary.LittleEndian, uint32(len(x))); err != nil {
		return errgo.Mask(

			// copy the string to the Oracle buffer
			err)
	}

	copy(v.dataBytes[p+4:p+4+uint(len(x))], x)
	return nil
}

// longVarGetValue returns the value stored at the given array position.
func longVarGetValue(v *Variable, pos uint) (interface{}, error) {
	p := v.bufferSize * pos
	size := uint32(v.bufferSize)
	if err := binary.Read(bytes.NewReader(v.dataBytes[p:p+4]),
		binary.LittleEndian, &size); err != nil {
		return nil, errgo.Mask(err)
	}
	data := v.dataBytes[p+4 : p+4+uint(size)]
	if v.typ == LongStringVarType {
		return string(data), nil
	}
	return data, nil
}

// longVarGetBufferSize returns the size of the buffer to use for data of the given size.
func longVarGetBufferSize(v *Variable) uint {
	if v.typ.isCharData {
		return v.size + C.sizeof_ub4
	}
	return C.sizeof_ub4 + v.size*v.environment.MaxBytesPerCharacter
}
