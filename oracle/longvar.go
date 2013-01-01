package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	// "log"
	// "unsafe"
)

var (
	LongStringVarType, LongBinaryVarType *VariableType
)

func init() {
	LongStringVarType = &VariableType{
		setValue:         longVar_SetValue,
		getValue:         longVar_GetValue,
		getBufferSize:    longVar_GetBufferSize,
		oracleType:       C.SQLT_LVC,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             128 * 1024,       // element length (default)
		isCharData:       true,             // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     false,            // can be in array
	}

	LongBinaryVarType = &VariableType{
		setValue:         longVar_SetValue,
		getValue:         longVar_GetValue,
		getBufferSize:    longVar_GetBufferSize,
		oracleType:       C.SQLT_LVB,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             128 * 1024,       // element length (default)
		isCharData:       false,            // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     false,            // can be in array
	}
}

// Set the value of the variable.
func longVar_SetValue(v *Variable, pos uint, value interface{}) error {
	var x []byte
	if y, ok := value.(string); !ok {
		if y, ok := value.([]byte); !ok {
			return fmt.Errorf("awaited string or []byte, got %T", value)
		} else {
			x = y
		}
	} else {
		x = []byte(y)
	}
	// verify there is enough space to store the value
	length := uint(len(x) + 4)
	if uint(len(v.dataBytes)) < length {
		if err := v.resize(length); err != nil {
			return err
		}
	}

	p := v.bufferSize * pos
	if err := binary.Write(bytes.NewBuffer(v.dataBytes[p:p+4]),
		binary.LittleEndian, uint32(len(x))); err != nil {
		return err
	}

	// copy the string to the Oracle buffer
	copy(v.dataBytes[p+4:p+4+uint(len(x))], x)
	return nil
}

// Returns the value stored at the given array position.
func longVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	p := v.bufferSize * pos
	size := uint32(v.bufferSize)
	if err := binary.Read(bytes.NewReader(v.dataBytes[p:p+4]),
		binary.LittleEndian, &size); err != nil {
		return nil, err
	}
	data := v.dataBytes[p+4 : p+4+uint(size)]
	if v.typ == LongStringVarType {
		return string(data), nil
	}
	return data, nil
}

// Returns the size of the buffer to use for data of the given size.
func longVar_GetBufferSize(v *Variable) uint {
	if v.typ.isCharData {
		return v.size + C.sizeof_ub4
	}
	return C.sizeof_ub4 + v.size*v.environment.MaxBytesPerCharacter
}
