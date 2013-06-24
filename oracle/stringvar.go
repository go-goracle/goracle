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
)

var (
	//StringVarType is a VariableType for VARCHAR2
	StringVarType *VariableType
	//FixedCharVarType is a VariableType for CHAR
	FixedCharVarType *VariableType
	//BinaryVarType is a VariableType for RAW
	BinaryVarType *VariableType
	//RowidVarType is a VariableType for ROWID
	RowidVarType *VariableType
)

// IsString returns whether the VariableType is a string type
func (t *VariableType) IsString() bool {
	if t == StringVarType || t == FixedCharVarType {
		return true
	}
	return false
}

// IsBinary returns whether the VariableType is a binary type
func (t *VariableType) IsBinary() bool {
	if t == BinaryVarType {
		return true
	}
	return false
}

// Initialize the variable.
func stringVarInitialize(v *Variable, cur *Cursor) error {
	v.actualLength = make([]C.ub2, v.allocatedElements)
	return nil
}

// Set the value of the variable.
func stringVarSetValue(v *Variable, pos uint, value interface{}) (err error) {
	var (
		text   string
		buf    []byte
		ok     bool
		length int
	)
	if text, ok = value.(string); !ok {
		if buf, ok = value.([]byte); !ok {
			if arr, ok := value.([]string); ok {
				for i := range arr {
					if err = stringVarSetValue(v, pos+uint(i), arr[i]); err != nil {
						return fmt.Errorf("error setting pos=%d + %d. element: %s", pos, i, err)
					}
				}
				return nil
			} else if arr, ok := value.([]byte); ok {
				for i := range arr {
					if err = stringVarSetValue(v, pos+uint(i), arr[i]); err != nil {
						return fmt.Errorf("error setting pos=%d + %d. element: %s", pos, i, err)
					}
				}
				return nil
			}
			// return fmt.Errorf("string or []byte required, got %T", value)
			log.Panicf("string or []byte required, got %T", value)
		} else {
			if v.typ.isCharData {
				text = string(buf)
				length = len(text)
			} else {
				length = len(buf)
			}
		}
	} else {
		if v.typ.isCharData {
			length = len(text)
		} else {
			length = len(buf)
		}
		buf = []byte(text)
	}
	if v.typ.isCharData && length > MaxStringChars {
		return errors.New("string data too large")
	} else if !v.typ.isCharData && length > MaxBinaryBytes {
		return errors.New("binary data too large")
	}

	// ensure that the buffer is large enough
	if length > int(v.bufferSize) {
		if err := v.resize(uint(length)); err != nil {
			return err
		}
	}

	// keep a copy of the string
	v.actualLength[pos] = C.ub2(length)
	if length > 0 {
		copy(v.dataBytes[int(v.bufferSize*pos):], buf)
	}

	return nil
}

// Returns the value stored at the given array position.
func stringVarGetValue(v *Variable, pos uint) (interface{}, error) {
	buf := v.dataBytes[int(v.bufferSize*pos) : int(v.bufferSize*pos)+int(v.actualLength[pos])]
	//log.Printf("stringVarGetValue(pos=%d length=%d): %v (%s)", pos, v.actualLength[pos], buf, buf)
	if v.typ == BinaryVarType {
		return buf, nil
	}
	return v.environment.FromEncodedString(buf), nil
	/*
		#if PY_MAJOR_VERSION < 3
		    if (var->type == &vt_FixedNationalChar
		            || var->type == &vt_NationalCharString)
		        return PyUnicode_Decode(data, var->actualLength[pos],
		                var->environment->nencoding, NULL);
		#endif
	*/
}

/*
#if PY_MAJOR_VERSION < 3
//-----------------------------------------------------------------------------
// StringVar_PostDefine()
//   Set the character set information when values are fetched from this
// variable.
//-----------------------------------------------------------------------------
static int StringVar_PostDefine(
    udt_StringVar *var)                 // variable to initialize
{
    sword status;

    status = OCIAttrSet(var->defineHandle, OCI_HTYPE_DEFINE,
            &var->type->charsetForm, 0, OCI_ATTR_CHARSET_FORM,
            var->environment->errorHandle);
    if (Environment_CheckForError(var->environment, status,
            "StringVar_PostDefine(): setting charset form") < 0)
        return -1;

    return 0;
}
#endif
*/

// Returns the buffer size to use for the variable.
func stringVarGetBufferSize(v *Variable) uint {
	if v.typ.isCharData {
		return v.size * v.environment.MaxBytesPerCharacter
	}
	return uint(v.size)
}

func init() {
	StringVarType = &VariableType{
		Name:             "String",
		isVariableLength: true,
		initialize:       stringVarInitialize,
		setValue:         stringVarSetValue,
		getValue:         stringVarGetValue,
		getBufferSize:    stringVarGetBufferSize,
		oracleType:       C.SQLT_CHR,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             MaxStringChars,   // element length (default)
		isCharData:       true,             // is character data
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	FixedCharVarType = &VariableType{
		Name:             "FixedChar",
		initialize:       stringVarInitialize,
		setValue:         stringVarSetValue,
		getValue:         stringVarGetValue,
		getBufferSize:    stringVarGetBufferSize,
		oracleType:       C.SQLT_AFC,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             2000,             // element length (default)
		isCharData:       true,             // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	RowidVarType = &VariableType{
		Name:             "Rowid",
		initialize:       stringVarInitialize,
		setValue:         stringVarSetValue,
		getValue:         stringVarGetValue,
		getBufferSize:    stringVarGetBufferSize,
		oracleType:       C.SQLT_CHR,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             18,               // element length (default)
		isCharData:       true,             // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	BinaryVarType = &VariableType{
		Name:             "Binary",
		initialize:       stringVarInitialize,
		setValue:         stringVarSetValue,
		getValue:         stringVarGetValue,
		oracleType:       C.SQLT_BIN,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             MaxBinaryBytes,   // element length (default)
		isCharData:       false,            // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}
}
