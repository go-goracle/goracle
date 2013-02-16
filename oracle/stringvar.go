package oracle

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
	"fmt"
	"log"
)

var (
	StringVarType, FixedCharVarType *VariableType
	BinaryVarType, RowidVarType     *VariableType
)

func (t *VariableType) IsString() bool {
	if t == StringVarType || t == FixedCharVarType {
		return true
	}
	return false
}

func (t *VariableType) IsBinary() bool {
	if t == BinaryVarType {
		return true
	}
	return false
}

// Initialize the variable.
func stringVar_Initialize(v *Variable, cur *Cursor) error {
	v.actualLength = make([]C.ub2, v.allocatedElements)
	return nil
}

// Set the value of the variable.
func stringVar_SetValue(v *Variable, pos uint, value interface{}) (err error) {
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
					if err = stringVar_SetValue(v, pos+uint(i), arr[i]); err != nil {
						return fmt.Errorf("error setting pos=%d + %d. element: %s", pos, i, err)
					}
				}
				return nil
			} else if arr, ok := value.([]byte); ok {
				for i := range arr {
					if err = stringVar_SetValue(v, pos+uint(i), arr[i]); err != nil {
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
	if v.typ.isCharData && length > MAX_STRING_CHARS {
		return errors.New("string data too large")
	} else if !v.typ.isCharData && length > MAX_BINARY_BYTES {
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
func stringVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	buf := v.dataBytes[int(v.bufferSize*pos) : int(v.bufferSize*pos)+int(v.actualLength[pos])]
	//log.Printf("stringVar_GetValue(pos=%d length=%d): %v (%s)", pos, v.actualLength[pos], buf, buf)
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
func stringVar_GetBufferSize(v *Variable) uint {
	if v.typ.isCharData {
		return v.size * v.environment.MaxBytesPerCharacter
	}
	return uint(v.size)
}

func init() {
	StringVarType = &VariableType{
		Name:             "String",
		isVariableLength: true,
		initialize:       stringVar_Initialize,
		setValue:         stringVar_SetValue,
		getValue:         stringVar_GetValue,
		getBufferSize:    stringVar_GetBufferSize,
		oracleType:       C.SQLT_CHR,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             MAX_STRING_CHARS, // element length (default)
		isCharData:       true,             // is character data
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}

	FixedCharVarType = &VariableType{
		Name:             "FixedChar",
		initialize:       stringVar_Initialize,
		setValue:         stringVar_SetValue,
		getValue:         stringVar_GetValue,
		getBufferSize:    stringVar_GetBufferSize,
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
		initialize:       stringVar_Initialize,
		setValue:         stringVar_SetValue,
		getValue:         stringVar_GetValue,
		getBufferSize:    stringVar_GetBufferSize,
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
		initialize:       stringVar_Initialize,
		setValue:         stringVar_SetValue,
		getValue:         stringVar_GetValue,
		oracleType:       C.SQLT_BIN,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             MAX_BINARY_BYTES, // element length (default)
		isCharData:       false,            // is character data
		isVariableLength: true,             // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}
}
