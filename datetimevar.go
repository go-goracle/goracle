// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
#include <orl.h>

const size_t sof_OCIIntervalp = sizeof(OCIInterval*);

void getDateTime(const OCIDate *date, sb2 *year, ub1 *month,
				                      ub1 *day, ub1 *hour, ub1 *min, ub1 *sec) {
     *year = (date)->OCIDateYYYY;
     *month = (date)->OCIDateMM;
     *day = (date)->OCIDateDD;
     *hour = (date)->OCIDateTime.OCITimeHH;
     *min = (date)->OCIDateTime.OCITimeMI;
     *sec = (date)->OCIDateTime.OCITimeSS;
}

void setDateTime(OCIDate *date, sb2 year, ub1 month, ub1 day,
				                ub1 hour, ub1 min, ub1 sec) {
     (date)->OCIDateYYYY = year;
     (date)->OCIDateMM = month;
     (date)->OCIDateDD = day;
     (date)->OCIDateTime.OCITimeHH = hour;
     (date)->OCIDateTime.OCITimeMI = min;
     (date)->OCIDateTime.OCITimeSS = sec;
}
*/
import "C"

import (
	// "log"
	// "bytes"
	// "encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

var (
	DateTimeVarType, IntervalVarType *VariableType
)

func dateTimeVar_SetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("awaited time.Time, got %T", value)
	}
	/*
		if err := v.environment.CheckStatus(
			C.OCIDateSetDate(unsafe.Pointer(&v.dataBytes[pos*sizeof_OCIDate]),
				x.Year(), x.Month(), x.Date()),
			"OCIDateSetDate"); err != nil {
			return err
		}
		return v.environment.CheckStatus(
			C.OCIDateSetTime(unsafe.Pointer(&v.dataBytes[pos*sizeof_OCIDate]),
				x.Hour(), x.Minute(), x.Second()),
			"OCIDateSetTime")
	*/
	C.setDateTime((*C.OCIDate)(unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCIDate])),
		C.sb2(x.Year()), C.ub1(x.Month()), C.ub1(x.Day()),
		C.ub1(x.Hour()), C.ub1(x.Minute()), C.ub1(x.Second()))
	return nil
}

func dateTimeVar_GetValue(v *Variable, pos uint) (interface{}, error) {
	var (
		year                             C.sb2
		month, day, hour, minute, second C.ub1
	)
	/*
		err := v.environment.CheckStatus(
			C.OCIDateGetDate(&v.dataBytes[pos*sizeof_OCIDate], &year, &month, &day),
			"OCIDateGetDate")
		if err != nil {
			return nil, err
		}
		if err = v.environment.CheckStatus(
			C.OCIDateGetTime(&v.dataBytes[pos*sizeof_OCIDate], &hour, &minute, &second),
			"OCIDateGetTime"); err != nil {
			return nil, err
		}
	*/
	C.getDateTime((*C.OCIDate)(unsafe.Pointer(&v.dataBytes[pos*C.sizeof_OCIDate])),
		&year, &month, &day, &hour, &minute, &second)
	return time.Date(int(year), time.Month(month), int(day),
		int(hour), int(minute), int(second), 0, time.Local), nil
}

func init() {
	DateTimeVarType = &VariableType{
		Name:             "DateTime",
		setValue:         dateTimeVar_SetValue,
		getValue:         dateTimeVar_GetValue,
		oracleType:       C.SQLT_ODT,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT, // charset form
		size:             C.sizeof_OCIDate, // element length
		isCharData:       false,            // is character data
		isVariableLength: false,            // is variable length
		canBeCopied:      true,             // can be copied
		canBeInArray:     true,             // can be in array
	}
	IntervalVarType = &VariableType{
		Name:             "Interval",
		setValue:         dateTimeVar_SetValue,
		getValue:         dateTimeVar_GetValue,
		oracleType:       C.SQLT_INTERVAL_DS,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,         // charset form
		size:             uint(C.sof_OCIIntervalp), // element length
		isCharData:       false,                    // is character data
		isVariableLength: false,                    // is variable length
		canBeCopied:      true,                     // can be copied
		canBeInArray:     true,                     // can be in array
	}
}
