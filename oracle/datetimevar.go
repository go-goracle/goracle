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
#include <orl.h>
#include <ociap.h>

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
	"fmt"
	"time"
	"unsafe"
)

var (
	//IntervalWorks should be true only if Interval datatype works
	IntervalWorks = false
	//DateTimeVarType is the variable type for DateTime
	DateTimeVarType *VariableType
	//IntervalVarType is the variable type for Interval
	IntervalVarType *VariableType
)

// IsDate checks whether the variable type is Date or Interval
func (t *VariableType) IsDate() bool {
	if t == DateTimeVarType || t == IntervalVarType {
		return true
	}
	return false
}

func dateTimeVarSetValue(v *Variable, pos uint, value interface{}) error {
	x, ok := value.(time.Time)
	if !ok {
		a, ok := value.([]time.Time)
		if !ok {
			return fmt.Errorf("awaited time.Time or []time.Time, got %T", value)
		}
		var err error
		for i, x := range a {
			if err = dateTimeVarSetValue(v, pos+uint(i), x); err != nil {
				return err
			}
		}
		return nil
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
	C.setDateTime((*C.OCIDate)(unsafe.Pointer(&v.dataBytes[pos*v.typ.size])),
		C.sb2(x.Year()), C.ub1(x.Month()), C.ub1(x.Day()),
		C.ub1(x.Hour()), C.ub1(x.Minute()), C.ub1(x.Second()))
	return nil
}

func dateTimeVarGetValue(v *Variable, pos uint) (interface{}, error) {
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
	C.getDateTime((*C.OCIDate)(unsafe.Pointer(&v.dataBytes[pos*v.typ.size])),
		&year, &month, &day, &hour, &minute, &second)
	return time.Date(int(year), time.Month(month), int(day),
		int(hour), int(minute), int(second), 0, time.Local), nil
}

// intervalVarSetValue sets the value of the variable.
func intervalVarSetValue(v *Variable, pos uint, value interface{}) error {
	var days, hours, minutes, seconds, microseconds C.sb4

	x, ok := value.(time.Duration)
	if !ok {
		return fmt.Errorf("requires time.Duration, got %T", value)
	}

	days = C.sb4(x.Hours()) / 24
	hours = C.sb4(x.Hours()) - days*24
	minutes = C.sb4(x.Minutes() - x.Hours()*60)
	seconds = C.sb4(x.Seconds()-x.Minutes()) * 60
	microseconds = C.sb4(float64(x.Nanoseconds()/1000) - x.Seconds()*1000*1000)
	return v.environment.CheckStatus(
		C.OCIIntervalSetDaySecond(unsafe.Pointer(v.environment.handle),
			v.environment.errorHandle,
			days, hours, minutes, seconds, microseconds,
			(*C.OCIInterval)(unsafe.Pointer(&v.dataBytes[pos*v.typ.size]))),
		"IntervalSetDaySecond")
}

// Returns the value stored at the given array position.
func intervalVarGetValue(v *Variable, pos uint) (interface{}, error) {
	var days, hours, minutes, seconds, microseconds C.sb4

	if err := v.environment.CheckStatus(
		C.OCIIntervalGetDaySecond(unsafe.Pointer(v.environment.handle),
			v.environment.errorHandle,
			&days, &hours, &minutes, &seconds, &microseconds,
			(*C.OCIInterval)(unsafe.Pointer((&v.dataBytes[pos*v.typ.size])))),
		"internalVar_GetValue"); err != nil {
		return nil, err
	}
	return (time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second +
		time.Duration(microseconds)*time.Microsecond), nil
}

func init() {
	DateTimeVarType = &VariableType{
		Name:             "DateTime",
		setValue:         dateTimeVarSetValue,
		getValue:         dateTimeVarGetValue,
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
		setValue:         intervalVarSetValue,
		getValue:         intervalVarGetValue,
		oracleType:       C.SQLT_INTERVAL_DS,       // Oracle type
		charsetForm:      C.SQLCS_IMPLICIT,         // charset form
		size:             uint(C.sof_OCIIntervalp), // element length
		isCharData:       false,                    // is character data
		isVariableLength: false,                    // is variable length
		canBeCopied:      true,                     // can be copied
		canBeInArray:     true,                     // can be in array
	}
}
