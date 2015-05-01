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

import (
	"fmt"
	"strconv"

	"gopkg.in/errgo.v1"
)

// Error is an error struct holding additional info
type Error struct {
	Code    int
	Message string
	At      string
	Offset  int
}

// NewError creates a new error, pointing to the code with the given message
func NewError(code int, message string) *Error {
	return NewErrorAt(code, message, "")
}

// NewErrorAt creates a new error, pointing to the code with the given message
func NewErrorAt(code int, message, at string) *Error {
	return &Error{Code: code, Message: message, At: at}
}

// Error returns a string representation of the error (implenets error)
func (err Error) Error() string {
	return err.String()
}

// String prints a nice(er) error message (implements Stringer)
func (err Error) String() string {
	tail := strconv.Itoa(err.Code) + ": " + err.Message
	var head string
	if err.Offset != 0 {
		head = "row " + strconv.Itoa(err.Offset) + " "
	}
	if err.At != "" {
		return head + "@" + err.At + " " + tail
	}
	return head + tail
}

type mismatchElementNum int

func (men mismatchElementNum) Error() string {
	return "Mismatch element number: found " + strconv.Itoa(int(men))
}

// ProgrammingError returns a programming error
func ProgrammingError(text string) error {
	return errgo.Newf("Programming error: %s", text)
}

func setErrAt(err error, at string) {
	if x, ok := errgo.Cause(err).(*Error); ok {
		x.At = at
	}
}

// IsDebug print debug messages?
var IsDebug bool

// debug prints with log.Printf iff IsDebug
func debug(format string, args ...interface{}) {
	if IsDebug {
		Log.Debug(fmt.Sprintf(format, args...))
	}
}
