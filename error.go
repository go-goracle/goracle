// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi

package goracle

import (
	"strconv"
)

type Error struct {
	Code    int
	Message string
	At      string
	Offset int
}

func NewError(code int, message string) *Error {
	return &Error{Code: code, Message: message}
}

func (err Error) Error() string {
	return err.String()
}

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
