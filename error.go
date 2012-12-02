// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi

package goracle

import "strconv"

type Error struct {
	Code    int
	Message string
	At      string
}

func NewError(code int, message string) *Error {
	return &Error{Code: code, Message: message}
}

func (err Error) Error() string {
	return err.String()
}

func (err Error) String() string {
	tail := strconv.Itoa(err.Code) + ": " + err.Message
	if err.At != "" {
		return "@" + err.At + " " + tail
	}
	return tail
}
