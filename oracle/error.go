package oracle

import (
	"fmt"
	"log"
	"strconv"
)

type Error struct {
	Code    int
	Message string
	At      string
	Offset  int
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

type mismatchElementNum int

func (men mismatchElementNum) Error() string {
	return "Mismatch element number: found " + strconv.Itoa(int(men))
}

func ProgrammingError(text string) error {
	return fmt.Errorf("Programming error: %s", text)
}

func setErrAt(err error, at string) {
	if x, ok := err.(*Error); ok {
		x.At = at
	}
}

// print debug messages?
var IsDebug bool

// print with log.Printf if IsDebug
func debug(format string, args ...interface{}) {
	if IsDebug {
		log.Printf(format, args...)
	}
}
