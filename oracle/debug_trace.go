// +build trace

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

import (
	"fmt"
	"runtime"
)

//CTrace is true iff we are printing TRACE messages
const CTrace = true

//ctrace prints with log.Printf the C-call trace
func ctrace(name string, args ...interface{}) {
	Log.Debug(fmt.Sprintf("CTRACE "+name, args...))
}

var pc = make([]byte, 4096)

// getStackTrace returns the calling stack trace
func getStackTrace() string {
	for {
		n := runtime.Stack(pc, false)
		if n < len(pc) {
			return string(pc[:n])
		}
		pc = make([]byte, n*2)
	}
}
