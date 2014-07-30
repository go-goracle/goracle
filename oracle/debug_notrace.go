// +build !trace

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

//CTrace is true iff we are printing TRACE messages
const CTrace = false

// no trace
//ctrace prints with log.Printf the C-call trace
func ctrace(name string, args ...interface{}) {
	//log.Printf("TRACE %s(%v)", name, args)
}

// getStackTrace returns an empty string
func getStackTrace() string { return "" }
