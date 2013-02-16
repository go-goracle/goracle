// +build !trace

package oracle

const CTrace = false

// no trace
// prints with log.Printf the C-call trace
func ctrace(name string, args ...interface{}) {
	//log.Printf("TRACE %s(%v)", name, args)
}
