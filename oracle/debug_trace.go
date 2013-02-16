// +build trace

package oracle

import "log"

const CTrace = true

// prints with log.Printf the C-call trace
func ctrace(name string, args ...interface{}) {
	log.Printf("CTRACE %s(%v)", name, args)
}
