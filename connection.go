package goracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <oci.h>
//#include <datetime.h>
//#include <structmember.h>
//#include <time.h>
//#include <oci.h>
//#include <orid.h>
//#include <xa.h>

*/
import "C"

import (
	"fmt"
)

//-----------------------------------------------------------------------------
// MakeDSN()
//   Make a data source name given the host port and SID.
//-----------------------------------------------------------------------------
func MakeDSN(host string, port int, sid, serviceName string) string {
	var format, conn string
	if sid != "" {
		conn = sid
		format = ("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=" +
			"(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SID=%s)))")
	} else {
		conn = serviceName
		format = ("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=" +
			"(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=" +
			"(SERVICE_NAME=%s)))")
	}
	if format == "" {
		return ""
	}
	return fmt.Sprintf(format, host, port, conn)
}

func ClientVersion() []int32 {
	var majorVersion, minorVersion, updateNum, patchNum, portUpdateNum C.sword

	C.OCIClientVersion(&majorVersion, &minorVersion, &updateNum,
		&patchNum, &portUpdateNum)
	return []int32{int32(majorVersion), int32(minorVersion), int32(updateNum),
		int32(patchNum), int32(portUpdateNum)}
}
