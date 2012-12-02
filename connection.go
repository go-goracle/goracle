// Copyright 2012-2013 Tamás Gulácsi
// See LICENSE.txt
// Translated from cx_Oracle ((c) Anthony Tuininga) by Tamás Gulácsi
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
	"sync"
)

// MakeDSN makea a data source name given the host port and SID.
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

// ClientVersion returns the client's version (slice of 5 int32s)
func ClientVersion() []int32 {
	var majorVersion, minorVersion, updateNum, patchNum, portUpdateNum C.sword

	C.OCIClientVersion(&majorVersion, &minorVersion, &updateNum,
		&patchNum, &portUpdateNum)
	return []int32{int32(majorVersion), int32(minorVersion), int32(updateNum),
		int32(patchNum), int32(portUpdateNum)}
}

type Connection struct {
	handle        *C.OCISvcCtx  //connection
	serverHandle  *C.OCIServer  //server's handle
	sessionHandle *C.OCISession //session's handle
	environment   *Environment  //environment
	// sessionPool *SessionPool //sessionpool
	username, password, dsn, version string
	commitMode                       int64
	autocommit, release, attached    int32
	srvMtx                           sync.Mutex
}

// Connection_IsConnected()
//   Determines if the connection object is connected to the database.
func (conn Connection) IsConnected() bool {
	return conn.handle != nil
}

//   Create a new connection object by connecting to the database.
func (conn *Connection) Connect(mode int64, twophase bool /*, newPassword string*/) (err error) {
	credentialType := C.OCI_CRED_EXT
	var status int32

	// allocate the server handle
	status = C.OCIHandleAlloc(conn.environment.handle,
		(**C.dvoid)(&conn.serverHandle), OCI_HTYPE_SERVER, 0, 0)
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[allocate server handle]"
		return
	}

	// attach to the server
	/*
	   if (cxBuffer_FromObject(&buffer, self->dsn,
	           self->environment->encoding) < 0)
	       return -1;
	*/

	buffer := []byte(conn.dsn) + []byte{0}
	// Py_BEGIN_ALLOW_THREADS
	conn.srvMtx.Lock()
	status = C.OCIServerAttach(conn.serverHandle,
		conn.environment.errorHandle, (*C.text)(unsafe.Pointer(buffer)),
		len(buffer), C.OCI_DEFAULT)
	// Py_END_ALLOW_THREADS
	conn.srvMtx.Unlock()
	// cxBuffer_Clear(&buffer);
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[server attach]"
		return
	}

	// allocate the service context handle
	status = C.OCIHandleAlloc(conn.environment.handle,
		(**C.dvoid)(&conn.handle), C.OCI_HTYPE_SVCCTX, 0, 0)
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[allocate service context handle]"
		return
	}

	// set attribute for server handle
	status = C.OCIAttrSet(conn.handle, C.OCI_HTYPE_SVCCTX, conn.serverHandle, 0,
		C.OCI_ATTR_SERVER, conn.environment.errorHandle)
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[set server handle]"
		return
	}

	// set the internal and external names; these are needed for global
	// transactions but are limited in terms of the lengths of the strings
	if twophase {
		buffer = []byte{"goracle"} + []byte{0}
		status = C.OCIAttrSet(conn.serverHandle, C.OCI_HTYPE_SERVER,
			buffer, 0, C.OCI_ATTR_INTERNAL_NAME,
			conn.environment.errorHandle)
		if err = CheckStatus(status); err != nil {
			err.At = "Connect[set internal name]"
			return
		}
		status = C.OCIAttrSet(conn.serverHandle, C.OCI_HTYPE_SERVER,
			buffer, 0, C.OCI_ATTR_EXTERNAL_NAME,
			conn.environment.errorHandle)
		if err = CheckStatus(status); err != nil {
			err.At = "Connect[set external name]"
			return
		}
	}

	// allocate the session handle
	status = C.OCIHandleAlloc(conn.environment.handle,
		(**C.dvoid)(&conn.sessionHandle), C.OCI_HTYPE_SESSION, 0, 0)
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[allocate session handle]"
		return
	}

	// set user name in session handle
	if conn.username != "" {
		buffer = []byte(conn.username) + []byte{0}
		credentialType = C.OCI_CRED_RDBMS
		status = C.OCIAttrSet(conn.sessionHandle, C.OCI_HTYPE_SESSION,
			(*C.text)(unsafe.Pointer(buffer)), len(buffer), C.OCI_ATTR_USERNAME,
			conn.environment.errorHandle)
		if err = CheckStatus(status); err != nil {
			err.At = "Connect[set user name]"
			return
		}
	}

	// set password in session handle
	if conn.password {
		buffer = []byte(conn.password) + []byte{0}
		credentialType = C.OCI_CRED_RDBMS
		status = C.OCIAttrSet(conn.sessionHandle, C.OCI_HTYPE_SESSION,
			(*C.text)(unsafe.Pointer(buffer)), len(buffer), C.OCI_ATTR_PASSWORD,
			conn.environment.errorHandle)
		if err = CheckStatus(status); err != nil {
			err.At = "Connect[set password]"
			return
		}
	}

	/*
	   #ifdef OCI_ATTR_DRIVER_NAME
	       status = OCIAttrSet(self->sessionHandle, OCI_HTYPE_SESSION,
	               (text*) DRIVER_NAME, strlen(DRIVER_NAME), OCI_ATTR_DRIVER_NAME,
	               self->environment->errorHandle);
	       if (Environment_CheckForError(self->environment, status,
	               "Connection_Connect(): set driver name") < 0)
	           return -1;

	   #endif
	*/

	// set the session handle on the service context handle
	status = C.OCIAttrSet(conn.handle, C.OCI_HTYPE_SVCCTX,
		conn.sessionHandle, 0, C.OCI_ATTR_SESSION,
		conn.environment.errorHandle)
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[set session handle]"
		return
	}

	/*
	   // if a new password has been specified, change it which will also
	   // establish the session
	   if (newPasswordObj)
	       return Connection_ChangePassword(self, self->password, newPasswordObj);
	*/

	// begin the session
	// Py_BEGIN_ALLOW_THREADS
	conn.srvMtx.Lock()
	status = C.OCISessionBegin(conn.handle, conn.environment.errorHandle,
		conn.sessionHandle, credentialType, mode)
	// Py_END_ALLOW_THREADS
	conn.srvMtx.Unlock()
	if err = CheckStatus(status); err != nil {
		err.At = "Connect[begin session]"
		conn.sessionHandle = nil
		return
	}

	return
}
