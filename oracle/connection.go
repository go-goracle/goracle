package oracle

/*
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <oci.h>
*/
import "C"

import (
	"fmt"
	// "log"
	"strings"
	"sync"
	"unsafe"
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
	autocommit, release, attached    bool
	srvMtx                           sync.Mutex
}

// Connection_IsConnected()
//   Determines if the connection object is connected to the database.
func (conn Connection) IsConnected() bool {
	return conn.handle != nil
}

// returns a (non-modifiable) Environment of the connection
func (conn Connection) GetEnvironment() Environment {
	return *conn.environment
}

func (conn *Connection) AttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX,
		key, value, valueLength)
}

func (conn *Connection) ServerAttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.serverHandle), C.OCI_HTYPE_SERVER,
		key, value, valueLength)
}

func (conn *Connection) SessionAttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.sessionHandle), C.OCI_HTYPE_SESSION,
		key, value, valueLength)
}

//   Initialize the connection members.
func NewConnection(username, password, dsn string /*commitMode , pool, twophase bool*/) (
	conn Connection, err error) {
	conn = Connection{username: username, password: password, dsn: dsn}
	/*
		if pool != nil {
			conn.environment = pool.environment
		} else
	*/
	conn.environment, err = NewEnvironment()

	return conn, err
}

// Connect to the database.
// good minimal example: http://www.adp-gmbh.ch/ora/misc/oci/index.html
func (conn *Connection) Connect(mode int64, twophase bool /*, newPassword string*/) error {
	credentialType := C.OCI_CRED_EXT
	var (
		status C.sword
		err    error
	)
	defer func() {
		if err != nil {
			if conn.sessionHandle != nil {
				C.OCIHandleFree(unsafe.Pointer(conn.sessionHandle),
					C.OCI_HTYPE_SESSION)
			}
			if conn.handle != nil {
				C.OCIHandleFree(unsafe.Pointer(conn.handle),
					C.OCI_HTYPE_SVCCTX)
			}
			if conn.serverHandle != nil {
				C.OCIHandleFree(unsafe.Pointer(conn.serverHandle),
					C.OCI_HTYPE_SERVER)
			}
		}
	}()

	// allocate the server handle
	if ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
		C.OCI_HTYPE_SERVER,
		(*unsafe.Pointer)(unsafe.Pointer(&conn.serverHandle)),
		"Connect[allocate server handle]"); err != nil {
		return err
	}

	// attach to the server
	/*
	   if (cxBuffer_FromObject(&buffer, self->dsn,
	           self->environment->encoding) < 0)
	       return -1;
	*/

	buffer := make([]byte, max(16, len(conn.dsn), len(conn.username), len(conn.password))+1)
	copy(buffer, []byte(conn.dsn))
	buffer[len(conn.dsn)] = 0
	// dsn := C.CString(conn.dsn)
	// defer C.free(unsafe.Pointer(dsn))
	// Py_BEGIN_ALLOW_THREADS
	conn.srvMtx.Lock()
	// log.Printf("buffer=%s", buffer)
	status = C.OCIServerAttach(conn.serverHandle,
		conn.environment.errorHandle, (*C.OraText)(&buffer[0]),
		C.sb4(len(buffer)), C.OCI_DEFAULT)
	// Py_END_ALLOW_THREADS
	conn.srvMtx.Unlock()
	// cxBuffer_Clear(&buffer);
	if err = conn.environment.CheckStatus(status, "Connect[server attach]"); err != nil {
		return err
	}
	// log.Printf("attached to server %s", conn.serverHandle)

	// allocate the service context handle
	if err = ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
		C.OCI_HTYPE_SVCCTX, (*unsafe.Pointer)(unsafe.Pointer(&conn.handle)),
		"Connect[allocate service context handle]"); err != nil {
		return err
	}
	// log.Printf("allocated service context handle")

	// set attribute for server handle
	if err = conn.AttrSet(C.OCI_ATTR_SERVER, unsafe.Pointer(conn.serverHandle), 0); err != nil {
		setErrAt(err, "Connect[set server handle]")
		return err
	}

	// set the internal and external names; these are needed for global
	// transactions but are limited in terms of the lengths of the strings
	if twophase {
		name := []byte("goracle")
		copy(buffer, name)
		buffer[len(name)] = 0

		if err = conn.ServerAttrSet(C.OCI_ATTR_INTERNAL_NAME,
			unsafe.Pointer(&buffer[0]), len(name)); err != nil {
			setErrAt(err, "Connect[set internal name]")
			return err
		}
		if err = conn.ServerAttrSet(C.OCI_ATTR_EXTERNAL_NAME,
			unsafe.Pointer(&buffer[0]), len(name)); err != nil {
			setErrAt(err, "Connect[set external name]")
			return err
		}
	}

	// allocate the session handle
	if err = ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
		C.OCI_HTYPE_SESSION,
		(*unsafe.Pointer)(unsafe.Pointer(&conn.sessionHandle)),
		"Connect[allocate session handle]"); err != nil {
		return err
	}
	// log.Printf("allocated session handle")

	// set user name in session handle
	if conn.username != "" {
		copy(buffer, []byte(conn.username))
		buffer[len(conn.username)] = 0
		credentialType = C.OCI_CRED_RDBMS

		if err = conn.SessionAttrSet(C.OCI_ATTR_USERNAME,
			unsafe.Pointer(&buffer[0]), len(conn.username)); err != nil {
			setErrAt(err, "Connect[set user name]")
			return err
		}
		// log.Printf("set user name %s", buffer)
	}

	// set password in session handle
	if conn.password != "" {
		copy(buffer, []byte(conn.password))
		buffer[len(conn.password)] = 0
		credentialType = C.OCI_CRED_RDBMS
		if err = conn.SessionAttrSet(C.OCI_ATTR_PASSWORD,
			unsafe.Pointer(&buffer[0]), len(conn.password)); err != nil {
			setErrAt(err, "Connect[set password]")
			return err
		}
		// log.Printf("set password %s", buffer)
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
	if err = conn.AttrSet(C.OCI_ATTR_SESSION,
		unsafe.Pointer(conn.sessionHandle), 0); err != nil {
		setErrAt(err, "Connect[set session handle]")
		return err
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
		conn.sessionHandle, C.ub4(credentialType), C.ub4(mode))
	// Py_END_ALLOW_THREADS
	conn.srvMtx.Unlock()
	if err = conn.environment.CheckStatus(status, "Connect[begin session]"); err != nil {
		conn.sessionHandle = nil
		return err
	}

	return nil
}

func (conn *Connection) Rollback() error {
	conn.srvMtx.Lock()
	defer conn.srvMtx.Unlock()
	return conn.environment.CheckStatus(
		C.OCITransRollback(conn.handle, conn.environment.errorHandle,
			C.OCI_DEFAULT), "Rollback")
}

// Deallocate the connection, disconnecting from the database if necessary.
func (conn *Connection) Free() {
	if conn.release {
		// Py_BEGIN_ALLOW_THREADS
		conn.Rollback()
		conn.srvMtx.Lock()
		C.OCISessionRelease(conn.handle, conn.environment.errorHandle, nil,
			0, C.OCI_DEFAULT)
		// Py_END_ALLOW_THREADS
		conn.srvMtx.Unlock()
	} else if !conn.attached {
		if conn.sessionHandle != nil {
			// Py_BEGIN_ALLOW_THREADS
			conn.Rollback()
			conn.srvMtx.Lock()
			C.OCISessionEnd(conn.handle, conn.environment.errorHandle,
				conn.sessionHandle, C.OCI_DEFAULT)
			// Py_END_ALLOW_THREADS
			conn.srvMtx.Unlock()
		}
		if conn.serverHandle != nil {
			C.OCIServerDetach(conn.serverHandle,
				conn.environment.errorHandle, C.OCI_DEFAULT)
		}
	}
}

//   Close the connection, disconnecting from the database.
func (conn *Connection) Close() (err error) {
	if !conn.IsConnected() {
		return nil //?
	}

	// perform a rollback
	if err = conn.Rollback(); err != nil {
		setErrAt(err, "Close[rollback]")
		return
	}
	conn.srvMtx.Lock()
	defer conn.srvMtx.Unlock()

	// logoff of the server
	if conn.sessionHandle != nil {
		// Py_BEGIN_ALLOW_THREADS
		if err = conn.environment.CheckStatus(C.OCISessionEnd((conn.handle),
			conn.environment.errorHandle, conn.sessionHandle,
			C.OCI_DEFAULT), "Close[end session]"); err != nil {
			return
		}
		C.OCIHandleFree(unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX)
	}
	conn.handle = nil
	if conn.serverHandle != nil {
		if err = conn.environment.CheckStatus(
			C.OCIServerDetach(conn.serverHandle, conn.environment.errorHandle, C.OCI_DEFAULT),
			"Close[server detach]"); err != nil {
			return
		}
		conn.serverHandle = nil
	}
	return nil
}

// Execute an OCIBreak() to cause an immediate (asynchronous) abort of any
// currently executing OCI function.
func (conn *Connection) Cancel() error {
	// make sure we are actually connected
	if !conn.IsConnected() {
		return nil
	}

	// perform the break
	return conn.environment.CheckStatus(C.OCIBreak(unsafe.Pointer(conn.handle),
		conn.environment.errorHandle), "Cancel")
}

// Makes a round trip call to the server to confirm that the connection and
// server are active.
func (conn *Connection) Ping() error {
	if !conn.IsConnected() {
		return nil
	}
	return conn.environment.CheckStatus(C.OCIPing(conn.handle, conn.environment.errorHandle,
		C.OCI_DEFAULT), "Ping")

}

// Create a new cursor (statement) referencing the connection.
func (conn *Connection) NewCursor() *Cursor {
	return NewCursor(conn)
}

func max(numbers ...int) int {
	if len(numbers) == 0 {
		return 0
	}
	m := numbers[0]
	for _, x := range numbers {
		if m < x {
			m = x
		}
	}
	return m
}

// splits username/password@sid
func SplitDsn(dsn string) (username, password, sid string) {
	if i := strings.Index(dsn, "/"); i > 0 {
		username = dsn[:i]
		dsn = dsn[i+1:]
	}
	if i := strings.Index(dsn, "@"); i > 0 {
		password = dsn[:i]
		dsn = dsn[i+1:]
	}
	sid = dsn
	return
}

// retrieve NLS parameters: OCI charset, client NLS_LANG and database NLS_LANG
// cur can be nil (in this case it allocates one)
func (conn *Connection) NlsSettings(cur *Cursor) (oci, client, database string, err error) {
	oci = conn.environment.Encoding
	if cur == nil {
		cur = conn.NewCursor()
		defer cur.Close()
	}
	if err = cur.Execute("SELECT USERENV('language') FROM DUAL", nil, nil); err != nil {
		err = fmt.Errorf("cannot get session language!?!: %s", err)
		return
	}
	var row []interface{}
	if row, err = cur.FetchOne(); err != nil {
		err = fmt.Errorf("no userenv('language')? %s", err)
		return
	}
	client = row[0].(string)

	if err = cur.Execute(`SELECT parameter, value FROM nls_database_parameters
			WHERE parameter IN ('NLS_TERRITORY', 'NLS_LANGUAGE', 'NLS_CHARACTERSET')`,
		nil, nil); err != nil {
		err = fmt.Errorf("error selecting from nls_database_parameters: %s", err)
		return
	}
	params := make(map[string]string, 3)
	for err == nil {
		if row, err = cur.FetchOne(); err == nil {
			params[row[0].(string)] = row[1].(string)
		}
	}
	err = nil
	database = params["NLS_LANGUAGE"] + "_" + params["NLS_TERRITORY"] + "." + params["NLS_CHARACTERSET"]
	return
}
