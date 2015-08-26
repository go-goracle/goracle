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

package oracle

/*

#include <stdlib.h>
#include <oci.h>
#include <string.h>
#include <xa.h>

const int sizeof_XID = sizeof(XID);
void setXID(XID *xid, int formatId, char *transactionId, int tIdLen, char *branchId, int bIdLen) {
    xid->formatID = formatId;
    xid->gtrid_length = tIdLen;
    xid->bqual_length = bIdLen;
    if (tIdLen > 0)
        strncpy(xid->data, transactionId, tIdLen);
    if (bIdLen > 0)
        strncpy(&xid->data[tIdLen], branchId, bIdLen);
}
*/
import "C"

import (
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"gopkg.in/errgo.v1"
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

// Connection stores the handles for a connection
type Connection struct {
	// not exported fields
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

// IsConnected determines if the connection object is connected to the database.
func (conn *Connection) IsConnected() bool {
	return conn != nil && conn.handle != nil
}

// GetEnvironment returns a (non-modifiable) Environment of the connection
func (conn *Connection) GetEnvironment() Environment {
	return *conn.environment
}

// AttrSet sets an attribute on the connection identified by key setting value with valueLength length
func (conn *Connection) AttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX,
		key, value, valueLength)
}

// ServerAttrSet sets an attribute on the server handle identified by key
func (conn *Connection) ServerAttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.serverHandle), C.OCI_HTYPE_SERVER,
		key, value, valueLength)
}

// SessionAttrSet sets an attribute on the session handle identified by key
func (conn *Connection) SessionAttrSet(key C.ub4, value unsafe.Pointer, valueLength int) error {
	return conn.environment.AttrSet(
		unsafe.Pointer(conn.sessionHandle), C.OCI_HTYPE_SESSION,
		key, value, valueLength)
}

// NewConnection creates a new connection and initializes the connection members.
func NewConnection(username, password, dsn string, autocommit bool /*commitMode , pool, twophase bool*/) (
	conn *Connection, err error) {
	conn = &Connection{username: username, password: password, dsn: dsn, autocommit: autocommit}
	/*
		if pool != nil {
			conn.environment = pool.environment
		} else
	*/
	if conn.environment, err = NewEnvironment(); err != nil {
		return
	}
	err = conn.Connect(0, false)
	return
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
			conn.Free(false)
		}
	}()

	// free handles
	conn.Free(false)
	// allocate the server handle
	if ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
		C.OCI_HTYPE_SERVER,
		(*unsafe.Pointer)(unsafe.Pointer(&conn.serverHandle)),
		"Connect[allocate server handle]"); err != nil {
		return errgo.Mask(

			// attach to the server
			/*
			   if (cxBuffer_FromObject(&buffer, self->dsn,
			           self->environment->encoding) < 0)
			       return -1;
			*/err)
	}

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
		return errgo.Mask(

			// log.Printf("attached to server %s", conn.serverHandle)
			err)
	}

	// allocate the service context handle
	if err = ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
		C.OCI_HTYPE_SVCCTX, (*unsafe.Pointer)(unsafe.Pointer(&conn.handle)),
		"Connect[allocate service context handle]"); err != nil {
		return errgo.Mask(

			// log.Printf("allocated service context handle")
			err)
	}

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
		return errgo.Mask(

			// log.Printf("allocated session handle")
			err)
	}

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
		//fmt.Printf("set user name %s\n", buffer)
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
		//fmt.Printf("set password %s\n", buffer)
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

// Commit commits the transaction on the connection.
func (conn *Connection) Commit() error {
	// make sure we are actually connected
	if !conn.IsConnected() {
		return nil //?
	}

	conn.srvMtx.Lock()
	// perform the commit
	//Py_BEGIN_ALLOW_THREADS
	err := conn.environment.CheckStatus(
		C.OCITransCommit(conn.handle, conn.environment.errorHandle,
			C.ub4(conn.commitMode)), "Commit")
	conn.srvMtx.Unlock()
	if err != nil {
		return errgo.Mask(err)
	}
	conn.commitMode = C.OCI_DEFAULT
	//Py_END_ALLOW_THREADS
	return nil
}

// Begin begins a new transaction on the connection.
func (conn *Connection) Begin(formatID int, transactionID, branchID string) error {
	var transactionHandle *C.OCITrans
	var xid C.XID

	// parse the arguments
	formatID = -1
	if len(transactionID) > C.MAXGTRIDSIZE {
		return errgo.New("transaction id too large")
	}
	if len(branchID) > C.MAXBQUALSIZE {
		return errgo.New("branch id too large")
	}

	// make sure we are actually connected
	if !conn.IsConnected() {
		return nil
	}

	// determine if a transaction handle was previously allocated
	_, err := conn.environment.AttrGet(
		unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX,
		C.OCI_ATTR_TRANS, unsafe.Pointer(&transactionHandle),
		"Connection.Begin(): find existing transaction handle")
	if err != nil {
		return errgo.Mask(

			// create a new transaction handle, if necessary
			err)
	}

	if transactionHandle == nil {
		if err = ociHandleAlloc(unsafe.Pointer(conn.environment.handle),
			C.OCI_HTYPE_TRANS,
			(*unsafe.Pointer)(unsafe.Pointer(&transactionHandle)),
			"Connection.Begin"); err != nil {
			return errgo.New("Connection.Begin(): allocate transaction handle: " +
				err.Error())
		}
	}

	// set the XID for the transaction, if applicable
	if formatID != -1 {
		tID := []byte(transactionID)
		bID := []byte(branchID)
		C.setXID(&xid, C.int(formatID),
			(*C.char)(unsafe.Pointer(&tID[0])), C.int(len(tID)),
			(*C.char)(unsafe.Pointer(&bID[0])), C.int(len(bID)))
		if err = conn.environment.AttrSet(
			unsafe.Pointer(transactionHandle), C.OCI_ATTR_XID,
			C.OCI_HTYPE_TRANS, unsafe.Pointer(&xid), C.sizeof_XID); err != nil {
			return errgo.New("Connection.Begin(): set XID: " + err.Error())
		}
	}

	// associate the transaction with the connection
	if err = conn.environment.AttrSet(
		unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX,
		C.OCI_ATTR_TRANS, unsafe.Pointer(transactionHandle), 0); err != nil {
		return errgo.New("Connection.Begin(): associate transaction: " + err.Error())
	}

	// start the transaction
	//Py_BEGIN_ALLOW_THREADS
	conn.srvMtx.Lock()
	err = conn.environment.CheckStatus(
		C.OCITransStart(conn.handle, conn.environment.errorHandle, 0, C.OCI_TRANS_NEW),
		"start transaction")
	conn.srvMtx.Unlock()
	if err != nil {
		return errgo.New("Connection.Begin(): start transaction: " + err.Error())
	}

	//Py_END_ALLOW_THREADS
	return nil
}

// Prepare commits (if there is anything, TWO-PAHSE) the transaction on the connection.
func (conn *Connection) Prepare() (bool, error) {
	// make sure we are actually connected
	if !conn.IsConnected() {
		return false, nil //?
	}

	conn.srvMtx.Lock()
	// perform the prepare
	//Py_BEGIN_ALLOW_THREADS
	status := C.OCITransPrepare(conn.handle, conn.environment.errorHandle, C.OCI_DEFAULT)
	conn.srvMtx.Unlock()
	// if nothing available to prepare, return False in order to allow for
	// avoiding the call to commit() which will fail with ORA-24756
	// (transaction does not exist)
	if status == C.OCI_SUCCESS_WITH_INFO {
		return false, nil
	}
	if err := conn.environment.CheckStatus(status, "Prepare"); err != nil {
		return false, errgo.Mask(err)
	}
	conn.commitMode = C.OCI_TRANS_TWOPHASE
	//Py_END_ALLOW_THREADS
	return true, nil
}

// Rollback rolls back the transaction
func (conn *Connection) Rollback() error {
	conn.srvMtx.Lock()
	err := conn.environment.CheckStatus(conn.rollback(), "Rollback")
	conn.srvMtx.Unlock()
	return err
}

func (conn *Connection) rollback() C.sword {
	return C.OCITransRollback(conn.handle, conn.environment.errorHandle,
		C.OCI_DEFAULT)
}

// Free deallocates the connection, disconnecting from the database if necessary.
func (conn *Connection) Free(freeEnvironment bool) {
	if conn.release {
		// Py_BEGIN_ALLOW_THREADS
		conn.srvMtx.Lock()
		conn.rollback()
		C.OCISessionRelease(conn.handle, conn.environment.errorHandle, nil,
			0, C.OCI_DEFAULT)
		// Py_END_ALLOW_THREADS
		conn.srvMtx.Unlock()
	} else if !conn.attached {
		conn.srvMtx.Lock()
		if conn.sessionHandle != nil {
			// Py_BEGIN_ALLOW_THREADS
			conn.rollback()
			C.OCISessionEnd(conn.handle, conn.environment.errorHandle,
				conn.sessionHandle, C.OCI_DEFAULT)
			// Py_END_ALLOW_THREADS
		}
		if conn.serverHandle != nil {
			C.OCIServerDetach(conn.serverHandle,
				conn.environment.errorHandle, C.OCI_DEFAULT)
		}
		conn.srvMtx.Unlock()
	}
	if conn.sessionHandle != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.sessionHandle), C.OCI_HTYPE_SESSION)
		conn.sessionHandle = nil
	}
	if conn.handle != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.handle), C.OCI_HTYPE_SVCCTX)
		conn.handle = nil
	}
	if conn.serverHandle != nil {
		C.OCIHandleFree(unsafe.Pointer(conn.serverHandle), C.OCI_HTYPE_SERVER)
		conn.serverHandle = nil
	}
	if freeEnvironment {
		// Free env (Issue #10)
		if conn.environment != nil {
			conn.environment.Free()
			conn.environment = nil
		}
	}
}

// Close the connection, disconnecting from the database.
func (conn *Connection) Close() (err error) {
	if !conn.IsConnected() {
		return nil //?
	}

	conn.srvMtx.Lock()
	// perform a rollback
	conn.rollback()

	// logoff of the server
	if conn.handle != nil && conn.sessionHandle != nil {
		// Py_BEGIN_ALLOW_THREADS
		err = conn.environment.CheckStatus(C.OCISessionEnd((conn.handle),
			conn.environment.errorHandle, conn.sessionHandle,
			C.OCI_DEFAULT), "Close[end session]")
	}
	if conn.serverHandle != nil {
		if err2 := conn.environment.CheckStatus(
			C.OCIServerDetach(conn.serverHandle, conn.environment.errorHandle, C.OCI_DEFAULT),
			"Close[server detach]"); err2 != nil && err == nil {
			err = err2
		}
	}
	conn.srvMtx.Unlock()
	conn.Free(true)
	return err
}

// Cancel executes an OCIBreak() to cause an immediate (asynchronous) abort of any
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

// Ping makes a round trip call to the server to confirm that the connection and
// server are active.
func (conn *Connection) Ping() error {
	if !conn.IsConnected() {
		return nil
	}
	return conn.environment.CheckStatus(C.OCIPing(conn.handle, conn.environment.errorHandle,
		C.OCI_DEFAULT), "Ping")

}

// NewCursor creates a new cursor (statement) referencing the connection.
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

//SplitDSN splits username/password@sid
func SplitDSN(dsn string) (username, password, sid string) {
	if i := strings.LastIndex(dsn, "@"); i >= 0 {
		//fmt.Printf("dsn=%q (%d) i=%d\n", dsn, len(dsn), i)
		if i > 0 {
			username = dsn[:i]
		}
		if i < len(dsn)-1 {
			sid = dsn[i+1:]
		}
	} else {
		username = dsn
	}
	if i := strings.Index(username, "/"); i >= 0 {
		//fmt.Printf("username=%q (%d) i=%d\n", username, len(username), i)
		if i > 0 {
			if i < len(username) {
				password = username[i+1:]
			}
			username = username[:i]
		} else {
			username, password = "", username[1:]
		}
	}
	return
}

// NlsSettings retrieves NLS parameters: OCI charset, client NLS_LANG and database NLS_LANG
// cur can be nil (in this case it allocates one)
func (conn *Connection) NlsSettings(cur *Cursor) (oci, client, database string, err error) {
	oci = conn.environment.Encoding
	if cur == nil {
		cur = conn.NewCursor()
		defer cur.Close()
	}
	if err = cur.Execute("SELECT USERENV('language') FROM DUAL", nil, nil); err != nil {
		err = errgo.Newf("cannot get session language!?!: %s", err)
		return
	}
	var row []interface{}
	if row, err = cur.FetchOne(); err != nil {
		err = errgo.Newf("no userenv('language')? %s", err)
		return
	}
	client = row[0].(string)

	if err = cur.Execute(`SELECT parameter, value FROM nls_database_parameters
			WHERE parameter IN ('NLS_TERRITORY', 'NLS_LANGUAGE', 'NLS_CHARACTERSET')`,
		nil, nil); err != nil {
		err = errgo.Newf("error selecting from nls_database_parameters: %s", err)
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
