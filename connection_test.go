package goracle

import (
	"flag"
	"testing"
)

var dsn = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")

func init() {
	flag.Parse()
}

func TestMakeDSN(t *testing.T) {
	dsn := MakeDSN("localhost", 1521, "sid", "")
	if dsn != ("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=" +
		"(PROTOCOL=TCP)(HOST=localhost)(PORT=1521)))(CONNECT_DATA=(SID=sid)))") {
		t.Logf(dsn)
		t.Fail()
	}
	dsn = MakeDSN("localhost", 1522, "", "service")
	if dsn != ("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=" +
		"(PROTOCOL=TCP)(HOST=localhost)(PORT=1522)))(CONNECT_DATA=" +
		"(SERVICE_NAME=service)))") {
		t.Logf(dsn)
		t.Fail()
	}
}

func TestClientVersion(t *testing.T) {
	t.Logf("%+v", ClientVersion())
}

func TestIsConnected(t *testing.T) {
	if (Connection{}).IsConnected() {
		t.Fail()
	}
}

func TestConnect(t *testing.T) {
	if !(dsn != nil && *dsn != "") {
		t.Logf("cannot test connection without dsn!")
		return
	}
	user, passw, sid := SplitDsn(*dsn)
	conn, err := NewConnection(user, passw, sid)
	if err != nil {
		t.Logf("error creating connection to %s: %s", *dsn, err)
		t.Fail()
	}
	if err = conn.Connect(0, false); err != nil {
		t.Logf("error connecting: %s", err)
		t.Fail()
	}
	conn.Free()
}
