package main

import (
	"flag"
	"github.com/tgulacsi/goracle"
	"os"
	"testing"
)

var dsn = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")

func init() {
	flag.Parse()
	if *dsn == "" {
		*dsn = os.Getenv("DSN")
	}
}

func TestCursor(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()
	qry := "SELECT owner, object_name, 1 FROM all_objects WHERE ROWNUM < 20"
	if err := cur.Execute(qry, nil, nil); err != nil {
		t.Logf(`error with "%s": %s`, qry, err)
		t.Fail()
	}
	row, err := cur.FetchOne()
	if err != nil {
		t.Logf("error fetching: %s", err)
		t.Fail()
	}
	t.Logf("row: %+v", row)
	rows, err := cur.FetchMany(1000)
	if err != nil {
		t.Logf("error fetching many: %s", err)
		t.Fail()
	}
	for i, row := range rows {
		t.Logf("%03d: %v", i, row)
	}

}

var conn goracle.Connection

func getConnection(t *testing.T) goracle.Connection {
	if conn.IsConnected() {
		return conn
	}

	if !(dsn != nil && *dsn != "") {
		t.Logf("cannot test connection without dsn!")
		return conn
	}
	user, passw, sid := goracle.SplitDsn(*dsn)
	var err error
	conn, err = goracle.NewConnection(user, passw, sid)
	if err != nil {
		t.Logf("error creating connection to %s: %s", *dsn, err)
		t.Fail()
	}
	if err = conn.Connect(0, false); err != nil {
		t.Logf("error connecting: %s", err)
		t.Fail()
	}
	return conn
}

func main() {
	t := new(testing.T)
	TestCursor(t)
}
