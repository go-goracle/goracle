package main

import (
	"flag"
	"github.com/tgulacsi/goracle/oracle"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

var (
	dsn  = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")
	wait = flag.Bool("wait", false, "wait for USR1 signal?")
)

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
	qry := "SELECT owner, object_name, object_id FROM all_objects WHERE ROWNUM < 20"
	log.Printf(`executing "%s"`, qry)
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

var conn oracle.Connection

func getConnection(t *testing.T) oracle.Connection {
	if conn.IsConnected() {
		return conn
	}

	if !(dsn != nil && *dsn != "") {
		t.Logf("cannot test connection without dsn!")
		return conn
	}
	user, passw, sid := oracle.SplitDsn(*dsn)
	var err error
	log.Printf("connecting to %s", *dsn)
	conn, err = oracle.NewConnection(user, passw, sid)
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
	if *wait {
		c := make(chan os.Signal)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			log.Printf("waiting for signal...")
			sig := <-c
			log.Printf("got signal %s", sig)
			TestCursor(t)
			wg.Done()
		}()
		signal.Notify(c, syscall.SIGUSR1)
		wg.Wait()
	} else {
		TestCursor(t)
	}
}
