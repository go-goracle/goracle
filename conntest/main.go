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
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"

	"github.com/tgulacsi/goracle/oracle"
)

var (
	fDsn = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")

	fUsername    = flag.String("username", "", "username to connect as (if you don't provide the dsn")
	fPassword    = flag.String("password", "", "password to connect with (if you don't provide the dsn")
	fHost        = flag.String("host", "", "Oracle DB's host (if you don't provide the dsn")
	fPort        = flag.Int("port", 1521, "Oracle DB's port (if you don't provide the dsn) - defaults to 1521")
	fSid         = flag.String("sid", "", "Oracle DB's SID (if you don't provide the dsn)")
	fServiceName = flag.String("service", "", "Oracle DB's ServiceName (if you don't provide the dsn and the sid)")

	fWait = flag.Bool("wait", false, "wait for USR1 signal?")
)

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

	var user, passw, sid string
	if !(fDsn == nil || *fDsn == "") {
		user, passw, sid = oracle.SplitDsn(*fDsn)
		log.Printf("user=%q passw=%q sid=%q", user, passw, sid)
	}
	if user == "" && fUsername != nil && *fUsername != "" {
		user = *fUsername
	}
	if passw == "" && fPassword != nil && *fPassword != "" {
		passw = *fPassword
	}
	if sid == "" {
		if fSid != nil && *fSid != "" {
			sid = *fSid
		} else {
			sid = oracle.MakeDSN(*fHost, *fPort, "", *fServiceName)
		}
	}
	dsn := user + "/" + passw + "@" + sid
	var err error
	log.Printf("connecting to %s", dsn)
	conn, err = oracle.NewConnection(user, passw, sid, false)
	if err != nil {
		t.Logf("error creating connection to %s: %s", dsn, err)
		t.Fail()
	}
	if err = conn.Connect(0, false); err != nil {
		t.Logf("error connecting: %s", err)
		t.Fail()
	}
	return conn
}

func main() {
	flag.Parse()
	if *fDsn == "" {
		*fDsn = os.Getenv("DSN")
	}
	t := new(testing.T)
	if *fWait {
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
