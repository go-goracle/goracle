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

package godrv

import (
	"database/sql"
	"flag"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/tgulacsi/goracle/oracle"
)

var fDsn = flag.String("dsn", "", "Oracle DSN")

func TestNull(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	var (
		err error
		str string
		dt  time.Time
	)
	qry := "SELECT '' FROM DUAL"
	row := conn.QueryRow(qry)
	if err = row.Scan(&str); err != nil {
		t.Errorf("0. error with %q test: %s", qry, err)
	}
	t.Logf("0. %q result: %#v (%T)", qry, str, str)

	qry = "SELECT TO_DATE(NULL) FROM DUAL"
	row = conn.QueryRow(qry)
	if err = row.Scan(&dt); err != nil {
		t.Errorf("0. error with %q test: %s", qry, err)
	}
	t.Logf("1. %q result: %#v (%T)", qry, dt, dt)
}

func TestSimple(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	var (
		err error
		dst interface{}
	)
	for i, qry := range []string{
		"SELECT ROWNUM FROM DUAL",
		"SELECT 1234567890 FROM DUAL",
		"SELECT LOG(10, 2) FROM DUAL",
		"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL",
		"SELECT HEXTORAW('00') FROM DUAL",
		"SELECT TO_DATE('2006-05-04 15:07:08', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL",
		"SELECT NULL FROM DUAL",
		"SELECT TO_CLOB('árvíztűrő tükörfúrógép') FROM DUAL",
	} {
		row := conn.QueryRow(qry)
		if err = row.Scan(&dst); err != nil {
			t.Errorf("%d. error with %q test: %s", i, qry, err)
		}
		t.Logf("%d. %q result: %#v", i, qry, dst)
		if strings.Index(qry, " TO_CLOB(") >= 0 {
			var b []byte
			var e error
			if true {
				r := dst.(io.Reader)
				b, e = ioutil.ReadAll(r)
			} else {
				clob := dst.(*oracle.ExternalLobVar)
				b, e = clob.ReadAll()
			}
			if e != nil {
				t.Errorf("error reading clob (%v): %s", dst, e)
			} else {
				t.Logf("clob=%s", b)
			}
		}
	}

	qry := "SELECT rn, CHR(rn) FROM (SELECT ROWNUM rn FROM all_objects WHERE ROWNUM < 256)"
	rows, err := conn.Query(qry)
	if err != nil {
		t.Errorf("error with multirow test, query %q: %s", qry, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Errorf("error getting columns for %q: %s", qry, err)
	}
	t.Logf("columns for %q: %v", qry, cols)
	var (
		num int
		str string
	)
	for rows.Next() {
		if err = rows.Scan(&num, &str); err != nil {
			t.Errorf("error scanning row: %s", err)
		}
		//t.Logf("%d=%q", num, str)
	}
}

func TestNumber(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	oldDebug := IsDebug
	IsDebug = true
	if !oldDebug {
		defer func() { IsDebug = false }()
	}

	var (
		err, errF error
		into      int64
		intoF     float64
	)
	for i, tst := range []struct {
		in   string
		want int64
	}{
		{"1", 1},
		{"1234567890", 1234567890},
	} {
		into, intoF, errF = 0, 0, nil
		qry := "SELECT " + tst.in + " FROM DUAL"
		row := conn.QueryRow(qry)
		if err = row.Scan(&into); err != nil {
			row = conn.QueryRow(qry)
			if errF = row.Scan(&intoF); errF != nil {
				t.Errorf("%d. error with %q testF: %s", i, qry, err)
				continue
			}
			t.Logf("%d. %q result: %#v", i, qry, intoF)
			if intoF != float64(tst.want) {
				t.Errorf("%d. got %#v want %#v", i, intoF, float64(tst.want))
			}
			continue
		}
		t.Logf("%d. %q result: %#v", i, qry, into)
		if into != tst.want {
			t.Errorf("%d. got %#v want %#v", i, into, tst.want)
		}
	}
}

// TestClob
func TestClob(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
}

func TestPrepared(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	stmt, err := conn.Prepare("SELECT ? FROM DUAL")
	if err != nil {
		t.Errorf("error preparing query: %v", err)
		t.FailNow()
	}
	rows, err := stmt.Query("a")
	if err != nil {
		t.Errorf("error executing query: %s", err)
		t.FailNow()
	}
	defer rows.Close()
}

var testDB *sql.DB

func getConnection(t *testing.T) *sql.DB {
	var err error
	if testDB != nil && testDB.Ping() == nil {
		return testDB
	}
	flag.Parse()
	if testDB, err = sql.Open("goracle", *fDsn); err != nil {
		t.Fatalf("error connecting to %q: %s", *fDsn, err)
	}
	return testDB
}
