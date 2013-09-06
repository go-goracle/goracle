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
	"testing"
	"time"
)

var fDsn = flag.String("dsn", "", "Oracle DSN")

func TestSimple(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	var (
		err  error
		num  int
		nul  interface{}
		rat  float64
		raw  []byte
		str  string
		date time.Time
		//dur time.Duration
	)
	for i, tst := range []struct {
		qry string
		dst interface{}
	}{
		{"SELECT ROWNUM FROM DUAL", &num},
		{"SELECT LOG(10, 2) FROM DUAL", &rat},
		{"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL", &str},
		{"SELECT HEXTORAW('00') FROM DUAL", &raw},
		{"SELECT TO_DATE('2006-05-04 15:07:08', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL", &date},
		{"SELECT NULL FROM DUAL", &nul},
	} {
		row := conn.QueryRow(tst.qry)
		if err = row.Scan(tst.dst); err != nil {
			t.Errorf("%d. error with %q test: %s", i, tst.qry, err)
		}
		t.Logf("%d. %q result: %#v", i, tst.qry, tst.dst)
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
	for rows.Next() {
		if err = rows.Scan(&num, &str); err != nil {
			t.Errorf("error scanning row: %s", err)
		}
		//t.Logf("%d=%q", num, str)
	}
}

// TestClob
func TestClob(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

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
