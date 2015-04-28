/*
Copyright 2015 Tamás Gulácsi

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

package goracle

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"gopkg.in/errgo.v1"
)

func TestCS(t *testing.T) {
	conn, tx := prepareTableCS(t)
	defer conn.Close()
	defer tx.Rollback()

	for i, txt := range []string{"Habitación doble", "雙人房", "двухместный номер"} {
		if err := insertCS(tx, txt); err != nil {
			t.Errorf("%d. %v", i, err)
		}
	}
}

var id int32

func insertCS(conn *sql.Tx, txt string) error {
	qry := "INSERT INTO tst_goracle_cs (F_id, F_txt, F_hex)" +
		" VALUES (?, ?, RAWTOHEX(UTL_RAW.CAST_TO_RAW(?)))"
	id := atomic.AddInt32(&id, 1)
	if _, err := conn.Exec(qry, id, txt, txt); err != nil {
		return fmt.Errorf("cannot insert into tst_goracle_cs (%q): %v", qry, err)
	}
	row := conn.QueryRow("SELECT F_txt, RAWTOHEX(UTL_RAW.CAST_TO_RAW(F_txt)), F_hex FROM tst_goracle_cs WHERE F_id = ?", id)
	var sTxt, sTxtRH, sHex string
	if err := row.Scan(&sTxt, &sTxtRH, &sHex); err != nil {
		return fmt.Errorf("error scanning row: %v", errgo.Details(err))
	}
	//t.Logf("txt=%q raw=%q hex=%q", sTxt, sTxtRH, sHex)
	if sTxt != txt {
		return fmt.Errorf("got txt=%q != %q", sTxt, txt)
	}
	if sTxtRH != sHex {
		return fmt.Errorf("got hex=%q != %q", sTxtRH, sHex)
	}
	if sHex != strToHex(txt) {
		return fmt.Errorf("got hex=%q != %q", sHex, strToHex(txt))
	}
	return nil
}

func strToHex(txt string) string {
	return strings.ToUpper(hex.EncodeToString([]byte(txt)))
}

func prepareTableCS(t *testing.T) (*sql.DB, *sql.Tx) {
	conn := getConnection(t)
	conn.Exec("DROP TABLE tst_goracle_cs")
	if _, err := conn.Exec(`CREATE TABLE tst_goracle_cs (
		F_id NUMBER(9),
		F_txt VARCHAR2(255),
		F_hex VARCHAR2(510))`,
	); err != nil {
		t.Skipf("Skipping table test, as cannot create tst_goracle_cs: %v", err)
		conn.Close()
		return nil, nil
	}
	//defer conn.Exec("DROP TABLE " + tbl)
	tx, err := conn.Begin()
	if err != nil {
		conn.Close()
		t.Errorf("cannot start transaction: %v", err)
		t.FailNow()
	}
	return conn, tx
}
