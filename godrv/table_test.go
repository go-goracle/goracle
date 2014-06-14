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
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/juju/errgo"
)

const tbl = "tst_goracle_godrv"

func TestTable(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()
	conn.Exec("DROP TABLE " + tbl)
	if _, err := conn.Exec(`CREATE TABLE ` + tbl + ` (
			F_int NUMBER(10,0), F_bigint NUMBER(20),
			F_real NUMBER(6,3), F_bigreal NUMBER(20,10),
			F_text VARCHAR2(1000), F_date DATE,
			F_text_spanish VARCHAR2(100),
			F_text_chinese VARCHAR2(100),
			F_text_russian VARCHAR2(100)
		)`); err != nil {
		t.Skipf("Skipping table test, as cannot create "+tbl+": %v", err)
		return
	}
	defer conn.Exec("DROP TABLE " + tbl)
	tx, err := conn.Begin()
	if err != nil {
		t.Errorf("cannot start transaction: %v", err)
		t.FailNow()
	}
	defer tx.Rollback()

	insert_num(t, tx, 1, "1234567890123456", 123.456,
		"123456789.123456789", "int64", time.Now())

	insert_num(t, tx, 2, "22345678901234567890", 223.456,
		"223456789.123456789", "big.Int", time.Now())

	insert_text(t, tx, "Habitación doble", "雙人房", "двухместный номер")
}

func insert_text(t *testing.T, conn *sql.Tx, spanish, chinese, russian string) bool {
	qry := "INSERT INTO " + tbl + " (F_int, F_text_spanish, F_text_chinese, F_text_russian)" +
		" VALUES (-1, :1, :2, :3)"
	if _, err := conn.Exec(qry, spanish, chinese, russian); err != nil {
		t.Errorf("cannot insert into "+tbl+" (%q): %v", qry, err)
	}
	row := conn.QueryRow("SELECT F_text_spanish, F_text_chinese, F_text_russian FROM " + tbl + " WHERE F_int = -1")
	var tSpanish, tChinese, tRussian string
	if err := row.Scan(&tSpanish, &tChinese, &tRussian); err != nil {
		t.Errorf("error scanning row: %v", errgo.Details(err))
		return false
	}
	t.Logf("spanish=%q chinese=%q russian=%q", spanish, chinese, russian)
	ok := true
	if tSpanish != spanish {
		t.Errorf("spanish mismatch: got %q, awaited %q", tSpanish, spanish)
		ok = false
	}
	if tChinese != chinese {
		t.Errorf("chinese mismatch: got %q, awaited %q", tChinese, chinese)
		ok = false
	}
	if tRussian != russian {
		t.Errorf("russian mismatch: got %q, awaited %q", tRussian, russian)
		ok = false
	}
	return ok
}

func insert_num(t *testing.T, conn *sql.Tx,
	small int, bigint string,
	notint float64, bigreal string,
	text string, date time.Time,
) bool {
	date = date.Round(time.Second)
	qry := fmt.Sprintf(`INSERT INTO `+tbl+`
			(F_int, F_bigint, F_real, F_bigreal, F_text, F_date)
			VALUES (%d, %s, %3.3f, %s, '%s', TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))
			`, small, bigint, notint, bigreal, text, date.Format("2006-01-02 15:04:05"))
	if _, err := conn.Exec(qry); err != nil {
		t.Errorf("cannot insert into "+tbl+" (%q): %v", qry, err)
		return false
	}
	row := conn.QueryRow("SELECT F_int, F_bigint, F_real, F_bigreal, F_text, F_date FROM "+tbl+" WHERE F_int = :1", small)
	var (
		smallO             int
		bigintO            big.Int
		notintO            float64
		bigrealF, bigrealO big.Rat
		bigintS, bigrealS  string
		textO              string
		dateO              time.Time
	)
	if err := row.Scan(&smallO, &bigintS, &notintO, &bigrealS, &textO, &dateO); err != nil {
		t.Errorf("error scanning row[%d]: %v", small, errgo.Details(err))
		return false
	}
	t.Logf("row: small=%d big=%s notint=%f bigreal=%s text=%q date=%s",
		smallO, bigintS, notintO, bigrealS, textO, dateO)

	if smallO != small {
		t.Errorf("small mismatch: got %d, awaited %d.", smallO, small)
	}
	(&bigintO).SetString(bigintS, 10)
	if bigintO.String() != bigint {
		t.Errorf("bigint mismatch: got %s, awaited %s.", bigintO, bigint)
	}
	if notintO != notint {
		t.Errorf("noting mismatch: got %f, awaited %f.", notintO, notint)
	}
	(&bigrealF).SetString(bigreal)
	(&bigrealO).SetString(bigrealS)
	if (&bigrealO).Cmp(&bigrealF) != 0 {
		t.Errorf("bigreal mismatch: got %s, awaited %f.", (&bigrealO), (&bigrealF))
	}
	if textO != text {
		t.Errorf("text mismatch: got %q, awaited %q.", textO, text)
	}
	if !dateO.Equal(date) {
		t.Errorf("date mismatch: got %s, awaited %s.", dateO, date.Round(time.Second))
	}

	return true
}
