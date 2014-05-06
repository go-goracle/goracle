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

import (
	"fmt"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	defer conn.Close()
	cur := conn.NewCursor()
	defer cur.Close()
	if err := cur.Execute(`CREATE TABLE tst_goracle (
			F_int NUMBER(3), F_bigint NUMBER(20),
			F_real NUMBER(6,3), F_bigreal NUMBER(20,10),
			F_text VARCHAR2(1000), F_date DATE
		)`, nil, nil); err != nil {
		t.Skipf("Skipping table test, as cannot create tst_goracle: %v", err)
		return
	}
	defer cur.Execute("DROP TABLE tst_goracle", nil, nil)
	if !insert(t, cur, 1, "1234567890123456", 123.456,
		"123456789.123456789", "int64", time.Now()) {
		return
	}

	if !insert(t, cur, 2, "22345678901234567890", 223.456,
		"223456789.123456789", "big.Int", time.Now()) {
		return
	}
}

func insert(t *testing.T, cur *Cursor,
	small int, bigint string,
	notint float64, bigreal string,
	text string, date time.Time,
) bool {
	qry := fmt.Sprintf(`INSERT INTO tst_goracle
			(F_int, F_bigint, F_real, F_bigreal, F_text, F_date)
			VALUES (%d, %s, %3.3f, %s, '%s', TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))
			`, small, bigint, notint, bigreal, text, date.Format("2006-01-02 15:04:05"))
	if err := cur.Execute(qry, nil, nil); err != nil {
		t.Errorf("cannot insert into tst_goracle (%q): %v", qry, err)
		return false
	}
	if err := cur.Execute("SELECT * FROM tst_goracle WHERE F_int = :1", []interface{}{small}, nil); err != nil {
		t.Errorf("error selecting tst_goracle: %v", err)
		return false
	}
	row, err := cur.FetchOne()
	if err != nil {
		t.Errorf("error fetching row: %v", err)
		return false
	}
	t.Logf("row: %#v", row)

	return true
}
