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
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"gopkg.in/goracle.v1/oracle"
)

func init() {
	IsDebug = true
}

func TestGoracleClobSelect(t *testing.T) {
	cx := getConnection(t)
	defer cx.Close()

	_, _ = cx.Exec("DROP TABLE CLOB_TEST")
	if _, err := cx.Exec("CREATE TABLE CLOB_TEST ( id NUMBER(38) NOT NULL, clobData CLOB)"); err != nil {
		t.Fatal(err)
	}

	test := strings.Repeat("test", 2000)

	user, passw, sid := oracle.SplitDSN(*fDsn)
	conn, err := oracle.NewConnection(user, passw, sid, false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cur := conn.NewCursor()
	defer cur.Close()
	clob, err := cur.NewVariable(0, oracle.ClobVarType, uint(len(test)))
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	err = clob.SetValue(0, []byte(test))
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	id := "123123123"
	err = cur.Execute("INSERT INTO CLOB_TEST (id, clobData) values (:1, :2)", []interface{}{id, clob}, nil)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	for i := 0; i < 10; i++ {
		err = cur.Execute("INSERT INTO CLOB_TEST (id, clobData) values (:1, :2)", []interface{}{strconv.Itoa(i), clob}, nil)
		if err != nil {
			t.Log(err)
			t.FailNow()
		}
	}
	conn.Commit()

	//works with one record
	cur.Execute("SELECT clobData FROM CLOB_TEST WHERE ID=123123123", nil, nil)
	c, err := cur.FetchOne()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	r, err := ioutil.ReadAll(c[0].(io.Reader))
	assert.Equal(t, test, (string(r)))

	s, err := cx.Query("SELECT clobData from CLOB_TEST")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	//First ok, second ko
	i := 0
	for s.Next() {
		var str interface{}
		s.Scan(&str)

		tBlob := str.(io.Reader)
		blobR, err := ioutil.ReadAll(tBlob)
		if err != nil {
			t.Log(err)
		}
		t.Logf("%d: %s", i, blobR)
		i++
	}
}

func TestGetLobConcurrentStmt(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	text := "abcdefghijkl"
	stmt, err := conn.Prepare("SELECT TO_CLOB('" + text + "') FROM DUAL")
	if err != nil {
		t.Errorf("error preparing query1: %v", err)
		return
	}
	defer stmt.Close()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			var clob *oracle.ExternalLobVar
			rows, err := stmt.Query()
			if err != nil {
				t.Errorf("query: %v", err)
				return
			}
			defer rows.Close()
			_ = rows.Next()
			if err = rows.Scan(&clob); err != nil {
				t.Errorf("Error scanning clob: %v", err)
				return
			}
			defer clob.Close()
			t.Logf("clob=%v", clob)
			got, err := clob.ReadAll()
			if err != nil {
				t.Errorf("error reading clob: %v", err)
				return
			}
			t.Logf("got=%q", got)
			if string(got) != text {
				t.Errorf("clob: got %q, awaited %q", got, text)
				return
			}
		}(text)
		//}(text + "-" + strconv.Itoa(i))
	}
	wg.Wait()
}
func TestGetLobConcurrent(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	text := "abcdefghijkl"

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			stmt, err := conn.Prepare("SELECT TO_CLOB('" + text + "') FROM DUAL")
			if err != nil {
				t.Errorf("error preparing query1: %v", err)
				return
			}
			defer stmt.Close()

			var clob *oracle.ExternalLobVar
			rows, err := stmt.Query()
			if err != nil {
				t.Errorf("query: %v", err)
				return
			}
			defer rows.Close()
			_ = rows.Next()
			if err = rows.Scan(&clob); err != nil {
				t.Errorf("Error scanning clob: %v", err)
				return
			}
			defer clob.Close()

			t.Logf("clob=%v", clob)
			got, err := clob.ReadAll()
			if err != nil {
				t.Errorf("error reading clob: %v", err)
				return
			}
			t.Logf("got=%q", got)
			if string(got) != text {
				t.Errorf("clob: got %q, awaited %q", got, text)
				return
			}
		}(text)
		//}(text + "-" + strconv.Itoa(i))
	}
	wg.Wait()
}
