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
	"sync"
	"testing"
)

func TestLobOutC(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	qry := `DECLARE
  clobvar CLOB;
  len     BINARY_INTEGER;
  x       VARCHAR2(80);
BEGIN
  dbms_lob.createtemporary(clobvar, TRUE);
  dbms_lob.open(clobvar, dbms_lob.lob_readwrite);
  x := 'before line break' || CHR(10) || 'after line break';
  len := length(x);
  dbms_lob.writeappend(clobvar, len, x);
  :1 := clobvar;
  dbms_lob.close(clobvar);
END;`

	if err := testLobOutC(cur, qry); err != nil {
		t.Errorf("error with _testLobOut: %s", err)
		t.FailNow()
	}

}

func TestGetLobConcurrent(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}

	text := "abcdefghijkl"

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			cur := conn.NewCursor()
			defer cur.Close()
			if err := cur.Prepare("SELECT TO_CLOB('"+text+"') FROM DUAL", ""); err != nil {
				t.Errorf("Prepare: %v", err)
				return
			}
			if err := cur.Execute("", nil, nil); err != nil {
				t.Errorf("Execute: %v", err)
				return
			}

			row, err := cur.FetchOne()
			if err != nil {
				t.Errorf("Fetch: %v", err)
				return
			}

			// close the underlying cursor, see whether it invalidates the LOB handle
			cur.Close()

			clob := row[0].(*ExternalLobVar)
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
