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
	"testing"
	"time"
)

func TestReuseBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		err             error
		timVar, textVar *Variable
		tim             = time.Now()
		text            string
	)
	if timVar, err = cur.NewVar(&tim); err != nil {
		t.Errorf("error creating variable for %s(%T): %s", tim, tim, err)
	}
	if textVar, err = cur.NewVar(&text); err != nil {
		t.Errorf("error creating variable for %s(%T): %s", tim, tim, err)
	}
	qry2 := `BEGIN SELECT SYSDATE, TO_CHAR(SYSDATE) INTO :1, :2 FROM DUAL; END;`
	if err = cur.Execute(qry2, []interface{}{timVar, textVar}, nil); err != nil {
		t.Errorf("error executing `%s`: %s", qry2, err)
		return
	}
	t.Logf("1. tim=%q text=%q", tim, text)
	qry1 := `BEGIN SELECT SYSDATE INTO :1 FROM DUAL; END;`
	if err = cur.Execute(qry1, []interface{}{timVar}, nil); err != nil {
		t.Errorf("error executing `%s`: %s", qry1, err)
		return
	}
	t.Logf("2. tim=%q", tim)

	if err = cur.Execute(qry2, nil, map[string]interface{}{"1": timVar, "2": textVar}); err != nil {
		t.Errorf("error executing `%s`: %s", qry2, err)
		return
	}
	t.Logf("2. tim=%q text=%q", tim, text)
	if err = cur.Execute(qry1, nil, map[string]interface{}{"1": timVar}); err != nil {
		t.Errorf("error executing `%s`: %s", qry1, err)
		return
	}
	t.Logf("3. tim=%q", tim)
}

func TestBindWithoutBind(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var err error
	output := string(make([]byte, 4000))
	qry := `BEGIN SELECT DUMP(:1) INTO :2 FROM DUAL; END;`
	testTable := []struct {
		input interface{}
		await string
	}{
		{"A", "Typ=1 Len=1: 65"},
	}
	for i, tup := range testTable {
		if err = cur.Execute(qry, []interface{}{tup.input.(string), &output}, nil); err != nil {
			t.Errorf("%d. error executing %q: %v", i, qry, err)
			continue
		}
		if output != tup.await {
			t.Errorf("%d. awaited %q, got %q", i, tup.await, output)
		}
	}
}
