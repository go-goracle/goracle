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
	"log"
	"testing"
	"time"

	"github.com/juju/errgo"
)

/*
declare
  ival interval day(0) to second(0);
begin
  select interval '05:30' hour to minute into ival from dual;
  dbms_output.put_line( ival );
end;
*/
var accented = "árvíztűrő tükörfúrógép"

var dataTypesTests = []struct {
	in  string
	out string
}{
	{"SELECT 1 FROM DUAL", "%!s(int32=1)"},
	{"SELECT -1 FROM DUAL", "%!s(int32=-1)"},
	{"SELECT 9876 FROM DUAL", "%!s(int32=9876)"},
	{"SELECT 9999999999 FROM DUAL", "9999999999"},
	{"SELECT -1/4 FROM DUAL", "-.25"},
	{"SELECT TO_DATE('2011-12-13 14:15:16', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL",
		"2011-12-13 14:15:16 +0100 CET"},
	{"SELECT 'AbraKA' FROM DUAL", "AbraKA"},
	{"SELECT NULL FROM DUAL", "%!s(<nil>)"},
	{"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL", "árvíztűrő tükörfúrógép"},
	{"SELECT HEXTORAW('00') FROM DUAL", "\x00"},
	{"SELECT INTERVAL '05:30' HOUR TO MINUTE FROM DUAL", "5h30m0s"},
	{"SELECT TO_CLOB('árvíztűrő tükörfúrógép') FROM DUAL", "string(árvíztűrő tükörfúrógép)"},
}

func TestSimpleTypes(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	var (
		row []interface{}
	)
	cur := conn.NewCursor()
	defer cur.Close()
	{
		if oci, client, db, err := conn.NlsSettings(cur); err != nil {
			t.Logf("NlsSettings: %s", err)
		} else {
			t.Logf("NLS oci=%s client=%s database=%s", oci, client, db)
		}
	}

	var (
		repr string
		err  error
	)
	for i, tt := range dataTypesTests {
		if err = cur.Execute(tt.in, nil, nil); err != nil {
			t.Errorf("error executing `%s`: %s", tt.in, err)
		} else {
			if row, err = cur.FetchOne(); err != nil {
				t.Errorf("cannot fetch row (%q): %s", tt.in, err)
			} else {
				if ex, ok := row[0].(*ExternalLobVar); ok {
					var reprB []byte
					if reprB, err = ex.ReadAll(); err != nil {
						t.Errorf("error reading LOB %s: %s", ex, err)
					}
					repr = "string(" + string(reprB) + ")"
				} else {
					repr = fmt.Sprintf("%s", row[0])
				}
				if repr != tt.out {
					t.Errorf("%d. exec(%q) => %q (%#v), want %q", i, tt.in, row[0], repr, tt.out)
				}
			}
		}
	}
}

var bindsTests = []struct {
	in  interface{}
	out string
}{
	{1, "Typ=2 Len=2: c1,2"},
	{1.0 / 2, "Typ=2 Len=2: c0,33"},
	{-1.25, "Typ=2 Len=4: 3e,64,4c,66"},
	{nil, "NULL"},
	{"SELECT", "Typ=1 Len=6 CharacterSet=AL32UTF8: 53,45,4c,45,43,54"},
	{"árvíztűrő tükörfúrógép", "Typ=1 Len=31 CharacterSet=AL32UTF8: c3,a1,72,76,c3,ad,7a,74,c5,b1,72,c5,91,20,74,c3,bc,6b,c3,b6,72,66,c3,ba,72,c3,b3,67,c3,a9,70"},
	{[]byte{0, 1, 2, 3, 5, 7, 11, 13}, "Typ=23 Len=8: 0,1,2,3,5,7,b,d"},
	{time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
		"Typ=12 Len=7: 78,71,1,2,b,7,32"},
}

func TestSimpleBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		err  error
		row  []interface{}
		repr string
	)
	for i, tt := range bindsTests {
		if err = cur.Execute("SELECT DUMP(:1, 1016) FROM DUAL", []interface{}{tt.in}, nil); err != nil {
			t.Errorf("error executing `%s`: %s", tt.in, err)
		} else {
			if row, err = cur.FetchOne(); err != nil {
				t.Errorf("cannot fetch row: %s", err)
			} else {
				repr = fmt.Sprintf("%s", row[0])
				if repr != tt.out {
					t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, repr, tt.out)
				}
			}
		}
	}
}

var outBindsTests = []struct {
	inStr  string
	outVal interface{}
	outStr string
}{
	{"3", int32(0), "3"},
	{"-10.24", float32(-10.24), "-10.24"},
	{"9999999999", int64(999999999), "9999999999"},
	{"TO_NUMBER(12345678901)", int64(12345678901), "12345678901"},
	{"TO_NUMBER(12345678901234567890)", "12345678901234567890", "12345678901234567890"},
	// {"VARCHAR2(40)", []string{"SELECT", "árvíztűrő tükörfúrógép"}, "Typ=1 Len=6 CharacterSet=AL32UTF8: 53,45,4c,45,43,54"},
	// {"RAW(4)", [][]byte{[]byte{0, 1, 2, 3}, []byte{5, 7, 11, 13}}, "Typ=23 Len=8: 0,1,2,3,5,7,b,d"},
	// {"DATE", []time.Time{time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
	// 	time.Date(2012, 1, 2, 10, 6, 49, 0, time.Local)},
	// 	"Typ=12 Len=7: 78,71,1,2,b,7,32"},
}

func TestOutBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		err         error
		qry, outStr string
		out         *Variable
	)
	for i, tt := range outBindsTests {
		qry = `BEGIN SELECT ` + tt.inStr + ` INTO :1 FROM DUAL; END;`
		if out, err = cur.NewVar(tt.outVal); err != nil {
			t.Errorf("error creating variable for %s(%T): %s", tt.outVal, tt.outVal, err)
		}
		if err = cur.Execute(qry, []interface{}{out}, nil); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if err = out.GetValueInto(&tt.outVal, 0); err != nil {
			t.Errorf("%d. error getting value: %s", i, errgo.Details(err))
			continue
		}
		t.Logf("%d. out:%s %v", i, out, tt.outVal)
		outStr = fmt.Sprintf("%v", tt.outVal)
		if outStr != tt.outStr {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.inStr, outStr, tt.outStr)
		}
	}
}

var inOutBindsTests = []struct {
	inTyp string
	in    interface{}
	out   string
}{
	{"INTEGER(3)", int32(1), "Typ=2 Len=2: 193,2"},
	{"NUMBER(5,3)", float32(1.0 / 2), "Typ=2 Len=2: 192,51"},
	{"VARCHAR2(40)", "árvíztűrő tükörfúrógép",
		"Typ=1 Len=31: 195,161,114,118,195,173,122,116,197,177,114,197,145,32,116,195,188,107,195,182,114,102,195,186,114,195,179,103,195,169,112"},
	{"DATE", time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
		"Typ=12 Len=7: 120,113,1,2,11,7,50"},
}

func TestInOutBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		ok     bool
		err    error
		qry    string
		out    *Variable
		val    interface{}
		outStr string
	)

	for i, tt := range inOutBindsTests {
		qry = `DECLARE
	v_in ` + tt.inTyp + ` := :1;
	v_out VARCHAR2(1000);
BEGIN
	SELECT DUMP(v_in) INTO v_out FROM DUAL;
	:2 := v_out;
END;`
		if out, err = cur.NewVar(""); err != nil {
			t.Errorf("error creating output variable: %s", err)
			t.FailNow()
		}
		if err = cur.Execute(qry, []interface{}{tt.in, out}, nil); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if val, err = out.GetValue(0); err != nil {
			t.Errorf("%d. error getting value: %s", i, err)
			continue
		}
		if outStr, ok = val.(string); !ok {
			t.Logf("output is not string!?!, but %T (%v)", val, val)
		}
		//t.Logf("%d. out:%s =?= %s", i, outStr, tt.out)
		if outStr != tt.out {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, outStr, tt.out)
		}
	}
}

var arrInBindsTests = []struct {
	tabTyp string
	in     interface{}
	out    string
}{
	{"INTEGER(3)", []int32{1, 3, 5}, "!3!1. Typ=2 Len=2: 193,2\n2. Typ=2 Len=2: 193,4\n3. Typ=2 Len=2: 193,6\n"},
	//{"PLS_INTEGER", []int32{1, 3, 5}, "!3!1. Typ=2 Len=2: 193,2\n2. Typ=2 Len=1: 128\n3. Typ=2 Len=2: 193,4\n"},
	{"NUMBER(5,3)", []float32{1.0 / 2, -10.24}, "!2!1. Typ=2 Len=2: 192,51\n2. Typ=2 Len=10: 62,91,78,2,2,24,90,83,81,102\n"},
	{"VARCHAR2(6)", []string{"KEDV01", "KEDV02"}, "!2!1. Typ=1 Len=6: 75,69,68,86,48,49\n2. Typ=1 Len=6: 75,69,68,86,48,50\n"},
	{"VARCHAR2(40)", []string{"SELECT", "árvíztűrő tükörfúrógép"},
		"!2!1. Typ=1 Len=6: 83,69,76,69,67,84\n2. Typ=1 Len=31: 195,161,114,118,195,173,122,116,197,177,114,197,145,32,116,195,188,107,195,182,114,102,195,186,114,195,179,103,195,169,112\n"},
	{"RAW(4)", [][]byte{[]byte{0, 1, 2, 3}, []byte{5, 7, 11, 13}},
		"!2!1. Typ=23 Len=4: 0,1,2,3\n2. Typ=23 Len=4: 5,7,11,13\n"},
	{"DATE", []time.Time{time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
		time.Date(2012, 1, 2, 10, 6, 49, 0, time.Local)},
		"!2!1. Typ=12 Len=7: 120,113,1,2,11,7,50\n2. Typ=12 Len=7: 120,112,1,2,11,7,50\n"},
}

func TestArrayInBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		err    error
		qry    string
		out    *Variable
		val    interface{}
		outStr string
		ok     bool
	)
	for i, tt := range arrInBindsTests {
		if out, err = cur.NewVar(""); err != nil {
			t.Errorf("cannot create out variable: %s", err)
			t.FailNow()
		}
		qry = `DECLARE
	TYPE tabTyp IS TABLE OF ` + tt.tabTyp + ` INDEX BY PLS_INTEGER;
	tab tabTyp := :in;
	v_idx PLS_INTEGER;
	v_out VARCHAR2(1000) := '!';
BEGIN
    v_out := v_out||tab.COUNT||'!';
	v_idx := tab.FIRST;
	IF FALSE and v_idx IS NULL THEN
		v_out := v_out||'EMPTY';
	END IF;
	WHILE v_idx IS NOT NULL LOOP
	    SELECT v_out||v_idx||'. '||DUMP(tab(v_idx))||CHR(10) INTO v_out FROM DUAL;
		v_idx := tab.NEXT(v_idx);
	END LOOP;
	:out := v_out;
END;`
		in, err := cur.NewVar(tt.in)
		if err != nil {
			t.Errorf("%d. error with NewVar: %v", i, err)
			continue
		}
		if err = cur.Execute(qry, nil, map[string]interface{}{"in": in, "out": out}); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if val, err = out.GetValue(0); err != nil {
			t.Errorf("%d. error getting value: %s", i, err)
			continue
		}
		if outStr, ok = val.(string); !ok {
			t.Logf("output is not string!?!, but %T (%v)", val, val)
		}
		//t.Logf("%d. in:%s => out:%v", i, out, outStr)
		if outStr != tt.out {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, outStr, tt.out)
		}
	}
}

var arrOutBindsTests = []struct {
	tabTyp string
	in     interface{}
	out    []string
}{
	{"INTEGER", []int32{-2, -4, -8}, []string{"Typ=2 Len=3: 62,99,102", "Typ=2 Len=3: 62,97,102", "Typ=2 Len=3: 62,93,102"}},
	{"INTEGER(3)", []int32{1, 3, 5}, []string{"Typ=2 Len=2: 193,2", "Typ=2 Len=2: 193,4", "Typ=2 Len=2: 193,6"}},
	{"NUMBER(5,3)", []float32{1.0 / 2, -10.24}, []string{"Typ=2 Len=2: 192,51", "Typ=2 Len=10: 62,91,78,2,2,24,90,83,81,102"}},
	{"VARCHAR2(40)", []string{"SELECT    012345678901234567890123456789", "árvíztűrő tükörfúrógép"},
		[]string{"Typ=1 Len=40: 83,69,76,69,67,84,32,32,32,32,48,49,50,51,52,53,54,55,56,57,48,49,50,51,52,53,54,55,56,57,48,49,50,51,52,53,54,55,56,57",
			"Typ=1 Len=31: 195,161,114,118,195,173,122,116,197,177,114,197,145,32,116,195,188,107,195,182,114,102,195,186,114,195,179,103,195,169,112",
		}},
	{"RAW(4)", [][]byte{[]byte{0, 1, 2, 3}, []byte{5, 7, 11, 13}},
		[]string{"Typ=23 Len=4: 0,1,2,3", "Typ=23 Len=4: 5,7,11,13"}},
	{"DATE", []time.Time{time.Date(2013, 1, 2, 10, 6, 49, 0, time.Local),
		time.Date(2012, 1, 2, 10, 6, 49, 0, time.Local)},
		[]string{"Typ=12 Len=7: 120,113,1,2,11,7,50", "Typ=12 Len=7: 120,112,1,2,11,7,50"}},
}

func TestArrayOutBinds(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	var (
		err    error
		qry    string
		out    *Variable
		val    interface{}
		outStr string
		ok     bool
	)
	placeholder := string(make([]byte, 1000))
	for i, tt := range arrOutBindsTests {
		//if out, err = cur.NewVar(""); err != nil {
		//if out, err = cur.NewVar([]string{"01234567890123456789", "01234567890123456789"}); err != nil {
		if out, err = cur.NewVariableArrayByValue(placeholder, 100); err != nil {
			t.Errorf("cannot create out variable: %s", err)
			t.FailNow()
		}
		qry = `DECLARE
	TYPE in_tabTyp IS TABLE OF ` + tt.tabTyp + ` INDEX BY PLS_INTEGER;
	in_tab in_tabTyp := :inp;
	TYPE out_tabTyp IS TABLE OF VARCHAR2(1000) INDEX BY PLS_INTEGER;
	out_tab out_tabTyp;
	v_idx PLS_INTEGER;
BEGIN
	v_idx := in_tab.FIRST;
	WHILE v_idx IS NOT NULL LOOP
	    SELECT SUBSTR(DUMP(in_tab(v_idx)), 1, 1000) INTO out_tab(v_idx) FROM DUAL;
		v_idx := in_tab.NEXT(v_idx);
	END LOOP;
	:out := out_tab;
END;`
		if err = cur.Execute(qry, nil,
			map[string]interface{}{"inp": tt.in, "out": out}); err != nil {
			t.Errorf("%d. error executing `%s`: %s", i, qry, err)
			continue
		}
		n := out.ArrayLength()
		//n = 2
		for j := uint(0); j < n; j++ {
			if val, err = out.GetValue(j); err != nil {
				t.Errorf("%d. error getting %d. value: %s", i, j, err)
				continue
			}
			if outStr, ok = val.(string); !ok {
				t.Logf("%d/%d. output is not string!?!, but %T (%v)", i, j, val, val)
			}
			t.Logf("%d/%d. => out:%#v", i, j, outStr)
			if j < uint(len(tt.out)) && outStr != tt.out[j] {
				t.Errorf("%d. exec(%q)[%d]\n got %q,\nwant %q", i, tt.in, j,
					outStr, tt.out[j])
			}
		}
	}
}

func TestCursorOut(t *testing.T) {
	//IsDebug = true

	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()
	out, err := cur.NewVariable(0, CursorVarType, 0)
	if err != nil {
		t.Errorf("error getting cursor variable: %s", err)
		t.FailNow()
	}

	var (
		row []interface{}
	)
	qry := `DECLARE
  v_cur SYS_REFCURSOR;
BEGIN
  OPEN v_cur FOR
    SELECT * FROM all_objects;
  :1 := v_cur;
END;`
	if err = cur.Execute(qry, []interface{}{out}, nil); err != nil {
		t.Errorf("error executing `%s`: %s", qry, err)
		t.FailNow()
	}
	outVal, err := out.GetValue(0)
	if err != nil {
		t.Errorf("cannot get out value: %s", err)
		t.FailNow()
	}
	outCur, ok := outVal.(*Cursor)
	if !ok {
		t.Errorf("got %v (%T), required cursor.", outCur, outCur)
		t.FailNow()
	}
	defer outCur.Close()
	if row, err = outCur.FetchOne(); err != nil {
		t.Errorf("cannot fetch row: %s", err)
		t.Fail()
	}

	t.Logf("row: %#v", row)
}

func TestLobOut(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()

	str := "before line break\nafter line break\n" + accented
	for i, rec := range []struct {
		qry  string
		vtyp *VariableType
	}{
		{`DECLARE
  clobvar CLOB;
  len     BINARY_INTEGER;
  x       VARCHAR2(32767);
BEGIN
  dbms_lob.createtemporary(clobvar, TRUE);
  dbms_lob.open(clobvar, dbms_lob.lob_readwrite);
  x := '` + str + `';
  len := length(x);
  dbms_lob.writeappend(clobvar, len, x);
  :1 := clobvar;
  dbms_lob.close(clobvar);
END;`, ClobVarType},
		{`DECLARE
  blobvar BLOB;
  len     BINARY_INTEGER;
  x       RAW(32767);
BEGIN
  dbms_lob.createtemporary(blobvar, TRUE);
  dbms_lob.open(blobvar, dbms_lob.lob_readwrite);
  x := UTL_RAW.CAST_TO_RAW('` + str + `');
  len := UTL_RAW.length(x);

  DBMS_LOB.writeappend(blobvar, len, x);

  :1 := blobvar;

  dbms_lob.close(blobvar);
END;`, BlobVarType},
	} {
		out, err := cur.NewVariable(0, rec.vtyp, 0)
		if err != nil {
			t.Errorf("%d. error getting cursor variable: %s", i, err)
			t.FailNow()
		}
		if err = cur.Execute(rec.qry, []interface{}{out}, nil); err != nil {
			t.Errorf("%d. error executing `%s`: %s", i, rec.qry, err)
			t.FailNow()
		}
		outVal, err := out.GetValue(0)
		if err != nil {
			t.Errorf("%d. cannot get out value: %s", i, err)
			t.FailNow()
		}
		log.Printf("%d. outVal: %T %s", i, outVal, outVal)
		ext, ok := outVal.(*ExternalLobVar)
		if !ok {
			t.Errorf("%d. outVal is not *ExternalLobVar, but %T", i, outVal)
			t.FailNow()
		}
		buf, err := ext.ReadAll()
		if err != nil {
			t.Errorf("%d. error reading LOB: %s", i, err)
			t.FailNow()
		}
		t.Logf("%d. read %q (%d)", i, buf, len(buf))
		if len(buf) != len(str) {
			t.Errorf("%d. read %q from the buffer (%d bytes), awaited %q (%d bytes)",
				i, buf, len(buf), str, len(str))
			t.Fail()
		}
	}
}
