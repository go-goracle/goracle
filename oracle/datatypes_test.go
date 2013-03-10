package oracle

import (
	"fmt"
	"testing"
	"time"
)

var dataTypesTests = []struct {
	in  string
	out string
}{
	{"SELECT 1 FROM DUAL", "%!s(float64=1)"},
	{"SELECT -1 FROM DUAL", "%!s(float64=-1)"},
	{"SELECT -1/4 FROM DUAL", "%!s(float64=-0.25)"},
	{"SELECT TO_DATE('2011-12-13 14:15:16', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL",
		"2011-12-13 14:15:16 +0100 CET"},
	{"SELECT 'AbraKA' FROM DUAL", "AbraKA"},
	{"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL", "árvíztűrő tükörfúrógép"},
	{"SELECT HEXTORAW('00') FROM DUAL", "\x00"},
	// {"SELECT TO_CLOB('árvíztűrő tükörfúrógép') FROM DUAL", "árvíztűrő tükörfúrógép"},
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

var bindsTests = []struct {
	in  interface{}
	out string
}{
	{1, "Typ=2 Len=2: c1,2"},
	{1.0 / 2, "Typ=2 Len=2: c0,33"},
	{-1.25, "Typ=2 Len=4: 3e,64,4c,66"},
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
	in_str  string
	out_val interface{}
	out_str string
}{
	{"3", int32(0), "3"},
	{"-10.24", float32(-10.24), "-10.24"},
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
		err          error
		qry, out_str string
		out          *Variable
	)
	for i, tt := range outBindsTests {
		qry = `BEGIN SELECT ` + tt.in_str + ` INTO :1 FROM DUAL; END;`
		if out, err = cur.NewVar(tt.out_val); err != nil {
			t.Errorf("error creating variable for %s(%T): %s", tt.out_val, tt.out_val, err)
		}
		if err = cur.Execute(qry, []interface{}{out}, nil); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if err = out.GetValueInto(&tt.out_val, 0); err != nil {
			t.Errorf("%d. error getting value: %s", i, err)
			continue
		}
		t.Logf("%d. out:%s %v", i, out, tt.out_val)
		out_str = fmt.Sprintf("%v", tt.out_val)
		if out_str != tt.out_str {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in_str, out_str, tt.out_str)
		}
	}
}

var inOutBindsTests = []struct {
	in_typ string
	in     interface{}
	out    string
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
		ok      bool
		err     error
		qry     string
		out     *Variable
		val     interface{}
		out_str string
	)

	for i, tt := range inOutBindsTests {
		qry = `DECLARE
	v_in ` + tt.in_typ + ` := :1;
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
		if out_str, ok = val.(string); !ok {
			t.Logf("output is not string!?!, but %T (%v)", val, val)
		}
		//t.Logf("%d. out:%s =?= %s", i, out_str, tt.out)
		if out_str != tt.out {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, out_str, tt.out)
		}
	}
}

var arrInBindsTests = []struct {
	tab_typ string
	in      interface{}
	out     string
}{
	{"INTEGER(3)", []int32{1, 3, 5}, "!3!1. Typ=2 Len=2: 193,2\n2. Typ=2 Len=2: 193,4\n3. Typ=2 Len=2: 193,6\n"},
	{"NUMBER(5,3)", []float32{1.0 / 2, -10.24}, "!2!1. Typ=2 Len=2: 192,51\n2. Typ=2 Len=10: 62,91,78,2,2,24,90,83,81,102\n"},
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
		err     error
		qry     string
		out     *Variable
		val     interface{}
		out_str string
		ok      bool
	)
	for i, tt := range arrInBindsTests {
		if out, err = cur.NewVar(""); err != nil {
			t.Errorf("cannot create out variable: %s", err)
			t.FailNow()
		}
		qry = `DECLARE
	TYPE tab_typ IS TABLE OF ` + tt.tab_typ + ` INDEX BY PLS_INTEGER;
	tab tab_typ := :1;
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
	:2 := v_out;
END;`
		if err = cur.Execute(qry, []interface{}{tt.in, out}, nil); err != nil {
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		if val, err = out.GetValue(0); err != nil {
			t.Errorf("%d. error getting value: %s", i, err)
			continue
		}
		if out_str, ok = val.(string); !ok {
			t.Logf("output is not string!?!, but %T (%v)", val, val)
		}
		//t.Logf("%d. in:%s => out:%v", i, out, out_str)
		if out_str != tt.out {
			t.Errorf("%d. exec(%q) => %q, want %q", i, tt.in, out_str, tt.out)
		}
	}
}

var arrOutBindsTests = []struct {
	tab_typ string
	in      interface{}
	out     []string
}{
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
		err     error
		qry     string
		out     *Variable
		val     interface{}
		out_str string
		ok      bool
	)
	placeholder := string(make([]byte, 100))
	for i, tt := range arrOutBindsTests {
		//if out, err = cur.NewVar(""); err != nil {
		//if out, err = cur.NewVar([]string{"01234567890123456789", "01234567890123456789"}); err != nil {
		if out, err = cur.NewVariableArrayByValue(placeholder, 10); err != nil {
			t.Errorf("cannot create out variable: %s", err)
			t.FailNow()
		}
		qry = `DECLARE
	TYPE in_tab_typ IS TABLE OF ` + tt.tab_typ + ` INDEX BY PLS_INTEGER;
	in_tab in_tab_typ := :inp;
	TYPE out_tab_typ IS TABLE OF VARCHAR2(1000) INDEX BY PLS_INTEGER;
	out_tab out_tab_typ;
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
			t.Errorf("error executing `%s`: %s", qry, err)
			continue
		}
		n := out.ArrayLength()
		//n = 2
		for j := uint(0); j < n; j++ {
			if val, err = out.GetValue(j); err != nil {
				t.Errorf("%d. error getting %d. value: %s", i, j, err)
				continue
			}
			if out_str, ok = val.(string); !ok {
				t.Logf("%d/%d. output is not string!?!, but %T (%v)", i, j, val, val)
			}
			t.Logf("%d/%d. => out:%#v", i, j, out_str)
			if j < uint(len(tt.out)) && out_str != tt.out[j] {
				t.Errorf("%d. exec(%q)[%d]\n got %q,\nwant %q", i, tt.in, j,
					out_str, tt.out[j])
			}
		}
	}
}

func TestCursorOut(t *testing.T) {
	conn := getConnection(t)
	if !conn.IsConnected() {
		t.FailNow()
	}
	cur := conn.NewCursor()
	defer cur.Close()
	cur_out := conn.NewCursor()
	defer cur_out.Close()

	var (
		err error
		row []interface{}
	)
	qry := `DECLARE
  v_cur SYS_REFCURSOR;
BEGIN
  OPEN v_cur FOR
    SELECT * FROM all_objects;
  :1 := v_cur;
END;`
	if err = cur.Execute(qry, []interface{}{cur_out}, nil); err != nil {
		t.Errorf("error executing `%s`: %s", qry, err)
		t.FailNow()
	}
	if row, err = cur_out.FetchOne(); err != nil {
		t.Errorf("cannot fetch row: %s", err)
		t.Fail()
	}

	t.Logf("row: %#v", row)
}
