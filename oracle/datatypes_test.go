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
