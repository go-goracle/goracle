package oracle

import (
	"fmt"
	"testing"
)

var dataTypesTests = []struct {
	in  string
	out string
}{
	{"SELECT 1 FROM DUAL", "%!s(int64=1)"},
	{"SELECT -1 FROM DUAL", "%!s(int64=-1)"},
	{"SELECT -1/4 FROM DUAL", "-0.25"},
	{"SELECT TO_DATE('2011-12-13 14:15:16', 'YYYY-MM-DD HH24:MI:SS') FROM DUAL",
		"2011-12-13 14:15:16 +0100 CET"},
	{"SELECT 'árvíztűrő tükörfúrógép' FROM DUAL", "árvíztűrő tükörfúrógép"},
}

func TestSimplTypes(t *testing.T) {
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
