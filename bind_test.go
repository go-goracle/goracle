/*
Copyright 2014 Tamás Gulácsi

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
	"database/sql/driver"
	"testing"
	"time"
)

func TestOutBind(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	d := conn.Driver().(*Driver)
	cx, err := d.Open(*fDsn)
	if err != nil {
		t.Errorf("Open(%q): %v", *fDsn, err)
		return
	}
	defer cx.Close()

	stmt, err := cx.Prepare("BEGIN :1 := SYSDATE; END;")
	if err != nil {
		t.Errorf("Prepare: %v", err)
		return
	}
	var dt time.Time
	dtV, err := NewVar(stmt, &dt)
	if err != nil {
		t.Errorf("NewVar: %v", err)
		return
	}
	if _, err = stmt.Exec([]driver.Value{dtV}); err != nil {
		t.Errorf("Exec(%v): %v", dtV, err)
		return
	}
	t.Logf("dt=%v", dt)
	if dt.IsZero() {
		t.Errorf("zero time from %v", dtV)
	}
}
