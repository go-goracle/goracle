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

package godrv

import (
	"sync"
	"testing"

	"gopkg.in/goracle.v1/oracle"
)

func init() {
	IsDebug = true
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
