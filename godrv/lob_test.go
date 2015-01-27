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
	"log"
	"sync"
	"testing"

	"github.com/tgulacsi/goracle/oracle"
)

func TestGetLob(t *testing.T) {
	conn := getConnection(t)
	defer conn.Close()

	text := "abcdefghijkl"
	stmt, err := conn.Prepare("SELECT TO_CLOB('" + text + "') FROM DUAL")
	if err != nil {
		log.Printf("error preparing query1: %v", err)
	}
	defer stmt.Close()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var clob *oracle.ExternalLobVar
			if err = stmt.QueryRow().Scan(&clob); err != nil {
				t.Errorf("Error scanning clob: %v", err)
			}
			defer clob.Close()
			t.Logf("clob=%v", clob)
			got, err := clob.ReadAll()
			if err != nil {
				t.Errorf("error reading clob: %v", err)
			}
			t.Logf("got=%q", got)
			if string(got) != text {
				t.Errorf("clob: got %q, awaited %q", got, text)
			}
		}()
	}
	wg.Wait()
}
