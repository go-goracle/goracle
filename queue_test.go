// Copyright 2019 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package goracle_test

import (
	"context"
	"testing"
	"time"

	goracle "gopkg.in/goracle.v2"
)

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const qName = "TEST_Q"
	if _, err = conn.ExecContext(ctx, "BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(:1, 'RAW'); END;", qName); err != nil {
		t.Fatal(err)
	}
	defer func() {
		conn.ExecContext(context.Background(), "BEGIN DBMS_AQADM.DROP_QUEUE(:1); END;", qName)
	}()

	q, err := goracle.NewQueue(conn, qName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	t.Log("name:", q.Name())
	enqOpts, err := q.EnqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("enqOpts:", enqOpts)
	deqOpts, err := q.DeqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("deqOpts:", deqOpts)

	if err = q.Enqueue([]goracle.Message{goracle.Message{Raw: []byte("árvíztűrő tükörfúrógép")}}); err != nil {
		t.Fatal("enqueue:", err)
	}
	msgs := make([]goracle.Message, 1)
	n, err := q.Dequeue(msgs)
	if err != nil {
		t.Error("dequeue:", err)
	}
	t.Logf("received %d messages", n)
	for _, m := range msgs[:n] {
		t.Log("got:", m)
	}
}
