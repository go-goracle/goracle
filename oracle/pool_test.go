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

package oracle

import (
	"testing"

	"github.com/tgulacsi/go/loghlp/tsthlp"
)

func TestPool(t *testing.T) {
	Log.SetHandler(tsthlp.TestHandler(t))

	p := NewPool(*dsn)
	cx1, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("1. %p", cx1.handle)
	defer cx1.Close()
	cx2, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("2. %p", cx2.handle)
	if cx1.handle == cx2.handle {
		t.Errorf("got the same for second, wanted different")
	}
	defer cx2.Close()
	p.Put(cx2)
	cx3, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("3. %p", cx3.handle)
	defer cx3.Close()
	if cx3.handle != cx2.handle {
		t.Errorf("put back %v, got %v", cx2, cx3)
	}
}
