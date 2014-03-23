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

// clear && go test -v -test.run Test_callBuildStatement_Function -dsn=XXXXXXXXX
func TestCallBuildStatementFunction(t *testing.T) {
	listOfArguments := []interface{}{}
	keywordArguments := map[string]interface{}{}
	callBuildStatementTest(true, listOfArguments, keywordArguments, "begin :1 := Func(); end;", t)
	listOfArguments = append(listOfArguments, "listarg1")
	callBuildStatementTest(true, listOfArguments, keywordArguments, "begin :1 := Func(:2); end;", t)
	keywordArguments["keyarg1"] = "keyval1"
	callBuildStatementTest(true, listOfArguments, keywordArguments, "begin :1 := Func(:2, keyarg1=>:3); end;", t)
	// empty listArgs
	listOfArguments = []interface{}{}
	callBuildStatementTest(true, listOfArguments, keywordArguments, "begin :1 := Func(keyarg1=>:2); end;", t)
}

// clear && go test -v -test.run Test_callBuildStatement_Procedure -dsn=XXXXXXXXX
func TestCallBuildStatementProcedure(t *testing.T) {
	listOfArguments := []interface{}{}
	keywordArguments := map[string]interface{}{}
	callBuildStatementTest(false, listOfArguments, keywordArguments, "begin Proc(); end;", t)
	listOfArguments = append(listOfArguments, "listarg1")
	callBuildStatementTest(false, listOfArguments, keywordArguments, "begin Proc(:1); end;", t)
	keywordArguments["keyarg1"] = "keyval1"
	callBuildStatementTest(false, listOfArguments, keywordArguments, "begin Proc(:1, keyarg1=>:2); end;", t)
	// empty listArgs
	listOfArguments = []interface{}{}
	callBuildStatementTest(false, listOfArguments, keywordArguments, "begin Proc(keyarg1=>:1); end;", t)
}

func callBuildStatementTest(
	withReturn bool,
	listOfArguments []interface{},
	keywordArguments map[string]interface{},
	expectedStatement string,
	t *testing.T) {
	mockedCursor := &Cursor{}
	var statement string
	var err error
	if withReturn { // Function
		var returnValue Variable
		statement, _, err = mockedCursor.callBuildStatement(
			"Func",
			&returnValue,
			listOfArguments,
			keywordArguments)
		if err != nil {
			t.Fatal(err)
		}
	} else { // Procedure
		statement, _, err = mockedCursor.callBuildStatement(
			"Proc",
			nil,
			listOfArguments,
			keywordArguments)
		if err != nil {
			t.Fatal(err)
		}
	}
	if statement != expectedStatement {
		t.Errorf("got:%s\nwant:%s", statement, expectedStatement)
	}
}
