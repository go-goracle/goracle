/*
   Package main in csvdump represents a cursor->csv dumper

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
package main

import (
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/tgulacsi/goracle/examples/connect"
	"github.com/tgulacsi/goracle/godrv"
)

func getQuery(table, where string, columns []string) string {
	cols := "*"
	if len(columns) > 0 {
		cols = strings.Join(columns, ", ")
	}
	if where == "" {
		return "SELECT " + cols + " FROM " + table
	}
	return "SELECT " + cols + " FROM " + table + " WHERE " + where
}

func dump(w io.Writer, qry string) error {
	db, err := connect.GetConnection("")
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query(qry)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	log.Printf("columns: %s", columns)
	//cols := godrv.ColumnDescriber(rows).DescribeColumns()
	//log.Printf("cols: %s", cols)
	for rows.Next() {
	}
	return nil
}

func main() {
	var (
		where   string
		columns []string
	)

	flag.Parse()
	if flag.NArg() > 1 {
		where = flag.Arg(1)
		if flag.NArg() > 2 {
			columns = flag.Args()[2:]
		}
	}
	qry := getQuery(flag.Arg(0), where, columns)
	dump(os.Stdout, qry)
}
