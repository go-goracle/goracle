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
package connect

import (
	"database/sql"
	"flag"
	"os"

	"github.com/juju/errgo/errors"
	_ "github.com/tgulacsi/goracle/godrv"
	"github.com/tgulacsi/goracle/oracle"
)

var (
	fDsn         = flag.String("db.dsn", "", "Oracle DSN (user/passw@sid)")
	fUsername    = flag.String("db.username", "", "username to connect as (if you don't provide the dsn")
	fPassword    = flag.String("db.password", "", "password to connect with (if you don't provide the dsn")
	fHost        = flag.String("db.host", "", "Oracle DB's host (if you don't provide the dsn")
	fPort        = flag.Int("db.port", 1521, "Oracle DB's port (if you don't provide the dsn) - defaults to 1521")
	fSid         = flag.String("db.sid", "", "Oracle DB's SID (if you don't provide the dsn)")
	fServiceName = flag.String("db.service", "", "Oracle DB's ServiceName (if you don't provide the dsn and the sid)")
)

// GetDSN returns a (command-line defined) connection string
func GetDSN() string {
	if !flag.Parsed() {
		flag.Parse()
		if *fDsn == "" {
			*fDsn = os.Getenv("DSN")
		}
	}

	var user, passw, sid string
	if !(fDsn == nil || *fDsn == "") {
		user, passw, sid = oracle.SplitDSN(*fDsn)
		//log.Printf("user=%q passw=%q sid=%q", user, passw, sid)
	}
	if user == "" && fUsername != nil && *fUsername != "" {
		user = *fUsername
	}
	if passw == "" && fPassword != nil && *fPassword != "" {
		passw = *fPassword
	}
	if sid == "" {
		if fSid != nil && *fSid != "" {
			sid = *fSid
		} else {
			sid = oracle.MakeDSN(*fHost, *fPort, "", *fServiceName)
		}
	}
	return user + "/" + passw + "@" + sid
}

// GetConnection returns a connection - using GetDSN if dsn is empty
func GetConnection(dsn string) (*sql.DB, error) {
	if dsn == "" {
		dsn = GetDSN()
	}
	return sql.Open("goracle", dsn)
}

// GetRawConnection returns a raw (*oracle.Connection) connection
// - using GetDSN if dsn is empty
func GetRawConnection(dsn string) (*oracle.Connection, error) {
	if dsn == "" {
		dsn = GetDSN()
	}
	user, passw, sid := oracle.SplitDSN(dsn)
	conn, err := oracle.NewConnection(user, passw, sid, false)
	if err != nil {
		return conn, errors.Mask(err)
	}
	return conn, conn.Connect(0, false)
}
