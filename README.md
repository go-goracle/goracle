# goracle #
*goracle/oracle* is a package is a translated version of
[cx_Oracle](http://cx-oracle.sourceforge.net/html/index.html)
(by Anthony Tuininga) converted from C (Python module) to Go.

[goracle/godrv](godrv/driver.go) is a package which is a
[database/sql/driver.Driver](http://golang.org/pkg/database/sql/driver/#Driver)
compliant wrapper for goracle/oracle - passes github.com/bradfitz/go-sql-test
(as github.com/tgulacsi/go-sql-test).

## There ##
CHAR, VARCHAR2, NUMBER, DATETIME, INTERVAL simple AND array bind/define.
CURSOR, CLOB, BLOB

## Not working ##
Nothing I know of.

## Not working ATM ##
Nothing I know of.

## Not tested (yet) ##
LONG, LONG RAW, BFILE


# Debug #
You can build the test executable (for debugging with gdb, for example) with
go test -c

You can build a tracing version with the "trace" build tag
(go build -tags=trace) that will print out everything before calling OCI
C functions.


# Install #
It is `go get`'able iff you have
[Oracle DB](http://www.oracle.com/technetwork/database/enterprise-edition/index.html) installed
OR the Oracle's
[InstantClient](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) installed
AND you have set proper environment variables:

    CGO_CFLAGS=-I$(basename $(find $ORACLE_HOME -type f -name oci.h))
    CGO_LDFLAGS=-L$(basename $(find $ORACLE_HOME -type f -name libclntsh.so\*))
    go get github.com/tgulacsi/goracle

For example, with my [XE](http://www.oracle.com/technetwork/products/express-edition/downloads/index.html):

    CGO_CFLAGS=-I/u01/app/oracle/product/11.2.0/xe/rdbms/public
    CGO_LDFLAGS=-L/u01/app/oracle/product/11.2.0/xe/lib

With InstantClient:

    CGO_CFLAGS=-I/usr/include/oracle/11.2/client64
    CGO_LDFLAGS=-L/usr/include/oracle/11.2/client64

