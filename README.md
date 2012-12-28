# goracle #
*goracle/oracle* is a package is a translated version of
cx_Oracle (by Anthony Tuininga) converted from C (Python module) to Go.

*goracle/godrv* is a package which (will be) a database/sql/driver.Driver
compliant wrapper for goracle/oracle.

## There ##
CHAR, VARCHAR2, NUMBER, DATETIME simple (not array) bind/define.

## Not working ##
INTERVAL

## Not tested (yet) ##
LONG, LONG RAW, CURSOR, LOB datatypes, array bind/define.