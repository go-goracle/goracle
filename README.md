# goracle #
Translate cx_Oracle (by Anthony Tuininga) from C (Python module) to Go.

NOT database/sql compliant, but I hope someone will write the additional layer...

## There ##
CHAR, VARCHAR2, NUMBER, DATETIME simple (not array) bind/define.

## Not working ##
INTERVAL

## Not tested (yet) ##
LONG, LONG RAW, CURSOR, LOB datatypes, array bind/define.