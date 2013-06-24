#!/bin/sh
set -e
echo fmt
go fmt ./...
echo vet
go vet ./...
which golint >/dev/null || {
    echo 'No golint installed: to install, run'
    echo 'go get github.com/golang/lint/golint'
    exit $?
}
set +e
echo lint
golint ./godrv | grep -v 'LastInsertID'
golint ./oracle
set -e
#echo build
#go build -tags trace ./...
echo test
TOPTS="${TOPTS} -test.v"
if [ -n "$TRACE" ]; then
  TOPTS="$TOPTS -tags trace"
fi
rm -rf /tmp/go-build[0-9]*
go test $TOPTS -work -c -tags trace ./oracle
ln -sf /tmp/go-build[0-9]* /tmp/go-build-goracle

if [ -e /etc/init.d/oracle-xe ]; then
    systemctl is-active oracle-xe.service || {
        sudo systemctl start oracle-xe.service
        while [ ! systemctl is-active oracle-xe.service ]; do
            echo "waiting for Oracle"
            sleep 1
        done
    }
fi
./oracle.test -dsn=$(cat $(dirname $0)/.dsn) "$@"
