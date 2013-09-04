#!/bin/sh
set -e
echo fmt
go fmt ./...
echo vet
go vet ./...
if ! which golint >/dev/null; then
    PATH=$GOPATH/bin:$PATH
    if ! which golint >/dev/null; then
        echo 'No golint installed: to install, run'
        echo 'go get github.com/golang/lint/golint'
        exit $?
    fi
fi
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

. $(dirname $0)/.env
go test $TOPTS -work -c -tags trace ./oracle || {
    echo "CFLAGS=$CGO_CFLAGS LDFLAGS=$CGO_LDFLAGS"
    exit $?
}
ln -sf /tmp/go-build[0-9]* /tmp/go-build-goracle

if [ -e /etc/init.d/oracle-xe ]; then
    if systemctl is-active oracle-xe.service; then
        echo 'oracle-xe is running'
    else
        sudo systemctl start oracle-xe.service
        while true; do
            if systemctl is-active oracle-xe.service; then
                break
            fi
            echo "waiting for Oracle"
            sleep 1
        done
    fi
fi
echo "./oracle.test -dsn=\$\(cat $(dirname $0)/.dsn\) ""$@"
./oracle.test -dsn=$(cat $(dirname $0)/.dsn) "$@"
