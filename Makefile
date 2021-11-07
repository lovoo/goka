

test:
	go test -race ./...

test-systemtest:
	GOKA_SYSTEMTEST=y go test -v github.com/lovoo/goka/systemtest

test-all: test test-systemtest
