

test:
	go test -race ./...

test-systemtest:
	GOKA_SYSTEMTEST=y go test -v -race github.com/lovoo/goka/systemtest

test-all: test test-systemtest
