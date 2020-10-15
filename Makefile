ifndef VERSION
	VERSION := $(shell git describe --tags --always --dirty="-dev")
endif

PKG := ./cmd/digger
BIN := build/digger

LDFLAGS := -ldflags='-X "main.version=$(VERSION)"'

.PHONY: digger
digger:
	go build -o $(BIN) $(LDFLAGS) $(PKG)

.PHONY: digger-linux
digger-linux:
	$QGOOS=linux GOARCH=amd64 go build -o build/digger-linux $(LDFLAGS) ./cmd/digger

.PHONY: install
install:
	go install $(LDFLAGS) $(PKG)

.PHONY: vet
vet:
	$Qgo vet ./...

.PHONY: vendor
vendor:
	$Qgo mod vendor

.PHONY: test
test: vet
	$Qgo test -count 1 -p 1 ./...

.PHONY: clean
clean:
	rm -Rf build
