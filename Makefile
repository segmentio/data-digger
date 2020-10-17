PKG := ./cmd/digger
BIN := build/digger

.PHONY: digger
digger:
	go build -o $(BIN) $(PKG)

.PHONY: digger-linux
digger-linux:
	$QGOOS=linux GOARCH=amd64 go build -o build/digger-linux ./cmd/digger

.PHONY: install
install:
	go install $(PKG)

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
	rm -Rf build vendor test_inputs
