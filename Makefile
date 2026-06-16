BINARY  := plotka
BIN_DIR := bin
PKG     := ./cmd/plotka
PREFIX  ?= /usr/local

# Version from the git tag (matches the GoReleaser build); falls back to "dev".
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# -trimpath: reproducible paths; -s strip symbol table, -w strip DWARF;
# -X main.version: embed the version (see cmd/plotka/main.go).
GOFLAGS := -trimpath
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build test race vet fmt tidy clean install run

all: build

build:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) $(PKG)

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)

# make install PREFIX=/usr/local  (honours DESTDIR for packaging)
install: build
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 0755 $(BIN_DIR)/$(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)

# make run ARGS="server --registry-bind 127.0.0.1 --registry-port 5354 --admin-socket /tmp/p.sock"
run: build
	$(BIN_DIR)/$(BINARY) $(ARGS)
