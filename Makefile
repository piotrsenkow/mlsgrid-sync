BINARY  := mlsgrid-sync
VERSION ?= dev
LDFLAGS := -X github.com/piotrsenkow/mlsgrid-sync/internal/cli.version=$(VERSION)

.PHONY: build run test test-integration lint fmt tidy docker-build clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/$(BINARY)

run: build
	./bin/$(BINARY) $(ARGS)

test:
	go test -race ./...

# Integration tests need Docker (testcontainers Postgres). Tagged //go:build integration.
test-integration:
	go test -race -tags integration ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .
	go vet ./...

tidy:
	go mod tidy

docker-build:
	docker build -t $(BINARY):$(VERSION) .

clean:
	rm -rf bin/
