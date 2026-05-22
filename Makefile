.PHONY: build test lint integration-test e2e-test e2e-infra-up e2e-infra-down clean

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/watcher .

test:
	go test ./internal/... -count=1 -race

lint:
	golangci-lint run ./...

integration-test:
	go test ./internal/... -tags=integration -count=1 -race -timeout=300s

# E2E: real binary, real infra (docker-compose), real NATS messages
e2e-infra-up:
	docker compose -f e2e/docker-compose.e2e.yml up -d --wait

e2e-infra-down:
	docker compose -f e2e/docker-compose.e2e.yml down -v

e2e-test: e2e-infra-up
	go test ./e2e/... -tags=e2e -count=1 -timeout=600s -v; \
	status=$$?; \
	$(MAKE) e2e-infra-down; \
	exit $$status

all-tests: test integration-test e2e-test

clean:
	rm -rf bin/
