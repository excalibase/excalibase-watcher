.PHONY: build test lint integration-test e2e-test e2e-certs e2e-infra-up e2e-infra-down clean

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/watcher .

test:
	go test ./internal/... -count=1 -race

lint:
	golangci-lint run ./...

integration-test:
	go test ./internal/... -tags=integration -count=1 -race -timeout=300s

# E2E: real binary, real infra (docker-compose), real NATS messages
# The e2e MySQL serves a certificate from a throwaway CA and requires TLS
# (EXC-589); the watcher verifies it with e2e/tls/ca.pem.
e2e-certs:
	mkdir -p e2e/tls
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 7 \
		-subj "/CN=watcher e2e CA" -keyout e2e/tls/ca-key.pem -out e2e/tls/ca.pem
	openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=localhost" \
		-keyout e2e/tls/server-key.pem -out e2e/tls/server.csr
	printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > e2e/tls/server.ext
	openssl x509 -req -in e2e/tls/server.csr -CA e2e/tls/ca.pem -CAkey e2e/tls/ca-key.pem -CAcreateserial \
		-days 7 -extfile e2e/tls/server.ext -out e2e/tls/server.pem
	chmod 644 e2e/tls/server-key.pem

e2e-infra-up: e2e-certs
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
