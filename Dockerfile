FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /watcher .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /watcher /watcher
USER nonroot:nonroot
ENTRYPOINT ["/watcher"]
