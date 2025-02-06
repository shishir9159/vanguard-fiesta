FROM golang:1.23 AS builder
WORKDIR /app

COPY go.* ./
RUN go mod download
COPY . ./
RUN go build -o main

FROM debian:bookworm-slim
RUN set -x && apt-get update && apt-get install -y \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/main /app/server
CMD ["/app/server"]
