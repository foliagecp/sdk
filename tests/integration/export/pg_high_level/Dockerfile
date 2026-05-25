FROM golang:1.21 as builder

WORKDIR /usr/src/app

COPY . .

RUN go mod tidy

RUN cd ./tests/integration/export/pg_high_level && go build -o /usr/bin/foliage-semantic-export-test

FROM ubuntu:22.04

WORKDIR /foliage

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/foliage-semantic-export-test /usr/bin/
