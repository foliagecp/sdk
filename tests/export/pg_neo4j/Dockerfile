FROM golang:1.21 as builder

WORKDIR /usr/src/app

COPY . .

RUN go mod tidy

RUN cd ./tests/export/pg_neo4j && go build -o /usr/bin/foliage-pg-neo4j-export-test

FROM ubuntu:22.04

WORKDIR /foliage

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/foliage-pg-neo4j-export-test /usr/bin/
