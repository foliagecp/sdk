FROM golang:1.21 as builder

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
      build-essential gcc

COPY . .

RUN go mod tidy

# --ldflags '-extldflags "-Wl,--allow-multiple-definition"' for resolving the following cgo conflict between go-graphviz and rogchap:
# running g++ failed: exit status 1; multiple definition of `Version'
RUN cd ./tests/basic && go build --ldflags '-extldflags "-Wl,--allow-multiple-definition"' -o /usr/bin/foliage

FROM ubuntu:22.04

WORKDIR /foliage

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/foliage /usr/bin/

