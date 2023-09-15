FROM golang:1.20 as builder

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
      build-essential gcc

COPY . .

RUN go mod tidy

RUN cd ./tests && go build -o /usr/bin/tests

FROM ubuntu:22.04

WORKDIR /tests

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/tests /usr/bin/

