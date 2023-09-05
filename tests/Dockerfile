FROM golang:1.20 as builder

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
      build-essential gcc

COPY . .

RUN go mod tidy

RUN cd ./tests && go build -o /usr/bin/tests

FROM ubuntu:22.04

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

ENV TESTS_ROOT_DIR /foliage_sdk_tests
COPY --from=builder /usr/bin/tests /usr/bin/
COPY --from=builder /usr/src/app/tests/ /foliage_sdk_tests
# Delete all .go files from all subfolders of "foliage_sdk_tests" && delete all files in "foliage_sdk_tests"
RUN find /foliage_sdk_tests -name "*.go" -type f | xargs rm -rf && find /foliage_sdk_tests -maxdepth 1 -type f | xargs rm -rf

