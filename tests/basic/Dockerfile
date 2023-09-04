FROM golang:1.20 as builder

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
      build-essential gcc

COPY . .

RUN go mod tidy

RUN go build -o /usr/bin/fns

FROM ubuntu:22.04

RUN apt update && \
    apt install ca-certificates curl -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/fns /usr/bin/
COPY --from=builder /usr/src/app/master_function_test.js /master_function_test.js
COPY --from=builder /usr/src/app/.env /.env

CMD [ ". .env && /usr/bin/fns" ]

