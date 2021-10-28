# syntax=docker/dockerfile:1

FROM golang:alpine

WORKDIR /app

RUN apk add --no-cache gcc libc-dev librdkafka-dev musl-dev

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./
COPY internal ./internal

RUN go build -work -tags musl -o /docker-kafka-delay-retry

RUN chmod a+rx /docker-kafka-delay-retry

ENTRYPOINT ["/docker-kafka-delay-retry"]