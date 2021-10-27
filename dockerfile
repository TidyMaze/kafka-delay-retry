# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:alpine AS build

WORKDIR /app

RUN apk add --no-cache gcc libc-dev librdkafka-dev

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./
COPY internal ./internal

RUN go build -tags musl -o /docker-kafka-delay-retry

##
## Deploy
##
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /docker-kafka-delay-retry /docker-kafka-delay-retry

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/kafka-delay-retry"]