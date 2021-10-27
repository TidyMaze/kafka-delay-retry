# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:1.17.2-bullseye AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./
COPY internal ./internal

RUN go build -o /docker-kafka-delay-retry

RUN chmod a+x /docker-kafka-delay-retry

##
## Deploy
##
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /docker-kafka-delay-retry /docker-kafka-delay-retry

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/docker-kafka-delay-retry"]