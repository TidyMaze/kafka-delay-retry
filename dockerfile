# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:alpine AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /docker-kafka-delay-retry

##
## Deploy
##
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /docker-kafka-delay-retry /docker-kafka-delay-retry

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/kafka-delay-retry"]