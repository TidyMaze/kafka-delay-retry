# syntax=docker/dockerfile:1

FROM golang:1.16-bullseye

WORKDIR /app

RUN apt update
RUN apt install gcc libc-dev

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN ls -rla

RUN CGO_ENABLED=1 go build -work -o /docker-kafka-delay-retry

RUN chmod a+rx /docker-kafka-delay-retry

CMD ["/docker-kafka-delay-retry"]