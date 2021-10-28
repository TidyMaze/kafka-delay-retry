.PHONY: start-deps test build-run

start-deps:
	docker-compose up -d

test:
	go test -v ./test -timeout 5m 

build-run:
	go install && kafka-delay-retry

build-docker:
	docker build -t docker-kafka-delay-retry .

run-docker:
	docker run --name docker-kafka-delay-retry --rm docker-kafka-delay-retry