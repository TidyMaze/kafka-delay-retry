.PHONY: start-deps test build-run

start-deps:
	docker-compose up -d

test:
	go test -v -timeout 5m ./...

build-run:
	go install && kafka-delay-retry