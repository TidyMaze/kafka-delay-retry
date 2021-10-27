.PHONY: start-deps test build-run

start-deps:
	docker-compose up -d

test:
	go test -v ./test -timeout 5m 

build-run:
	go install && kafka-delay-retry