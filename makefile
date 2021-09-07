start-deps:
	docker-compose up -d

test:
	go test -v

build-run:
	go install && kafka-delay-retry