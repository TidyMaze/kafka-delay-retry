start-deps:
	docker-compose up -d

test:
	go test -v -timeout 60s

build-run:
	go install && kafka-delay-retry