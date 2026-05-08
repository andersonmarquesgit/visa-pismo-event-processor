.PHONY: up down dow producer processor logs test tidy

producer:
	go run ./cmd/producer

processor:
	go run ./cmd/processor

up:
	docker compose up -d --build
	$(MAKE) producer

down:
	docker compose down

dow:
	$(MAKE) down

logs:
	docker compose logs -f --tail=200

test:
	go test ./...

coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

tidy:
	go mod tidy

