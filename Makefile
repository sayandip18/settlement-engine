.PHONY: up down run test tidy build

up:
	docker compose up -d --wait

down:
	docker compose down

run:
	go run ./cmd/api

build:
	go build -o bin/api ./cmd/api

test:
	go test ./... -count=1

tidy:
	go mod tidy
