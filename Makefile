.PHONY: up down run run-worker test tidy build

up:
	docker compose up -d --wait

down:
	docker compose down

run:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

test:
	go test ./... -count=1

tidy:
	go mod tidy
