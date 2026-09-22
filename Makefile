.PHONY: build up demo verify test logs down clean

build:
	docker compose build

up:
	docker compose up --build -d

demo:
	docker compose down --volumes --remove-orphans
	docker compose up --build -d
	docker compose run --rm verifier

verify:
	docker compose run --rm verifier

test:
	docker compose run --rm --no-deps processor pytest -q

logs:
	docker compose logs processor article-publisher

down:
	docker compose stop

clean:
	docker compose down --volumes --remove-orphans
