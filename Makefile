.PHONY: up up-all down logs core app ops

up:
	docker compose --profile core --profile app up -d --build

up-all:
	docker compose --profile core --profile app --profile ops up -d --build

down:
	docker compose --profile core --profile app --profile ops down

logs:
	docker compose logs -f

core:
	docker compose --profile core up -d

app:
	docker compose --profile app up -d --build

ops:
	docker compose --profile ops up -d --build
