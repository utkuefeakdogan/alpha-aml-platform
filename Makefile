.PHONY: up up-all down logs core app ops reload-configs

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

# Recreate the containers that bind-mount ./configs so they pick up edited
# rule/scenario files. Needed because editors that replace the configs dir
# inode can orphan a long-running container's bind mount (stale = empty).
reload-configs:
	docker compose --profile core --profile app --profile ops up -d \
		--force-recreate --no-deps spark-job transaction-gen streamlit
