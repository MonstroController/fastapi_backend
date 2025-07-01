up:
	docker compose up -d

# Остановить контейнеры
down:
	docker compose down

# Пересобрать образы и запустить
rebuild:
	docker compose down
	docker compose build
	docker compose up -d


