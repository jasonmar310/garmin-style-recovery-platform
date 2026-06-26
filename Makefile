.PHONY: up down ps logs verify clean

# Bring up the foundation stack (Kafka x3 + Timescale + MinIO)
up:
	docker compose up -d

# Stop and remove containers (keeps named volumes / data)
down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# Verify the 3-node KRaft quorum is healthy:
#   expect 3 voters, one Leader, and the same LogEndOffset converging.
verify:
	@echo "== KRaft quorum status =="
	docker exec kafka1 kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
	@echo "\n== Broker API reachable on each broker =="
	@for b in kafka1 kafka2 kafka3; do \
		echo "-- $$b --"; \
		docker exec $$b kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null \
		&& echo "OK" || echo "UNREACHABLE"; \
	done

# DANGER: also removes volumes (wipes all data). Use to start clean.
clean:
	docker compose down -v