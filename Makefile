.PHONY: up down ps logs verify clean topics topics-plan simulate simulate-surge route backfill airflow-up airflow-down monitoring-up monitoring-down gen-alerts

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

# Preview the topic plan derived from metadata (no connection)
topics-plan:
	python ingest/create_topics.py --dry-run

# Create topics from metadata, then verify partitions/RF on the cluster
topics:
	python ingest/create_topics.py --verify

# Produce baseline synthetic load (realistic rate)
simulate:
	python simulator/generator.py --devices 200 --rate 1

# Produce a throughput surge (the anomaly-demo engine)
simulate-surge:
	python simulator/generator.py --devices 200 --rate 10

# Consume telemetry and write to hot (Timescale) + cold (MinIO)
route:
	python ingest/router.py

# Burst-produce 14 days of history so Airflow has daily data to aggregate
backfill:
	python simulator/generator.py --backfill-days 14

# Bring up the Airflow stack (scheduler + webserver + its own metadata DB)
airflow-up:
	docker compose --profile airflow up -d

airflow-down:
	docker compose --profile airflow down

# Bring up Prometheus + Grafana + exporters
monitoring-up:
	docker compose --profile monitoring up -d

monitoring-down:
	docker compose --profile monitoring down

# DANGER: also removes volumes (wipes all data). Use to start clean.
clean:
	docker compose down -v
	gen-alerts:
		python monitoring/grafana/gen_alerts.py