.PHONY: up down ps logs log-% verify clean help \
        topics topics-plan \
        simulate simulate-surge route backfill \
        airflow-up airflow-down \
        monitoring-up monitoring-down monitoring-stop \
        pgbouncer-up pgbouncer-down route-pooled \
        gen-alerts \
        chaos-surge chaos-kill chaos-restore chaos-choke chaos-stop-hrv chaos-stale chaos-stale-restore \
        startup-all shutdown-all

# ====================== 基礎指令 ======================
# 地基 (Kafka + Timescale + MinIO)
up:
	docker compose up -d

# 停止並移除容器（保留 volumes）
down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# 查看特定服務 log，例如：make log-router  /  make log-grafana
log-%:
	docker compose logs -f --tail=50 $*

# ====================== 驗證與資料 ======================
verify:
	@echo "== KRaft quorum status =="
	docker exec kafka1 kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
	@printf "\n== Broker API reachable ==\n"
	@for b in kafka1 kafka2 kafka3; do \
		echo "-- $$b --"; \
		docker exec $$b kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null && echo "OK" || echo "UNREACHABLE"; \
	done

topics-plan:
	python ingest/create_topics.py --dry-run

topics:
	python ingest/create_topics.py --verify

# ====================== 模擬與測試 ======================
simulate:
	python simulator/generator.py --devices 200 --rate 1

simulate-surge:
	python simulator/generator.py --devices 200 --rate 10

route:
	python ingest/router.py

# 灌 14 天歷史，讓 Airflow 有跨日資料可聚合
backfill:
	python simulator/generator.py --backfill-days 14

# ====================== 額外 Stack ======================
# Airflow
airflow-up:
	docker compose --profile airflow up -d

airflow-down:
	docker compose --profile airflow down

# Monitoring (Prometheus + Grafana + exporters)
monitoring-up:
	docker compose --profile monitoring up -d

monitoring-down:
	docker compose --profile monitoring down

# 只關監控（省資源）
monitoring-stop:
	docker compose --profile monitoring down

# 從 streams.yaml 的 SLA 生成 Prometheus 告警規則
gen-alerts:
	python monitoring/gen_alerts.py

# ====================== PgBouncer (連線池 demo) ======================
# 啟動 PgBouncer（夾在 router 和 TimescaleDB 之間）
pgbouncer-up:
	docker compose --profile pgbouncer up -d

pgbouncer-down:
	docker compose --profile pgbouncer down

# 讓 router 走 PgBouncer（PGPORT=6432）。開多個觀察連線收斂
route-pooled:
	PGPORT=6432 python ingest/router.py

# ====================== Chaos (Day 6 異常模擬) ======================
# 情境 1：流量突增 → consumer lag（需先 make route）
chaos-surge:
	./chaos/surge.sh 15 120

# 情境 2：broker 故障 → under-replicated
chaos-kill:
	./chaos/kill_broker.sh kafka2

chaos-restore:
	./chaos/kill_broker.sh --restore kafka2

# 情境 3：下游 backpressure（需先 source .env 讓腳本讀得到 TIMESCALE_*）
chaos-choke:
	./chaos/choke_sink.sh 60

# 情境 4：停 HRV stream → 資料層 freshness 異常（infra 全綠、DQ 紅）
chaos-stop-hrv:
	./chaos/stop_hrv.sh 200 180

# 情境 4（demo 觸發）：刪最新天 HRV+gold 製造 freshness gap，立即可見
chaos-stale:
	./chaos/stale_hrv.sh

chaos-stale-restore:
	./chaos/stale_hrv.sh --restore

# ====================== 快捷操作 ======================
# 全部開啟（地基 + Airflow + Monitoring）
startup-all:
	docker compose up -d
	docker compose --profile airflow up -d
	docker compose --profile monitoring up -d
	@echo "✅ All services started (Foundation + Airflow + Monitoring)"

# 實驗完全部關閉（推薦實驗後使用）
shutdown-all:
	docker compose --profile monitoring --profile airflow down
	docker compose down
	@echo "✅ All services stopped (Foundation + Airflow + Monitoring)"

# ====================== IMPORTANT 危險指令 ======================
# 完整清除資料（含 volumes，會 wipe 所有資料）— 需手動輸入 yes 確認
clean:
	@read -p "⚠️  This wipes ALL data (volumes). Type 'yes' to confirm: " ok && [ "$$ok" = "yes" ] && docker compose --profile monitoring --profile airflow down -v || echo "aborted"
	@echo "done"

help:
	@echo "Available commands:"
	@echo "  make up                → 啟動地基 (Kafka+Timescale+MinIO)"
	@echo "  make monitoring-up     → 啟動 Prometheus + Grafana + exporters"
	@echo "  make airflow-up        → 啟動 Airflow"
	@echo "  make startup-all       → 全部啟動"
	@echo "  make shutdown-all      → 全部關閉（實驗完推薦）"
	@echo "  make verify            → 檢查 KRaft 狀態"
	@echo "  make backfill          → 灌 14 天歷史資料"
	@echo "  make gen-alerts        → 從 SLA 生成告警規則"
	@echo "  make chaos-surge       → 情境1：流量突增"
	@echo "  make chaos-kill        → 情境2：Kill broker （chaos-restore 復原）"
	@echo "  make chaos-choke       → 情境3：下游 backpressure"
	@echo "  make clean             → 清除所有資料"