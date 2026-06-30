.PHONY: up down ps logs log-% verify clean help \
        topics topics-plan \
        simulate simulate-surge route backfill \
        airflow-up airflow-down \
        monitoring-up monitoring-down monitoring-stop \
        pgbouncer-up pgbouncer-down route-pooled \
        dq-status dag-run \
        check-lag check-cluster check-db check-targets check-all \
        ps-pipeline stop-pipeline \
        gen-alerts \
        chaos-surge chaos-kill chaos-restore chaos-choke chaos-stop-hrv chaos-stale chaos-stale-restore \
        startup-all shutdown-all

# ====================== 基礎指令 ======================
# Infra (Kafka + Timescale + MinIO)
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

# ====================== Airflow DAG / DQ 快捷 ======================
# DAG（可覆寫：make dag-run DAG=gold_day_strain）
DAG ?= gold_recovery_score

dq-status:
	@docker exec -e PGPASSWORD="$$TIMESCALE_PASSWORD" timescaledb \
		psql -U $${TIMESCALE_USER:-ian} -d $${TIMESCALE_DB:-telemetry} -c \
		"SELECT metric, freshness_lag_days, status, checked_at \
		 FROM dq_results ORDER BY checked_at DESC;"

dag-run:
	@before=$$(docker exec -e PGPASSWORD="$$TIMESCALE_PASSWORD" timescaledb psql -U $${TIMESCALE_USER:-ian} -d $${TIMESCALE_DB:-telemetry} -tAc "SELECT coalesce(max(checked_at)::text,'none') FROM dq_results;"); \
	echo ">> triggering $(DAG)  (last dq_results write: $$before)"; \
	docker exec airflow-scheduler airflow dags trigger $(DAG) >/dev/null; \
	echo ">> waiting for dq_check to write a new result (polling the table)..."; \
	for i in $$(seq 1 40); do \
		now=$$(docker exec -e PGPASSWORD="$$TIMESCALE_PASSWORD" timescaledb psql -U $${TIMESCALE_USER:-ian} -d $${TIMESCALE_DB:-telemetry} -tAc "SELECT coalesce(max(checked_at)::text,'none') FROM dq_results;"); \
		if [ "$$now" != "$$before" ]; then echo ">> done — new result at $$now"; break; fi; \
		printf '.'; sleep 3; \
	done; \
	echo; \
	$(MAKE) --no-print-directory dq-status

# ====================== PgBouncer (連線池 demo) ======================
# 啟動 PgBouncer（夾在 router 和 TimescaleDB 之間）
pgbouncer-up:
	docker compose --profile pgbouncer up -d

pgbouncer-down:
	docker compose --profile pgbouncer down

# 讓 router 走 PgBouncer（PGPORT=6432）。開多個觀察連線收斂
route-pooled:
	PGPORT=6432 python ingest/router.py

# ====================== 診斷 / 檢查（情境驗證用，直接查源頭，比 Grafana 快）======================
# 情境 1 & 3：consumer lag —— 每個 topic/partition 的 lag（kafka 原生視圖）
check-lag:
	@docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
		--describe --group router

# 情境 2：叢集健康 —— 哪台 broker 活著 + 有沒有 under-replicated 分區
check-cluster:
	@echo "== brokers responding =="
	@for b in kafka1 kafka2 kafka3; do \
		docker exec $$b kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 \
		&& echo "  $$b OK" || echo "  $$b DOWN"; \
	done
	@echo "== under-replicated partitions (空白 = 健康) =="
	@docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
		--describe --under-replicated-partitions

# 情境 3：TimescaleDB 狀態 —— 連線數 + 有沒有 query 卡在 Lock 上（backpressure 訊號）
check-db:
	@docker exec -e PGPASSWORD="$$TIMESCALE_PASSWORD" timescaledb \
		psql -U $${TIMESCALE_USER:-ian} -d $${TIMESCALE_DB:-telemetry} -c \
		"SELECT state, wait_event_type, count(*) \
		 FROM pg_stat_activity WHERE datname = current_database() \
		 GROUP BY 1,2 ORDER BY 3 DESC;"

# 所有 Prometheus target 健康嗎（up=1 才正常）
check-targets:
	@curl -s localhost:9090/api/v1/targets | \
		python3 -c "import sys,json; [print('  %-10s %s' % (t['labels']['job'], t['health'])) for t in json.load(sys.stdin)['data']['activeTargets']]"

# 一次檢查四個情境（cluster + lag + db + DQ）
check-all: check-cluster check-lag check-db dq-status

# ====================== Pipeline 程序控制（host 上的 route/generator）======================
# 看 route / generator 運行情況（情境 4 注入前要先停掉，不然 HRV 會被 refill）
ps-pipeline:
	@ps aux | grep -E "generator.py|router.py" | grep -v grep || echo "  none running"

# 乾淨停掉 route + generator（解 Ctrl-C 停不掉的問題）
stop-pipeline:
	@pkill -f generator.py 2>/dev/null && echo "  stopped generator" || echo "  generator not running"
	@pkill -f router.py 2>/dev/null && echo "  stopped router" || echo "  router not running"


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
# 全部開啟 (Infra + Airflow + Monitoring）
startup-all:
	docker compose up -d
	docker compose --profile airflow up -d
	docker compose --profile monitoring up -d
	@echo "All services started (Foundation + Airflow + Monitoring)"

# 實驗完全部關閉（推薦實驗後使用）
shutdown-all:
	docker compose --profile monitoring --profile airflow down
	docker compose down
	@echo "All services stopped (Foundation + Airflow + Monitoring)"

# ====================== IMPORTANT 危險指令 ======================
# 完整清除資料（含 volumes，會 wipe 所有資料）— 需手動輸入 yes 確認
clean:
	@read -p "DANGER - This wipes ALL data (volumes). Type 'yes' to confirm: " ok && [ "$$ok" = "yes" ] && docker compose --profile monitoring --profile airflow down -v || echo "aborted"
	@echo "done"

help:
	@echo "Available commands:"
	@echo "  make up                → 啟動Infra (Kafka+Timescale+MinIO)"
	@echo "  make monitoring-up     → 啟動 Prometheus + Grafana + exporters"
	@echo "  make airflow-up        → 啟動 Airflow"
	@echo "  make startup-all       → 全部啟動"
	@echo "  make shutdown-all      → 全部關閉（實驗完推薦）"
	@echo "  make verify            → 檢查 KRaft 狀態"
	@echo "  make backfill          → Backfill 14 天歷史資料"
	@echo "  make gen-alerts        → 從 SLA 生成告警規則"
	@echo "  make chaos-surge       → 情境1：流量突增"
	@echo "  make chaos-kill        → 情境2：Kill broker （chaos-restore 復原）"
	@echo "  make chaos-choke       → 情境3：下游 backpressure"
	@echo "  make chaos-stale       → 情境4：HRV 沉默 → DQ freshness（chaos-stale-restore 復原）"
	@echo "  --- 診斷 / 檢查 ---"
	@echo "  make check-all         → 四情整體檢查 (cluster+lag+db+DQ)"
	@echo "  make check-lag         → 情境1/3：consumer lag (kafka 原生)"
	@echo "  make check-cluster     → 情境2：broker status + under-replicated"
	@echo "  make check-db          → 情境3：DB 連線 + Lock 等待"
	@echo "  make dq-status         → 情境4：直接讀 dq_results 表"
	@echo "  make check-targets     → Prometheus target 健康"
	@echo "  --- DAG / pipeline ---"
	@echo "  make dag-run           → 觸發 DAG + 等跑完 + 顯示 DQ"
	@echo "  make ps-pipeline       → 看 route/generator 在不在跑"
	@echo "  make stop-pipeline     → 乾淨停掉 route + generator"
	@echo "  make clean             → 清除所有資料"