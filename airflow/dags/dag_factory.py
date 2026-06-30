"""
dag_factory.py — generate one bronze->gold DAG per gold metric in streams.yaml.

Metadata-driven: add a metric under gold_metrics in streams.yaml and a DAG
appears here with no code change (as long as a compute SQL is registered below).
Each DAG runs: compute (aggregate bronze -> upsert daily gold) then dq_check
(row count, freshness, and distribution vs the REAL Whoop truth in
seed_params.yaml — the closed validation loop).

Gold derivations are deliberately approximate proxies for Garmin's proprietary
metrics (see ADR-0001): the point is a real bronze->silver->gold story, not
medical precision.
"""
from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

META = Path("/opt/airflow/metadata")
meta = yaml.safe_load((META / "streams.yaml").read_text())
seed = yaml.safe_load((META / "seed_params.yaml").read_text())


def _conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("PGHOST", "timescaledb"), port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("TIMESCALE_DB", "telemetry"),
        user=os.getenv("TIMESCALE_USER", "ian"),
        password=os.getenv("TIMESCALE_PASSWORD", ""))


# --- per-metric DDL + compute SQL. Metrics absent here (e.g. sleep_score, which
#     needs a sleep stream we don't produce yet) generate a DAG that skips. -----
COMPUTE = {
    "recovery_score": {
        "table": "gold_recovery", "score_col": "recovery_score",
        "fresh_source": "hr_readings",   # recovery should keep pace with HR days;
                                         # if HRV goes silent it can't, and lags.
        "ddl": """CREATE TABLE IF NOT EXISTS gold_recovery (
                    device_id TEXT, day DATE, resting_hr DOUBLE PRECISION,
                    hrv_rmssd DOUBLE PRECISION, recovery_score DOUBLE PRECISION,
                    PRIMARY KEY (device_id, day));""",
        # recovery: higher overnight HRV good, lower resting HR good. Anchored on
        # the real Whoop means (HRV 41, RHR 67), clipped to 0-100.
        "sql": """
            INSERT INTO gold_recovery (device_id, day, resting_hr, hrv_rmssd, recovery_score)
            SELECT h.device_id, h.day, h.resting_hr, v.hrv_rmssd,
                   GREATEST(0, LEAST(100,
                     63 + 1.0*(v.hrv_rmssd - 41) - 1.2*(h.resting_hr - 67))) AS recovery_score
            FROM (SELECT device_id, date_trunc('day', ts)::date AS day,
                         percentile_cont(0.05) WITHIN GROUP (ORDER BY bpm) AS resting_hr
                  FROM hr_readings GROUP BY 1,2) h
            JOIN (SELECT device_id, date_trunc('day', ts)::date AS day,
                         avg(rmssd_ms) AS hrv_rmssd
                  FROM hrv_readings GROUP BY 1,2) v
              ON v.device_id = h.device_id AND v.day = h.day
            ON CONFLICT (device_id, day) DO UPDATE SET
              resting_hr=EXCLUDED.resting_hr, hrv_rmssd=EXCLUDED.hrv_rmssd,
              recovery_score=EXCLUDED.recovery_score;""",
    },
    "day_strain": {
        "table": "gold_strain", "score_col": "day_strain",
        "fresh_source": "workout_events",
        "ddl": """CREATE TABLE IF NOT EXISTS gold_strain (
                    device_id TEXT, day DATE, workout_strain DOUBLE PRECISION,
                    day_strain DOUBLE PRECISION, PRIMARY KEY (device_id, day));""",
        # day strain ~ summed workout strain, nudged into Whoop's ~0-21 range.
        "sql": """
            INSERT INTO gold_strain (device_id, day, workout_strain, day_strain)
            SELECT device_id, date_trunc('day', ts)::date AS day,
                   sum(strain) AS workout_strain,
                   LEAST(21, sum(strain) + 4) AS day_strain
            FROM workout_events GROUP BY 1,2
            ON CONFLICT (device_id, day) DO UPDATE SET
              workout_strain=EXCLUDED.workout_strain, day_strain=EXCLUDED.day_strain;""",
    },
}


def compute(metric: str, **_):
    spec = COMPUTE.get(metric)
    if spec is None:
        raise AirflowSkipException(
            f"no compute registered for {metric} — needs a source stream we "
            f"don't produce yet (e.g. sleep stages). DAG generated from metadata "
            f"to show the pattern; skipping until the stream exists.")
    conn = _conn()
    with conn, conn.cursor() as cur:
        cur.execute(spec["ddl"])
        cur.execute(spec["sql"])
    conn.close()
    print(f"[compute] upserted {spec['table']}")


def dq_check(metric: str, truth_key: str, **_):
    spec = COMPUTE.get(metric)
    if spec is None:
        raise AirflowSkipException(f"{metric} not computed; nothing to check")
    truth = seed["params"].get(truth_key, {})
    t_mean, t_std = truth.get("mean"), truth.get("std")
    fresh_source = spec.get("fresh_source")
    conn = _conn()
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*), avg({spec['score_col']}), max(day) "
                    f"FROM {spec['table']};")
        n, avg, max_day = cur.fetchone()
        src_day = None
        if fresh_source:
            cur.execute(f"SELECT max(date_trunc('day', ts)::date) FROM {fresh_source};")
            (src_day,) = cur.fetchone()
    print(f"[DQ] {spec['table']}: rows={n} avg={avg} max_day={max_day} "
          f"| whoop truth mean={t_mean} std={t_std} | {fresh_source} latest={src_day}")

    # Evaluate checks -> a freshness lag (days) and a pass/fail status.
    freshness_lag = (src_day - max_day).days if (src_day and max_day) else 0
    drift = bool(t_mean and t_std and n and abs(float(avg) - t_mean) > 2 * t_std)
    failed = (not n) or drift or freshness_lag > 0

    # Persist the result so postgres-exporter can surface it to Prometheus
    # (data-quality signals land on the SAME Grafana as infra — single pane of glass).
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS dq_results (
                         metric TEXT PRIMARY KEY, rows BIGINT,
                         avg_value DOUBLE PRECISION, freshness_lag_days INT,
                         status INT, checked_at TIMESTAMPTZ DEFAULT now());""")
        cur.execute("""INSERT INTO dq_results
                         (metric, rows, avg_value, freshness_lag_days, status, checked_at)
                       VALUES (%s,%s,%s,%s,%s, now())
                       ON CONFLICT (metric) DO UPDATE SET
                         rows=EXCLUDED.rows, avg_value=EXCLUDED.avg_value,
                         freshness_lag_days=EXCLUDED.freshness_lag_days,
                         status=EXCLUDED.status, checked_at=now();""",
                    (metric, n, float(avg) if avg is not None else None,
                     freshness_lag, 0 if failed else 1))
    conn.close()

    if not n:
        raise ValueError(f"DQ FAIL: {spec['table']} is empty (no gold produced)")
    if drift:
        raise ValueError(
            f"DQ FAIL: {spec['table']} avg {float(avg):.1f} is >2σ from Whoop "
            f"mean {t_mean} — gold distribution drifted (upstream anomaly?)")
    # Freshness: gold must keep pace with the raw it derives from. If it lags,
    # a required input stream has gone silent — infra looks fine (Prometheus sees
    # no error), but readiness scores are stale. This is the data-layer anomaly.
    if freshness_lag > 0:
        raise ValueError(
            f"DQ FAIL: {spec['table']} latest day {max_day} lags {fresh_source} "
            f"latest {src_day} — a required input stream went silent; "
            f"readiness scores are stale.")


default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(seconds=30)}

# Generate one DAG per gold metric declared in metadata — the metadata-driven core.
for gm in meta["gold_metrics"]:
    name = gm["name"]
    dag_id = f"gold_{name}"
    dag = DAG(
        dag_id, default_args=default_args,
        schedule="@daily", start_date=datetime(2026, 6, 1),
        catchup=False, tags=["gold", "medallion"],
        doc_md=f"Bronze→gold for **{name}** (validated vs Whoop `gold.{name}`).",
    )
    with dag:
        c = PythonOperator(task_id="compute", python_callable=compute,
                           op_kwargs={"metric": name})
        d = PythonOperator(task_id="dq_check", python_callable=dq_check,
                           op_kwargs={"metric": name, "truth_key": f"gold.{name}"})
        c >> d
    globals()[dag_id] = dag