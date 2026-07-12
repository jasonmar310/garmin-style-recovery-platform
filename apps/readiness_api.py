"""
readiness_api.py — read-path application (interview Q2/Q3).

Serves the latest gold_recovery row per device over HTTP and instruments
itself with pure RED metrics (rate / errors / duration) — the Duration axis
this stack previously lacked.

Two deliberate omissions (design discipline, be ready to defend):
  - NO staleness metric here. Data freshness is the DQ layer's job
    (Airflow dq_check -> dq_results -> Prometheus). During chaos-stale this
    API's own metrics stay green while the payload's `day` is yesterday —
    that contrast IS the two-layer observability story, seen from the user.
  - NO connection pool: one connection per request is fine at demo scale and
    makes the API visible on the Data Stores connections panel. Production
    path: PgBouncer transaction mode (already a compose profile) — run with
    PGPORT=6432 and it just works, same as route-pooled.

Run:  uvicorn apps.readiness_api:app --host 0.0.0.0 --port 8003
"""
from __future__ import annotations
import os

import psycopg2
from fastapi import FastAPI, HTTPException
from prometheus_client import make_asgi_app, Counter, Histogram

app = FastAPI(title="readiness-api")
metrics_app = make_asgi_app()

@app.get("/metrics")
def metrics_no_slash():
    from starlette.responses import Response
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

REQS = Counter("readiness_api_requests_total",
               "Requests by outcome", ["status"])       # low-cardinality label only
LAT = Histogram("readiness_api_request_duration_seconds",
                "End-to-end request latency",
                buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5))


def _conn():
    # Host-side defaults: PGPORT=5433 (the container's 5432 is host-published
    # as 5433 — see docs/PITFALLS #2). Sourcing .env sets these anyway.
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5433"),
        dbname=os.getenv("TIMESCALE_DB", "telemetry"),
        user=os.getenv("TIMESCALE_USER", "ian"),
        password=os.getenv("TIMESCALE_PASSWORD", ""),
        connect_timeout=3)


@app.get("/readiness/{device_id}")
def readiness(device_id: str):
    with LAT.time():
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    """SELECT day, recovery_score, resting_hr, hrv_rmssd
                       FROM gold_recovery
                       WHERE device_id = %s
                       ORDER BY day DESC LIMIT 1""", (device_id,))
                row = cur.fetchone()
            conn.close()
        except Exception as e:
            REQS.labels("500").inc()
            raise HTTPException(status_code=500, detail=f"db error: {e}")

    if row is None:
        REQS.labels("404").inc()
        raise HTTPException(status_code=404, detail="no readiness for device")

    day, score, rhr, hrv = row
    REQS.labels("200").inc()
    # NOTE: `day` is returned as-is. Whether it is TODAY is the DQ layer's
    # verdict, not this API's — during chaos-stale, this field is the
    # user-visible symptom while every metric here stays green.
    return {"device_id": device_id, "day": str(day),
            "recovery_score": score, "resting_hr": rhr, "hrv_rmssd": hrv}


@app.get("/healthz")
def healthz():
    return {"ok": True}