-- Hot-tier schema. Runs once on first boot (mounted into
-- /docker-entrypoint-initdb.d). One hypertable per bronze stream.
--
-- Idempotency: UNIQUE (event_id, ts) lets the router upsert with
-- ON CONFLICT DO NOTHING, so a re-delivered Kafka message never duplicates.
-- TimescaleDB quirk: any unique index MUST include the partitioning column
-- (ts) — that's why it's (event_id, ts), not just (event_id).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- heart_rate ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hr_readings (
  event_id    TEXT        NOT NULL,
  device_id   TEXT        NOT NULL,
  device_type TEXT,
  ts          TIMESTAMPTZ NOT NULL,
  bpm         DOUBLE PRECISION,
  UNIQUE (event_id, ts)
);
SELECT create_hypertable('hr_readings', 'ts', if_not_exists => TRUE);

-- hrv -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hrv_readings (
  event_id         TEXT        NOT NULL,
  device_id        TEXT        NOT NULL,
  device_type      TEXT,
  ts               TIMESTAMPTZ NOT NULL,
  rmssd_ms         DOUBLE PRECISION,
  respiratory_rate DOUBLE PRECISION,
  UNIQUE (event_id, ts)
);
SELECT create_hypertable('hrv_readings', 'ts', if_not_exists => TRUE);

-- workout -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workout_events (
  event_id      TEXT        NOT NULL,
  device_id     TEXT        NOT NULL,
  device_type   TEXT,
  ts            TIMESTAMPTZ NOT NULL,
  activity_name TEXT,
  duration_min  DOUBLE PRECISION,
  avg_hr        DOUBLE PRECISION,
  strain        DOUBLE PRECISION,
  UNIQUE (event_id, ts)
);
SELECT create_hypertable('workout_events', 'ts', if_not_exists => TRUE);

-- Hot tier keeps only recent data; cold tier (MinIO) is the long-term store.
-- Drop chunks older than 30 days automatically.
SELECT add_retention_policy('hr_readings',    INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('hrv_readings',   INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('workout_events', INTERVAL '30 days', if_not_exists => TRUE);