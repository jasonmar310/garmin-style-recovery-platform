"""
hrv_alerter.py — second application on the shared platform (interview Q2).

Purpose: exist as an INDEPENDENT consumer group so anomaly attribution has a
second dimension. It consumes telemetry.hrv only, keeps a per-device rolling
mean in memory, raises a low-HRV alert when the mean drops below threshold,
and — critically — touches NO database. During chaos-choke (hr_readings
locked) the router's lag climbs while this app's lag stays flat: same broker,
one group lagging => the fault is the router's sink, not Kafka.

Deliberate contrasts with router.py (see docs / ADR-0006 spirit):
  - enable.auto.commit=True : alerts are low-stakes; a lost sample on crash is
    acceptable. Effectively-once machinery would be over-engineering here.
  - auto.offset.reset=latest: an alerter that just started must not alert on
    three-day-old backlog. It cares about NOW; the router cares about ALL data.
"""
from __future__ import annotations
import argparse, json, os
from collections import defaultdict, deque
from datetime import datetime


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap",
                    default=os.getenv("BOOTSTRAP_SERVERS",
                                      "localhost:29092,localhost:29093,localhost:29094"))
    ap.add_argument("--group", default="hrv-alerter")
    ap.add_argument("--window", type=int, default=10,
                    help="rolling-window size per device")
    ap.add_argument("--threshold", type=float, default=30.0,
                    help="alert when rolling mean rmssd_ms drops below this")
    args = ap.parse_args()

    from confluent_kafka import Consumer
    from prometheus_client import start_http_server, Counter, Gauge

    port = int(os.getenv("ALERTER_METRICS_PORT", "8002"))
    start_http_server(port)
    events = Counter("hrv_alerter_events_total", "HRV events consumed")
    alerts = Counter("hrv_alerter_alerts_total", "Low-HRV alerts raised")
    tracked = Gauge("hrv_alerter_devices_tracked", "Devices with a rolling window")
    print(f"hrv-alerter: metrics on :{port}/metrics", flush=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": True,     # low-stakes output; simplicity wins (vs router)
        "auto.offset.reset": "latest",  # alert on NOW, never on old backlog (vs router)
    })
    consumer.subscribe(["telemetry.hrv"])
    print(f"hrv-alerter: consuming telemetry.hrv (group={args.group}, "
          f"window={args.window}, threshold={args.threshold})", flush=True)

    windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=args.window))
    in_alert: set[str] = set()          # edge-trigger: alert once per crossing

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"  consume error: {msg.error()}", flush=True)
                continue
            evt = json.loads(msg.value())
            dev, rmssd = evt.get("device_id"), evt.get("rmssd_ms")
            if dev is None or rmssd is None:
                continue
            events.inc()
            w = windows[dev]
            w.append(float(rmssd))
            tracked.set(len(windows))
            if len(w) == args.window:
                mean = sum(w) / len(w)
                if mean < args.threshold and dev not in in_alert:
                    in_alert.add(dev)
                    alerts.inc()
                    print(f"[ALERT] {datetime.now():%H:%M:%S} {dev} "
                          f"rolling HRV {mean:.1f} < {args.threshold}", flush=True)
                elif mean >= args.threshold:
                    in_alert.discard(dev)
    except KeyboardInterrupt:
        print("\nstopping hrv-alerter...")
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())