"""
profile_seed.py — source-data-driven, metadata-driven seed extraction.

Reads metadata/streams.yaml, collects every declared `seed: {dataset, column}`
target across streams[].signals[] and gold_metrics[], profiles the matching
column from the real CSV, and writes metadata/seed_params.yaml.

Nothing about the signals is hardcoded here: add a signal in streams.yaml and
it is profiled automatically. Real personal data never leaves the box — only
the non-identifying distribution parameters are written out.
"""
from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "metadata" / "streams.yaml"
OUT = ROOT / "metadata" / "seed_params.yaml"


def collect_targets(meta: dict) -> list[dict]:
    """Walk the metadata tree and gather every (dataset, column, model) target."""
    targets: list[dict] = []
    for stream in meta.get("streams", []):
        for sig in stream.get("signals", []):
            seed = sig.get("seed")
            if seed:
                targets.append({
                    "key": f"{stream['name']}.{sig['field']}",
                    "dataset": seed["dataset"],
                    "column": seed["column"],
                    "model": sig.get("model", "gaussian"),
                })
    for gm in meta.get("gold_metrics", []):
        truth = gm.get("source_truth")
        if truth:
            targets.append({
                "key": f"gold.{gm['name']}",
                "dataset": truth["dataset"],
                "column": truth["column"],
                "model": "validation_truth",
            })
    return targets


def profile_numeric(s: pd.Series) -> dict:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {"n": 0}
    q = s.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    return {
        "n": int(s.size),
        "mean": round(float(s.mean()), 3),
        "std": round(float(s.std(ddof=0)), 3),
        "min": round(float(s.min()), 3),
        "p05": round(float(q.loc[0.05]), 3),
        "p25": round(float(q.loc[0.25]), 3),
        "p50": round(float(q.loc[0.50]), 3),
        "p75": round(float(q.loc[0.75]), 3),
        "p95": round(float(q.loc[0.95]), 3),
        "max": round(float(s.max()), 3),
    }


def profile_categorical(s: pd.Series) -> dict:
    s = s.dropna().astype(str)
    if s.empty:
        return {"n": 0}
    probs = (s.value_counts(normalize=True).round(4)).to_dict()
    return {"n": int(s.size), "categories": probs}


def main() -> int:
    meta = yaml.safe_load(META.read_text())
    sources = meta.get("seed_sources", {})

    # Load only the datasets that exist on disk (fitbit may be absent).
    frames: dict[str, pd.DataFrame] = {}
    for name, cfg in sources.items():
        path = ROOT / cfg["path"]
        if path.exists():
            frames[name] = pd.read_csv(path)
        elif not cfg.get("optional"):
            print(f"[warn] required seed source missing: {path}", file=sys.stderr)

    out: dict = {"_meta": {"generated_from": "metadata/streams.yaml",
                           "datasets_loaded": sorted(frames)}, "params": {}}

    for t in collect_targets(meta):
        ds, col = t["dataset"], t["column"]
        if ds not in frames:
            continue
        df = frames[ds]
        if col not in df.columns:
            print(f"[warn] {ds} has no column {col!r}", file=sys.stderr)
            continue
        prof = (profile_categorical(df[col]) if t["model"] == "categorical"
                else profile_numeric(df[col]))
        prof["model"] = t["model"]
        prof["source"] = f"{ds}:{col}"
        out["params"][t["key"]] = prof

    OUT.write_text(yaml.safe_dump(out, sort_keys=False, allow_unicode=True))
    print(f"[ok] profiled {len(out['params'])} signals -> {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
