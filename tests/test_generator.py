"""
Unit tests for the pure value/math functions in simulator/generator.py.

Scope discipline: we test ONLY the input->output logic where a bug ships silently
(wrong distribution shape, wrong event count). The wiring — run_live / run_backfill /
build_event's Kafka path — needs a broker and is covered by chaos/ + `make verify`.

  - sample()           : each model produces values in-range with the right shape
  - events_per_tick()  : the throughput math (the --rate anomaly engine)
"""
import math

import numpy as np
import pytest

import generator


# --- sample() ---------------------------------------------------------------

def test_sample_gamma_recovers_mean_and_std_via_method_of_moments():
    # k=(mean/std)^2, theta=std^2/mean  =>  gamma mean=mean, std=std.
    # Wide bounds so clipping doesn't bias the moments we're checking.
    np.random.seed(0)
    p = {"mean": 16.0, "std": 10.0, "min": 0.0, "max": 1e9}
    draws = np.array([generator.sample("gamma", p, hour=12.0) for _ in range(50_000)])
    assert draws.mean() == pytest.approx(16.0, abs=0.5)
    assert draws.std() == pytest.approx(10.0, abs=0.5)


def test_sample_gamma_clips_to_bounds():
    np.random.seed(1)
    p = {"mean": 16.0, "std": 10.0, "min": 5.0, "max": 30.0}
    draws = [generator.sample("gamma", p, hour=12.0) for _ in range(5_000)]
    assert all(5.0 <= x <= 30.0 for x in draws)


def test_sample_gamma_rejects_nonpositive_mean():
    # gamma's method-of-moments is undefined for mean<=0; fail loud, not silent.
    with pytest.raises(ValueError):
        generator.sample("gamma", {"mean": 0.0, "std": 1.0}, hour=12.0)


def test_sample_circadian_stays_in_physiological_range():
    np.random.seed(2)
    p = {"mean": 67.0, "std": 3.7}
    draws = [generator.sample("circadian_gaussian", p, hour=h % 24)
             for h in range(0, 24_000)]
    assert all(40.0 <= x <= 190.0 for x in draws), "circadian must clip to [40,190]"


def test_sample_circadian_envelope_peaks_mid_afternoon():
    # day_factor: ~0 at 03:00 (trough), ~1 at 15:00 (peak). The anchored mean must
    # rise across the day, otherwise the circadian shape is broken.
    np.random.seed(3)
    p = {"mean": 67.0, "std": 3.7}
    trough = np.mean([generator.sample("circadian_gaussian", p, 3.0) for _ in range(5_000)])
    peak = np.mean([generator.sample("circadian_gaussian", p, 15.0) for _ in range(5_000)])
    assert peak > trough + 30.0  # envelope adds ~45 bpm at the peak


def test_sample_gaussian_clips_to_bounds():
    np.random.seed(4)
    p = {"mean": 120.0, "std": 13.6, "min": 88.0, "max": 152.0}
    draws = [generator.sample("gaussian", p, hour=12.0) for _ in range(10_000)]
    assert all(88.0 <= x <= 152.0 for x in draws)


def test_sample_gaussian_clip_is_tight():
    # A band far narrower than the std must still bound every draw.
    np.random.seed(5)
    p = {"mean": 120.0, "std": 13.6, "min": 119.0, "max": 121.0}
    draws = [generator.sample("gaussian", p, hour=12.0) for _ in range(2_000)]
    assert all(119.0 <= x <= 121.0 for x in draws)


def test_sample_categorical_returns_a_declared_category():
    np.random.seed(6)
    p = {"categories": {"Walking": 0.7, "Running": 0.2, "Hiking": 0.1}}
    out = {generator.sample("categorical", p, hour=12.0) for _ in range(500)}
    assert out <= {"Walking", "Running", "Hiking"}
    assert "Walking" in out  # the dominant class should appear


# --- events_per_tick() ------------------------------------------------------

def test_events_per_tick_continuous_scales_with_devices_and_rate():
    hr = {"frequency_hz": 1}
    assert generator.events_per_tick(hr, devices=200, rate=1) == pytest.approx(200.0)
    # --rate is a linear throughput multiplier (the surge / anomaly engine).
    assert generator.events_per_tick(hr, devices=200, rate=10) == pytest.approx(2000.0)


def test_events_per_tick_respects_fractional_frequency():
    hrv = {"frequency_hz": 0.0033}  # ~every 5 min
    assert generator.events_per_tick(hrv, devices=300, rate=1) == pytest.approx(0.99)


def test_events_per_tick_event_stream_uses_session_probability():
    evt = {"frequency_hz": "event"}
    per_sec = generator.SESSIONS_PER_DEVICE_PER_DAY / 86_400.0
    expected = 200 * per_sec * 1 * generator.TICK
    assert generator.events_per_tick(evt, devices=200, rate=1) == pytest.approx(expected)
    # and still scales linearly with rate
    assert generator.events_per_tick(evt, devices=200, rate=10) == pytest.approx(expected * 10)
