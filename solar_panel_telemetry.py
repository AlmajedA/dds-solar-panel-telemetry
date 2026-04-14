#!/usr/bin/env python3
"""
solar_panel_telemetry.py
Generate synthetic telemetry data individually for each solar panel.

Features:
- Diurnal irradiance curve (daylight window with smooth sunrise/sunset)
- Random cloud cover & noise
- Panel-to-panel variance & slow degradation
- Temperature effect on power (simple linear model)
- Occasional faults (with categorical status)
- CSV (default) or JSONL output
- Deterministic reproducibility via --seed

Example:
  python solar_panel_telemetry.py --panels 100 --start "2025-10-18T06:00:00" \
    --hours 12 --step 60 --out telemetry.csv

  # Stream to stdout (CSV)
  python solar_panel_telemetry.py --panels 10 --hours 1 --step 30

  # JSONL output
  python solar_panel_telemetry.py --format jsonl --panels 5 --minutes 30 --step 10
"""

import argparse
import csv
import json
import math
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Tuple, Optional

# ----------------------------
# Models & Utilities
# ----------------------------

@dataclass
class PanelSpec:
    panel_id: str
    p_stc_w: float       # STC max power (W)
    v_mppt: float        # MPPT voltage around Pmax (V)
    i_stc_a: float       # Short-circuit-ish current scale (A)
    temp_coeff_p: float  # Power temperature coefficient (%/°C), e.g., -0.4%/°C => -0.004
    degradation_per_day: float  # slow daily degradation factor (fraction per day)
    efficiency_jitter: float    # per-panel multiplicative jitter
    orientation_deg: float      # simple azimuth effect placeholder
    tilt_deg: float             # simple tilt effect placeholder
    string_id: str

FAULT_CATALOG = [
    ("NONE",          "OK",       0.0),
    ("SHADING",       "WARNING",  0.0015),
    ("HOTSPOT",       "FAULT",    0.0005),
    ("STRING_OPEN",   "FAULT",    0.0004),
    ("INVERTER_TRIP", "FAULT",    0.0003),
    ("SOILING",       "WARNING",  0.0008),
]

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate per-panel synthetic solar telemetry.")
    ap.add_argument("--panels", type=int, default=50, help="Number of panels.")
    ap.add_argument("--start", type=str, default=None,
                    help='ISO start time (e.g., "2025-10-18T06:00:00"). Default: now (UTC).')
    ap.add_argument("--hours", type=float, default=None, help="Duration in hours (mutually exclusive with --minutes).")
    ap.add_argument("--minutes", type=float, default=None, help="Duration in minutes (mutually exclusive with --hours).")
    ap.add_argument("--step", type=int, default=60, help="Step in seconds between samples.")
    ap.add_argument("--out", type=str, default="-", help='Output file path or "-" for stdout.')
    ap.add_argument("--format", choices=["csv", "jsonl"], default="csv", help="Output format.")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    ap.add_argument("--site", type=str, default="Site-A", help="Logical site name.")
    ap.add_argument("--daylight", type=str, default="06:00-18:00",
                    help='Local daylight window "HH:MM-HH:MM" for diurnal curve (approx).')
    ap.add_argument("--timezone", type=str, default="UTC",
                    help="Label only (no tz math); timestamps are emitted in UTC.")
    return ap.parse_args()

def utc_now_truncated() -> datetime:
    # ensure naive UTC for consistency, then make it timezone-aware UTC
    return datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)

def make_time_range(start_utc: datetime, duration: timedelta, step_s: int) -> Iterator[datetime]:
    current = start_utc
    end = start_utc + duration
    while current <= end:
        yield current
        current += timedelta(seconds=step_s)

def parse_daylight_window(daylight: str) -> Tuple[int, int]:
    """
    Return seconds-from-midnight (start, end) for daylight window.
    """
    try:
        s, e = daylight.split("-")
        sh, sm = [int(x) for x in s.split(":")]
        eh, em = [int(x) for x in e.split(":")]
        start_s = sh * 3600 + sm * 60
        end_s = eh * 3600 + em * 60
        return start_s, end_s
    except Exception:
        # default 6am-6pm
        return 6 * 3600, 18 * 3600

def seconds_since_midnight(t: datetime) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second

def smoothstep(x: float) -> float:
    """ Quintic smoothstep for nice sunrise/sunset edges. """
    return 6*x**5 - 15*x**4 + 10*x**3

def diurnal_irradiance_factor(ts: datetime, dl_start: int, dl_end: int) -> float:
    """
    Returns [0..1] factor representing available irradiance based on a daylight window.
    Uses a smoothstep ramp up/down to emulate sunrise/sunset; zero outside window.
    """
    ssm = seconds_since_midnight(ts)
    if ssm <= dl_start or ssm >= dl_end:
        return 0.0
    span = dl_end - dl_start
    x = (ssm - dl_start) / span  # 0..1 over daylight
    # Peak mid-day with a gentle bell-shaped curve: sin(pi*x)^{1.5}, wrapped in smooth edges
    base = math.sin(math.pi * x)
    if base < 0:
        return 0.0
    # emphasize midday peak
    shaped = base ** 1.5
    # apply soft start/end
    edge = smoothstep(x)
    return max(0.0, min(1.0, shaped * (0.7 + 0.3 * edge)))

def cloud_cover_factor() -> float:
    """
    Random multiplicative factor to emulate transient clouds (mean ~0.9, heavier tail).
    """
    # mix of light and heavier attenuation
    light = random.uniform(0.85, 1.0)
    occasional = 1.0 - random.random() ** 6 * 0.25  # 0.75..1 more likely near 1
    return max(0.6, min(1.0, (light * 0.7 + occasional * 0.3)))

def ambient_temperature_c(ts: datetime, base: float = 26.0) -> float:
    """
    Simple daily temperature model (°C): base +/- 8°C over the day with peak mid-afternoon.
    """
    # peak around 15:00 => shift the sine
    ssm = seconds_since_midnight(ts)
    phase = (ssm / 86400.0) * 2 * math.pi
    return base + 8.0 * math.sin(phase - math.pi / 6)

def panel_cell_temp_c(ambient_c: float, irradiance_wm2: float) -> float:
    """
    Very simple NOCT-like approximation for cell temperature.
    """
    # rule of thumb: + (irradiance/800)*20°C above ambient
    return ambient_c + (irradiance_wm2 / 800.0) * 20.0

def assign_fault() -> Tuple[str, str]:
    """
    Randomly assign a fault (type, status) based on FAULT_CATALOG weights.
    Most of the time 'NONE','OK'.
    """
    r = random.random()
    acc = 0.0
    for fault, status, p in FAULT_CATALOG:
        acc += p
        if r <= acc:
            return fault, status
    return "NONE", "OK"

def orientation_tilt_modifier(orientation_deg: float, tilt_deg: float) -> float:
    """
    Placeholder modifier for panel pointing. Keeps it simple (0.9..1.05).
    """
    # Favor south-ish orientations (180 deg) and moderate tilt (20-30 deg)
    orient_score = 1.0 - (abs(180 - orientation_deg) / 180.0) * 0.1  # up to -10%
    tilt_score = 1.0 - (abs(25 - tilt_deg) / 25.0) * 0.08            # up to -8%
    return max(0.85, min(1.05, orient_score * tilt_score))

def generate_panel_fleet(n: int, seed: int) -> List[PanelSpec]:
    random.seed(seed)
    fleet = []
    for i in range(n):
        # draw around typical 400W panel with variation
        p_stc = random.uniform(370, 430)  # watts
        v_mppt = random.uniform(33, 40)   # volts at Pmax
        i_stc = p_stc / v_mppt * random.uniform(0.95, 1.05)
        coeff = random.uniform(-0.0045, -0.0035)  # -0.45% to -0.35% per °C
        degr = random.uniform(0.00003, 0.00007)   # ~0.003%–0.007% per day
        jitter = random.uniform(0.98, 1.02)
        orient = random.choice([150, 165, 180, 195, 210]) + random.uniform(-5, 5)
        tilt = random.choice([15, 20, 25, 30, 35]) + random.uniform(-2, 2)
        string = f"S{1 + (i // 20):02d}"
        fleet.append(PanelSpec(
            panel_id=f"P{i+1:05d}",
            p_stc_w=p_stc,
            v_mppt=v_mppt,
            i_stc_a=i_stc,
            temp_coeff_p=coeff,
            degradation_per_day=degr,
            efficiency_jitter=jitter,
            orientation_deg=orient,
            tilt_deg=tilt,
            string_id=string
        ))
    return fleet

def compute_telemetry(
    spec: PanelSpec,
    ts: datetime,
    dl_start: int,
    dl_end: int,
    day_index: int,
    cloud_factor: float
) -> Dict[str, object]:
    # Diurnal irradiance base (fraction 0..1)
    sun_f = diurnal_irradiance_factor(ts, dl_start, dl_end)
    # Convert to notional irradiance (W/m^2) with clouds
    irr = 1000.0 * sun_f * cloud_factor  # 0..1000 W/m^2

    # Ambient & cell temps
    amb_c = ambient_temperature_c(ts)
    cell_c = panel_cell_temp_c(amb_c, irr)

    # Orientation/tilt modifier
    orient_mod = orientation_tilt_modifier(spec.orientation_deg, spec.tilt_deg)

    # Daily degradation
    degr_factor = (1.0 - spec.degradation_per_day) ** day_index

    # Raw DC power before temperature & noise
    p_raw = spec.p_stc_w * (irr / 1000.0) * spec.efficiency_jitter * orient_mod * degr_factor

    # Temperature adjustment of power
    delta_t = cell_c - 25.0  # STC cell temp = 25°C
    p_temp = p_raw * (1.0 + spec.temp_coeff_p * delta_t)

    # Add small random electronic noise (±2%)
    p_noisy = p_temp * random.uniform(0.98, 1.02)

    # Ensure non-negative and cap to ~110% STC to allow brief over-performance
    p_dc = max(0.0, min(spec.p_stc_w * 1.10, p_noisy))

    # Derive current & voltage around MPPT point (very rough)
    if p_dc <= 0.1:
        v = spec.v_mppt * random.uniform(0.92, 1.05)
        i = 0.0
    else:
        v = spec.v_mppt * random.uniform(0.97, 1.03)
        i = p_dc / v

    # Irradiance noise floor at night
    if irr < 1.0:
        irr = 0.0

    # Faults & status
    fault, status = assign_fault()
    if fault != "NONE":
        # Apply effect of some faults
        if fault == "SHADING":
            p_dc *= random.uniform(0.6, 0.9)
        elif fault == "HOTSPOT":
            p_dc *= random.uniform(0.4, 0.8)
            cell_c += random.uniform(3, 8)
        elif fault == "STRING_OPEN":
            p_dc = 0.0
            i = 0.0
        elif fault == "INVERTER_TRIP":
            # emulate brief zero output despite sun
            p_dc = 0.0
            i = 0.0
        elif fault == "SOILING":
            p_dc *= random.uniform(0.8, 0.95)

    # Recompute current if power changed due to fault
    if v > 0:
        i = p_dc / v
    else:
        i = 0.0

    # Clip to sane ranges
    i = max(0.0, i)
    v = max(0.0, v)
    p_dc = max(0.0, p_dc)

    return {
        "timestamp_utc": ts.isoformat().replace("+00:00", "Z"),
        "panel_id": spec.panel_id,
        "string_id": spec.string_id,
        "status": status,
        "fault": fault,
        "power_w": round(p_dc, 2),
        "voltage_v": round(v, 2),
        "current_a": round(i, 3),
        "irradiance_wm2": round(irr, 1),
        "ambient_temp_c": round(amb_c, 2),
        "cell_temp_c": round(cell_c, 2),
        "orientation_deg": round(spec.orientation_deg, 1),
        "tilt_deg": round(spec.tilt_deg, 1)
    }

def day_number_since(start: datetime, t: datetime) -> int:
    return (t.date() - start.date()).days

# ----------------------------
# Main
# ----------------------------

def main():
    args = parse_args()
    random.seed(args.seed)

    # Time setup
    if args.start:
        start_utc = datetime.fromisoformat(args.start)
        if start_utc.tzinfo is None:
            start_utc = start_utc.replace(tzinfo=timezone.utc)
        else:
            start_utc = start_utc.astimezone(timezone.utc)
    else:
        start_utc = utc_now_truncated()

    if (args.hours is None) == (args.minutes is None):
        # default 8 hours if neither set
        duration = timedelta(hours=8) if args.hours is None and args.minutes is None else None
        if duration is None:
            print("Provide ONLY one of --hours or --minutes.", file=sys.stderr)
            sys.exit(2)
    else:
        duration = timedelta(hours=args.hours) if args.hours is not None else timedelta(minutes=args.minutes)

    # Daylight window (seconds from midnight)
    dl_start_s, dl_end_s = parse_daylight_window(args.daylight)

    # Fleet
    fleet = generate_panel_fleet(args.panels, seed=args.seed)

    # Output target
    out_fh = sys.stdout if args.out == "-" else open(args.out, "w", newline="", encoding="utf-8")

    try:
        if args.format == "csv":
            fieldnames = [
                "timestamp_utc","panel_id","string_id","status","fault",
                "power_w","voltage_v","current_a","irradiance_wm2",
                "ambient_temp_c","cell_temp_c","orientation_deg","tilt_deg"
            ]
            writer = csv.DictWriter(out_fh, fieldnames=fieldnames)
            writer.writeheader()
        else:
            writer = None  # JSONL

        # Generate
        for ts in make_time_range(start_utc, duration, args.step):
            day_idx = day_number_since(start_utc, ts)

            # Shared cloud factor at site level + small per-string variance
            site_cloud = cloud_cover_factor()
            string_cloud: Dict[str, float] = {}

            for spec in fleet:
                if spec.string_id not in string_cloud:
                    string_cloud[spec.string_id] = max(0.6, min(1.0, site_cloud * random.uniform(0.95, 1.05)))
                rec = compute_telemetry(
                    spec=spec,
                    ts=ts,
                    dl_start=dl_start_s,
                    dl_end=dl_end_s,
                    day_index=day_idx,
                    cloud_factor=string_cloud[spec.string_id]
                )

                if args.format == "csv":
                    writer.writerow(rec)
                else:
                    out_fh.write(json.dumps(rec) + "\n")

    finally:
        if out_fh is not sys.stdout:
            out_fh.close()

if __name__ == "__main__":
    main()
