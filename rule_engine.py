"""Compare parsed DICOM data against rules.json — physics drift, semantic mapping, value metrics."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from generate_rules import _extract_json_object

load_dotenv(Path(__file__).resolve().parent / ".env")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_API_KEY = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
SEMANTIC_MODEL = os.environ.get("OPENROUTER_SEMANTIC_MODEL", "google/gemini-2.5-flash")


def _is_one_point_five_t(field_strength_t: float | None) -> bool:
    return isinstance(field_strength_t, (int, float)) and 1.35 <= float(field_strength_t) <= 1.65


def load_rules(path: str | Path | None = None) -> dict[str, Any]:
    """Load rules from JSON. Default: rules.json next to this module."""
    p = Path(path) if path is not None else Path(__file__).resolve().parent / "rules.json"
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _format_range(spec: dict[str, Any]) -> str:
    return f"{spec['min']}-{spec['max']} ms"


def _check_timing(
    label: str,
    actual: float | None,
    spec: dict[str, Any],
) -> tuple[bool, str | None]:
    """Return (ok, message). ok False means drift or missing value when a range exists."""
    if not spec or "min" not in spec or "max" not in spec:
        return True, None
    lo, hi = float(spec["min"]), float(spec["max"])
    if actual is None:
        return False, (
            f"{label}: PROTOCOL DRIFT DETECTED - value missing; "
            f"expected {_format_range(spec)}"
        )
    if not (lo <= actual <= hi):
        return False, (
            f"{label}: PROTOCOL DRIFT DETECTED - expected {_format_range(spec)}, "
            f"actual {actual:g} ms"
        )
    return True, None


def map_series_semantic(series_description: str, candidate_keys: list[str]) -> str | None:
    """
    Ask OpenRouter which protocol label is the closest clinical match to the scanner string.
    Returns one of candidate_keys or None.
    """
    if not OPENROUTER_API_KEY or not candidate_keys:
        return None
    s = (series_description or "").strip()
    if not s:
        return None

    labels_json = json.dumps(candidate_keys, ensure_ascii=False)
    user = (
        f'Scanner series description: "{s}"\n\n'
        f"Canonical protocol labels (choose at most one): {labels_json}\n\n"
        'Reply with ONLY a JSON object: {"match": "<exact string from list>"} '
        'or {"match": null} if none fit.'
    )
    payload = {
        "model": SEMANTIC_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an MRI protocol expert. Map vendor-specific series names to "
                    "canonical protocol labels. Output JSON only, no markdown."
                ),
            },
            {"role": "user", "content": user},
        ],
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/local/protocol-optimiser-mvp",
        "X-Title": "Protocol Optimiser Semantic Map",
    }
    try:
        resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=45)
        resp.raise_for_status()
        body = resp.json()
        content = body["choices"][0]["message"]["content"]
    except (requests.RequestException, KeyError, IndexError, TypeError):
        return None

    text = content.strip()
    try:
        text = _extract_json_object(text)
    except ValueError:
        m = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if m:
            text = m.group(0)
    try:
        obj = json.loads(text)
        key = obj.get("match")
        if key is None:
            return None
        key = str(key).strip()
        return key if key in candidate_keys else None
    except (json.JSONDecodeError, TypeError):
        return None


def _actual_duration_ms(parsed_data: dict[str, Any]) -> float | None:
    direct = parsed_data.get("acquisition_duration_ms")
    if isinstance(direct, (int, float)) and direct > 0:
        return float(direct)
    est = parsed_data.get("duration_estimate_ms")
    if isinstance(est, (int, float)) and est > 0:
        return float(est)
    return None


def _compute_efficiency_and_revenue(
    parsed_data: dict[str, Any],
    spec: dict[str, Any] | None,
) -> dict[str, Any]:
    """Efficiency score 0–100, revenue opportunity if >20% slower than target."""
    out: dict[str, Any] = {
        "efficiency_score": None,
        "efficiency_target_ms": None,
        "efficiency_actual_ms": None,
        "efficiency_ratio": None,
        "revenue_opportunity": False,
        "revenue_message": None,
    }
    if not spec or not isinstance(spec, dict):
        return out

    target = spec.get("target_duration_ms")
    if target is None:
        return out
    try:
        target_ms = float(target)
    except (TypeError, ValueError):
        return out
    if target_ms <= 0:
        return out

    actual_ms = _actual_duration_ms(parsed_data)
    if actual_ms is None:
        return out

    out["efficiency_target_ms"] = target_ms
    out["efficiency_actual_ms"] = actual_ms
    ratio = actual_ms / target_ms
    out["efficiency_ratio"] = ratio

    # Score: 100 if at or faster than target; decays when slower
    if ratio <= 1.0:
        score = 100.0
    elif ratio <= 1.2:
        score = max(0.0, 100.0 - (ratio - 1.0) * 100.0)
    else:
        score = max(0.0, 80.0 - (ratio - 1.2) * 150.0)

    out["efficiency_score"] = round(score, 1)

    if ratio > 1.2:
        out["revenue_opportunity"] = True
        pct = (ratio - 1.0) * 100.0
        out["revenue_message"] = (
            f"Scan time is ~{pct:.0f}% longer than the PubMed benchmark target. "
            "Optimization could add **1.5 more patients per day** to this scanner "
            "(capacity model — illustrative)."
        )

    return out


def _clinical_grade(
    physics_status: str,
    efficiency_score: float | None,
    revenue_opportunity: bool,
) -> str:
    if physics_status == "error":
        return "F"
    if physics_status == "fail":
        return "F"

    # physics pass
    if efficiency_score is None:
        return "B"

    if efficiency_score >= 80 and not revenue_opportunity:
        return "A"
    if efficiency_score >= 65:
        return "B"
    if efficiency_score >= 45:
        return "C"
    return "C"


def _normalized_position(actual: float | None, spec: dict[str, Any]) -> float | None:
    if actual is None or not spec or "min" not in spec or "max" not in spec:
        return None
    lo = float(spec["min"])
    hi = float(spec["max"])
    if hi <= lo:
        return None
    return (float(actual) - lo) / (hi - lo)


def _hardware_signal_warning(
    parsed_data: dict[str, Any],
    physics_status: str,
    te_spec: dict[str, Any],
    tr_spec: dict[str, Any],
) -> str | None:
    """
    Flag likely over-aggressive low-end timing on 1.5T hardware.
    """
    if physics_status != "pass":
        return None

    field_strength_t = parsed_data.get("magnetic_field_strength_t")
    if not _is_one_point_five_t(field_strength_t):
        return None

    te_norm = _normalized_position(
        parsed_data.get("te_ms") if isinstance(parsed_data.get("te_ms"), (int, float)) else None,
        te_spec,
    )
    tr_norm = _normalized_position(
        parsed_data.get("tr_ms") if isinstance(parsed_data.get("tr_ms"), (int, float)) else None,
        tr_spec,
    )

    if (te_norm is not None and te_norm < 0.25) or (tr_norm is not None and tr_norm < 0.25):
        return "Parameters appear too aggressive for 1.5T hardware; potential for graininess/low SNR."
    return None


def evaluate(parsed_data: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    """
    Drift check with optional semantic series mapping and value metrics.

    Returns status, messages, mapping fields, efficiency, grade, revenue flags.
    """
    base_extra: dict[str, Any] = {
        "mapped_series_key": None,
        "mapping_method": "none",
        "efficiency_score": None,
        "efficiency_target_ms": None,
        "efficiency_actual_ms": None,
        "efficiency_ratio": None,
        "revenue_opportunity": False,
        "revenue_message": None,
        "hardware_signal_warning": None,
        "clinical_grade": "F",
    }

    if parsed_data.get("read_error"):
        err = parsed_data["read_error"]
        out = {
            "status": "error",
            "messages": [f"DICOM read failed: {err}"],
        }
        out.update(base_extra)
        out["clinical_grade"] = "F"
        return out

    series_raw = parsed_data.get("series_description", "Unknown")
    series_key = str(series_raw).strip() if series_raw is not None else ""
    te = parsed_data.get("te_ms")
    tr = parsed_data.get("tr_ms")

    protocols = rules.get("series_protocols") or {}
    if not isinstance(protocols, dict):
        protocols = {}

    candidate_keys = list(protocols.keys())
    matched_key: str | None = None
    mapping_method = "none"

    if series_key in protocols:
        matched_key = series_key
        mapping_method = "exact"
    else:
        mapped = map_series_semantic(series_key, candidate_keys)
        if mapped:
            matched_key = mapped
            mapping_method = "semantic"

    if not matched_key:
        out = {
            "status": "pass",
            "messages": [
                "Drift check skipped: no matching protocol rule "
                f"(series {series_key!r}; semantic map unavailable or no match).",
            ],
        }
        out.update(base_extra)
        out["mapping_method"] = mapping_method
        out["clinical_grade"] = "B"
        return out

    spec = protocols[matched_key]
    te_spec = spec.get("te_ms") or {}
    tr_spec = spec.get("tr_ms") or {}

    messages: list[str] = []
    if mapping_method == "semantic":
        messages.append(
            f"Semantic map: {series_key!r} → {matched_key!r} (closest clinical match).",
        )

    all_ok = True

    te_ok, te_msg = _check_timing("TE", te if isinstance(te, (int, float)) else None, te_spec)
    if not te_ok and te_msg:
        all_ok = False
        messages.append(te_msg)

    tr_ok, tr_msg = _check_timing("TR", tr if isinstance(tr, (int, float)) else None, tr_spec)
    if not tr_ok and tr_msg:
        all_ok = False
        messages.append(tr_msg)

    physics_status = "pass" if all_ok else "fail"

    if all_ok:
        parts: list[str] = []
        if te_spec and "min" in te_spec and "max" in te_spec:
            parts.append(f"TE {_format_range(te_spec)}")
        if tr_spec and "min" in te_spec and "max" in te_spec:
            parts.append(f"TR {_format_range(tr_spec)}")
        ranges = "; ".join(parts) if parts else "n/a"
        messages.append(
            f"PASS - series {matched_key!r}: TE and TR within expected ranges ({ranges}).",
        )

    eff = _compute_efficiency_and_revenue(parsed_data, spec)
    revenue_opportunity = bool(eff.get("revenue_opportunity"))
    eff_score = eff.get("efficiency_score")
    hardware_signal_warning = _hardware_signal_warning(parsed_data, physics_status, te_spec, tr_spec)

    grade = _clinical_grade(physics_status, eff_score, revenue_opportunity)
    if physics_status == "pass" and revenue_opportunity and grade == "A":
        grade = "B"

    result: dict[str, Any] = {
        "status": "fail" if not all_ok else "pass",
        "messages": messages,
        "mapped_series_key": matched_key,
        "mapping_method": mapping_method,
        "efficiency_score": eff["efficiency_score"],
        "efficiency_target_ms": eff["efficiency_target_ms"],
        "efficiency_actual_ms": eff["efficiency_actual_ms"],
        "efficiency_ratio": eff["efficiency_ratio"],
        "revenue_opportunity": eff["revenue_opportunity"],
        "revenue_message": eff["revenue_message"],
        "hardware_signal_warning": hardware_signal_warning,
        "clinical_grade": grade,
    }

    if not all_ok:
        result["messages"] = [m for m in messages if "PASS" not in m]

    return result
