"""DICOM header parsing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pydicom
from pydicom.dataset import Dataset


def _safe_str(ds: Dataset, keyword: str, tag: tuple[int, int]) -> str:
    """Return decoded string tag value, or 'Unknown' if missing or empty."""
    elem = ds.get(keyword)
    if elem is None:
        elem = ds.get(tag)
    if elem is None:
        return "Unknown"
    raw = getattr(elem, "value", elem)
    if raw is None:
        return "Unknown"
    if isinstance(raw, pydicom.multival.MultiValue):
        parts = [str(x).strip() for x in raw if x is not None and str(x).strip()]
        return " / ".join(parts) if parts else "Unknown"
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8", errors="replace").strip()
        except Exception:
            return "Unknown"
        return text if text else "Unknown"
    text = str(raw).strip()
    return text if text else "Unknown"


def _first_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, tuple, pydicom.multival.MultiValue)):
        return _first_scalar(value[0]) if len(value) else None
    return value


def _safe_float_ms(ds: Dataset, keyword: str, tag: tuple[int, int]) -> float | None:
    """Return TR/TE in ms as float, or None if missing or not numeric."""
    elem = ds.get(keyword)
    if elem is None:
        elem = ds.get(tag)
    if elem is None:
        return None
    raw = getattr(elem, "value", elem)
    raw = _first_scalar(raw)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("ascii", errors="replace").strip()
        except Exception:
            return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _safe_positive_int(ds: Dataset, keyword: str, tag: tuple[int, int]) -> int | None:
    elem = ds.get(keyword) or ds.get(tag)
    if elem is None:
        return None
    raw = _first_scalar(getattr(elem, "value", elem))
    if raw is None:
        return None
    try:
        v = int(float(raw))
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_positive_float(ds: Dataset, keyword: str, tag: tuple[int, int]) -> float | None:
    elem = ds.get(keyword) or ds.get(tag)
    if elem is None:
        return None
    raw = _first_scalar(getattr(elem, "value", elem))
    if raw is None:
        return None
    try:
        v = float(raw)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _acquisition_duration_ms(ds: Dataset) -> float | None:
    """
    (0018,9073) AcquisitionDuration — DICOM gives total duration in seconds (VR FD).
    Return milliseconds, or None if missing.
    """
    elem = ds.get("AcquisitionDuration") or ds.get((0x0018, 0x9073))
    if elem is None:
        return None
    raw = _first_scalar(getattr(elem, "value", elem))
    if raw is None:
        return None
    try:
        sec = float(raw)
    except (TypeError, ValueError):
        return None
    if sec <= 0:
        return None
    return sec * 1000.0


def _duration_heuristic_ms(ds: Dataset, tr_ms: float | None) -> float | None:
    """
    Rough scan-time proxy when AcquisitionDuration is absent (single-instance estimate).
    Uses TR, Number of Averages, Echo Train Length — not a substitute for true duration.
    """
    if tr_ms is None:
        return None
    nsa = _safe_positive_float(ds, "NumberOfAverages", (0x0018, 0x0083)) or 1.0
    etl = float(_safe_positive_int(ds, "EchoTrainLength", (0x0018, 0x0091)) or 1)
    # Order-of-magnitude heuristic for cartesian/TSE-style burden (not validated per vendor)
    return float(tr_ms) * max(1.0, nsa) * max(1.0, etl / 4.0)


def parse_dicom(path: str | Path) -> dict[str, Any]:
    """
    Read a DICOM file and return study/series timing fields.

    Missing string tags use 'Unknown'; missing or invalid TR/TE use None.
    On file read failure, returns the same keys plus read_error (str).
    """
    path = Path(path)
    empty: dict[str, Any] = {
        "study_description": "Unknown",
        "series_description": "Unknown",
        "tr_ms": None,
        "te_ms": None,
        "magnetic_field_strength_t": None,
        "acquisition_duration_ms": None,
        "duration_estimate_ms": None,
        "matrix_rows": None,
        "matrix_columns": None,
        "number_of_averages": None,
    }

    try:
        ds = pydicom.dcmread(
            path,
            stop_before_pixels=True,
            force=True,
        )
    except Exception as exc:
        out = dict(empty)
        out["read_error"] = f"{type(exc).__name__}: {exc}"
        return out

    if not isinstance(ds, Dataset):
        out = dict(empty)
        out["read_error"] = "Loaded object is not a DICOM dataset."
        return out

    tr_ms = _safe_float_ms(ds, "RepetitionTime", (0x0018, 0x0080))
    te_ms = _safe_float_ms(ds, "EchoTime", (0x0018, 0x0081))

    acq_ms = _acquisition_duration_ms(ds)
    est_ms = None
    if acq_ms is None:
        est_ms = _duration_heuristic_ms(ds, tr_ms)

    field_strength_t = _safe_positive_float(ds, "MagneticFieldStrength", (0x0018, 0x0087))
    rows = _safe_positive_int(ds, "Rows", (0x0028, 0x0010))
    cols = _safe_positive_int(ds, "Columns", (0x0028, 0x0011))
    nsa = _safe_positive_float(ds, "NumberOfAverages", (0x0018, 0x0083))

    out = {
        "study_description": _safe_str(ds, "StudyDescription", (0x0008, 0x1030)),
        "series_description": _safe_str(ds, "SeriesDescription", (0x0008, 0x103E)),
        "tr_ms": tr_ms,
        "te_ms": te_ms,
        "magnetic_field_strength_t": field_strength_t,
        "acquisition_duration_ms": acq_ms,
        "duration_estimate_ms": est_ms,
        "matrix_rows": rows,
        "matrix_columns": cols,
        "number_of_averages": nsa,
    }
    return out
