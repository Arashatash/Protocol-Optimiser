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


def _pixel_spacing_mm(ds: Dataset) -> tuple[float | None, float | None]:
    """Return (row_spacing_mm, col_spacing_mm) from PixelSpacing (0028,0030)."""
    elem = ds.get("PixelSpacing") or ds.get((0x0028, 0x0030))
    if elem is None:
        return None, None
    raw = getattr(elem, "value", elem)
    if raw is None:
        return None, None
    if not isinstance(raw, (list, tuple, pydicom.multival.MultiValue)):
        return None, None
    try:
        row_sp = float(raw[0]) if len(raw) > 0 else None
        col_sp = float(raw[1]) if len(raw) > 1 else None
    except (TypeError, ValueError, IndexError):
        return None, None
    return (
        row_sp if row_sp is not None and row_sp > 0 else None,
        col_sp if col_sp is not None and col_sp > 0 else None,
    )


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
        "manufacturer": "Unknown",
        "manufacturer_model_name": "Unknown",
        "acquisition_duration_ms": None,
        "duration_estimate_ms": None,
        "matrix_rows": None,
        "matrix_columns": None,
        "number_of_averages": None,
        "flip_angle_deg": None,
        "inversion_time_ms": None,
        "slice_thickness_mm": None,
        "spacing_between_slices_mm": None,
        "pixel_spacing_row_mm": None,
        "pixel_spacing_col_mm": None,
        "fov_row_mm": None,
        "fov_col_mm": None,
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

    flip_angle = _safe_positive_float(ds, "FlipAngle", (0x0018, 0x1314))
    ti_ms = _safe_float_ms(ds, "InversionTime", (0x0018, 0x0082))
    slice_thick = _safe_positive_float(ds, "SliceThickness", (0x0018, 0x0050))
    slice_spacing = _safe_positive_float(ds, "SpacingBetweenSlices", (0x0018, 0x0088))
    px_row, px_col = _pixel_spacing_mm(ds)

    fov_row = round(rows * px_row, 1) if rows and px_row else None
    fov_col = round(cols * px_col, 1) if cols and px_col else None

    out = {
        "study_description": _safe_str(ds, "StudyDescription", (0x0008, 0x1030)),
        "series_description": _safe_str(ds, "SeriesDescription", (0x0008, 0x103E)),
        "tr_ms": tr_ms,
        "te_ms": te_ms,
        "magnetic_field_strength_t": field_strength_t,
        "manufacturer": _safe_str(ds, "Manufacturer", (0x0008, 0x0070)),
        "manufacturer_model_name": _safe_str(ds, "ManufacturerModelName", (0x0008, 0x1090)),
        "acquisition_duration_ms": acq_ms,
        "duration_estimate_ms": est_ms,
        "matrix_rows": rows,
        "matrix_columns": cols,
        "number_of_averages": nsa,
        "flip_angle_deg": flip_angle,
        "inversion_time_ms": ti_ms,
        "slice_thickness_mm": slice_thick,
        "spacing_between_slices_mm": slice_spacing,
        "pixel_spacing_row_mm": px_row,
        "pixel_spacing_col_mm": px_col,
        "fov_row_mm": fov_row,
        "fov_col_mm": fov_col,
    }
    return out
