"""
Phase 6: Generate rules.json via OpenRouter (Claude) from a natural-language protocol prompt.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# Set OPENROUTER_API_KEY in the environment (recommended), or fill the fallback for local use only—never commit secrets.
_OPENROUTER_API_KEY_FALLBACK = ""
OPENROUTER_API_KEY = (
    os.environ.get("OPENROUTER_API_KEY") or _OPENROUTER_API_KEY_FALLBACK
).strip()

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, requests.ConnectionError)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=16),
    reraise=True,
)
def _post_openrouter(headers: dict[str, str], payload: dict, timeout: int) -> dict:
    """POST to OpenRouter with automatic retry on transient failures."""
    resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.6")

ROOT = Path(__file__).resolve().parent
RULES_PATH = ROOT / "rules.json"
EVIDENCE_STRENGTH_LEVELS = {"high", "moderate", "low"}

ONE_POINT_FIVE_T_NOTE = (
    "Note: The current scanner is 1.5 Tesla. Please ensure the TE/TR ranges are "
    "optimized for 1.5T signal-to-noise ratios, even if standard references focus on 3T."
)


def _is_one_point_five_t(scanner_tesla: float | None) -> bool:
    return isinstance(scanner_tesla, (int, float)) and 1.35 <= float(scanner_tesla) <= 1.65

SYSTEM_PROMPT = """You are a Senior Neuroradiology Consultant and MRI protocol specialist.

The user will describe a clinical MRI protocol (e.g. indication, sequences, site conventions).

Your ONLY output must be a single JSON object—no markdown, no code fences, no backticks, no commentary before or after the JSON.

The JSON MUST match this exact structure and key names:

{
  "clinical_rationale": {
    "summary": "2-4 sentences summarizing the consensus from the available evidence and current neuroradiology best practice",
    "evidence_strength": "High | Moderate | Low",
    "key_changes": "short paragraph explaining what has changed in recent literature versus older standard protocols, or state that no significant changes were identified if the evidence is stable"
  },
  "study_rules": [
    {
      "id": "string_snake_case_identifier",
      "study_description_substring": "short phrase that might appear in Study Description (0008,1030) for this protocol",
      "required_series_keywords": ["keyword1", "keyword2"]
    }
  ],
  "series_protocols": {
    "EXACT SERIES LABEL AS USED ON SCANNER": {
      "te_ms": { "min": <number>, "max": <number> },
      "tr_ms": { "min": <number>, "max": <number> },
      "target_duration_ms": <optional total acquisition time benchmark in ms>
    }
  }
}

Rules:
- clinical_rationale: required for newly generated output. Explain the trade-offs between signal-to-noise and scan speed, including how those trade-offs differ in 1.5T versus 3T practice when relevant.
- evidence_strength grading rubric (you MUST follow this, aligned with GRADE certainty methodology):
  - "High": you have strong, specific clinical evidence with explicit TE/TR or sequence parameter numbers from established neuroradiology practice (GRADE: high certainty).
  - "Moderate": you have general clinical consensus but limited explicit parameter numbers from recent literature (GRADE: moderate certainty).
  - "Low": the output relies primarily on your training knowledge with no specific literature backing (GRADE: very low certainty).
- study_rules: at least one entry; required_series_keywords lists substrings that should appear in Series Description for critical sequences (e.g. SWI, DWI).
- series_protocols: keys are typical Series Description strings; te_ms and tr_ms are echo/repetition time ranges in milliseconds (reasonable clinical ranges). Include target_duration_ms when you can estimate a typical total acquisition time benchmark.
- Use numbers only for min/max (integers or decimals as appropriate).
- Output valid JSON only."""


def _strip_code_fences(text: str) -> str:
    text = text.strip()
    m = re.match(r"^```(?:json)?\s*\n?(.*)\n?```\s*$", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text


def _extract_json_object(text: str) -> str:
    text = _strip_code_fences(text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in model output.")
    return text[start : end + 1]


def validate_rules_schema(data: Any) -> None:
    if not isinstance(data, dict):
        raise ValueError("Root must be a JSON object.")
    if "study_rules" not in data or "series_protocols" not in data:
        raise ValueError("Missing required keys: study_rules, series_protocols.")
    rationale = data.get("clinical_rationale")
    if rationale is not None:
        if not isinstance(rationale, dict):
            raise ValueError("clinical_rationale must be an object when provided.")
        for k in ("summary", "evidence_strength", "key_changes"):
            if k not in rationale:
                raise ValueError(f"clinical_rationale missing key: {k}")
            value = rationale[k]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"clinical_rationale.{k} must be a non-empty string.")
        strength = rationale["evidence_strength"].strip().lower()
        if strength not in EVIDENCE_STRENGTH_LEVELS:
            raise ValueError("clinical_rationale.evidence_strength must be High, Moderate, or Low.")
    sr = data["study_rules"]
    if not isinstance(sr, list) or len(sr) == 0:
        raise ValueError("study_rules must be a non-empty array.")
    for item in sr:
        if not isinstance(item, dict):
            raise ValueError("Each study_rules entry must be an object.")
        for k in ("id", "study_description_substring", "required_series_keywords"):
            if k not in item:
                raise ValueError(f"study_rules item missing key: {k}")
        if not isinstance(item["required_series_keywords"], list):
            raise ValueError("required_series_keywords must be an array.")
    sp = data["series_protocols"]
    if not isinstance(sp, dict) or len(sp) == 0:
        raise ValueError("series_protocols must be a non-empty object.")
    for name, spec in sp.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("series_protocols keys must be non-empty strings.")
        if not isinstance(spec, dict):
            raise ValueError(f"Invalid spec for series {name!r}.")
        for dim in ("te_ms", "tr_ms"):
            if dim not in spec:
                raise ValueError(f"series {name!r} missing {dim}.")
            rng = spec[dim]
            if not isinstance(rng, dict) or "min" not in rng or "max" not in rng:
                raise ValueError(f"series {name!r} {dim} must have min and max.")
            lo = float(rng["min"])
            hi = float(rng["max"])
            if lo > hi:
                raise ValueError(f"series {name!r} {dim}: min must be <= max.")
        td = spec.get("target_duration_ms")
        if td is not None:
            try:
                tf = float(td)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"series {name!r}: invalid target_duration_ms.") from exc
            if tf <= 0:
                raise ValueError(f"series {name!r}: target_duration_ms must be positive.")


def generate_protocol_rules(
    prompt: str,
    scanner_tesla: float | None = None,
) -> dict[str, Any]:
    """Call OpenRouter chat completions; return parsed rules dict."""
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. Set it in your environment, e.g.\n"
            '  PowerShell: $env:OPENROUTER_API_KEY = "your-key-here"\n'
            "  cmd: set OPENROUTER_API_KEY=your-key-here"
        )

    user_content = prompt.strip()
    if _is_one_point_five_t(scanner_tesla):
        user_content = f"{ONE_POINT_FIVE_T_NOTE}\n\n{user_content}"

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.2,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/local/protocol-optimiser-mvp",
        "X-Title": "Protocol Optimiser Rule Generator",
    }

    body = _post_openrouter(headers, payload, timeout=120)

    try:
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected API response shape: {body!r}") from exc

    json_str = _extract_json_object(content)
    data = json.loads(json_str)
    validate_rules_schema(data)
    return data


def write_rules_file(data: dict[str, Any], path: Path = RULES_PATH) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate rules.json from a protocol description via OpenRouter.",
    )
    parser.add_argument(
        "prompt",
        help='Protocol description, e.g. "Standard Multiple Sclerosis Brain MRI protocols"',
    )
    parser.add_argument(
        "--scanner-tesla",
        type=float,
        default=None,
        help="Scanner field strength in Tesla (e.g. 1.5 or 3.0) for hardware-aware generation.",
    )
    args = parser.parse_args()

    print(f"Model: {MODEL}")
    if args.scanner_tesla is not None:
        print(f"Scanner: {args.scanner_tesla}T")
    print(f"Writing to: {RULES_PATH}")
    print("Calling OpenRouter...")

    try:
        data = generate_protocol_rules(args.prompt, scanner_tesla=args.scanner_tesla)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    write_rules_file(data)
    print("OK: rules.json updated with valid JSON.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
