"""
Phase 7: PubMed retrieval + OpenRouter to generate rules.json (RAG).
Gold-standard journal prioritization and top-5 abstract selection.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import requests

# Imports generate_rules first so its load_dotenv() runs before we read NCBI_* from the environment.
from generate_rules import (
    OPENROUTER_API_KEY,
    OPENROUTER_URL,
    RULES_PATH,
    _extract_json_object,
    validate_rules_schema,
    write_rules_file,
)

# Optional: NCBI API key (higher rate limits) — https://www.ncbi.nlm.nih.gov/account/settings/
NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "").strip() or None
# Recommended by NCBI for eutils
NCBI_TOOL = os.environ.get("NCBI_TOOL", "protocol_optimiser_mvp")
NCBI_EMAIL = os.environ.get("NCBI_EMAIL", "").strip() or None

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# High-impact clinical radiology journals (PubMed Source strings vary; see journal_is_gold).
TOP_RADIOLOGY_JOURNALS: list[str] = [
    "Radiology",
    "European Radiology",
    "AJNR",
    "Journal of Magnetic Resonance Imaging (JMRI)",
    "RadioGraphics",
    "Investigative Radiology",
    "American Journal of Roentgenology (AJR)",
]

# Extra substrings / abbreviations commonly seen in PubMed Source / FullJournalName
_GOLD_EXTRA_NEEDLES: list[str] = [
    "eur radiol",
    "j magn reson imaging",
    "jmri",
    "ajnr am j neuroradiol",
    "radiographics",
    "invest radiol",
    "ajr am j roentgenol",
    "am j roentgenol",
]

# Match Phase 6 default; set OPENROUTER_MODEL to override (e.g. google/gemini-2.5-flash)
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.6")
ONE_POINT_FIVE_T_NOTE = (
    "Note: The current scanner is 1.5 Tesla. Please ensure the TE/TR ranges are "
    "optimized for 1.5T signal-to-noise ratios, even if the PubMed abstracts focus on 3T."
)

RAG_SYSTEM_PROMPT = """You are an expert MRI Radiologist. I will provide you with exactly five PubMed abstracts. Each block is labeled with its PMID, source journal, and whether it is a high-impact gold-standard source.

Weighting rules (critical):
- Papers from Radiology, Journal of Magnetic Resonance Imaging (JMRI), and AJNR are high-impact gold standards; give their reported sequence parameters and timing the HIGHEST weight when synthesizing rules.
- Other journals in our gold list (European Radiology, RadioGraphics, Investigative Radiology, American Journal of Roentgenology) are also strong evidence; weight them highly but below the three above when they conflict with those three.
- If a lower-tier or non-gold journal contradicts a gold-standard journal, FOLLOW THE GOLD-STANDARD source.

Act as a Senior Neuroradiology Consultant. Use your reasoning capability to explain the trade-offs between signal-to-noise and scan speed for the current hardware (1.5T vs 3T).

Based on these abstracts (and your clinical knowledge only where TE/TR numbers are omitted in the text), output the standard MRI sequences and parameters for the requested protocol. You must output ONLY valid JSON matching our exact schema, including clinical_rationale plus study_rules and series_protocols. No markdown, no conversational text.

The JSON MUST use this exact structure and key names:

{
  "clinical_rationale": {
    "summary": "exactly 3 sentences summarizing the consensus from the PubMed evidence",
    "evidence_strength": "High | Moderate | Low",
    "key_changes": "short paragraph explaining what has changed in 2024-2026 literature versus older standard protocols"
  },
  "study_rules": [
    {
      "id": "string_snake_case_identifier",
      "study_description_substring": "phrase that might appear in Study Description",
      "required_series_keywords": ["keyword1", "keyword2"]
    }
  ],
  "series_protocols": {
    "SERIES LABEL AS ON SCANNER": {
      "te_ms": { "min": <number>, "max": <number> },
      "tr_ms": { "min": <number>, "max": <number> },
      "target_duration_ms": <optional total acquisition time benchmark in ms>
    }
  }
}

clinical_rationale must be present and should read like a concise executive brief. study_rules must be a non-empty array. series_protocols must be a non-empty object. Use numeric min/max in milliseconds for te_ms and tr_ms. Include target_duration_ms when literature suggests a typical total scan duration benchmark. Output valid JSON only."""


def journal_is_gold(journal: str) -> bool:
    """Return True if the journal string matches our gold list (substring / alias aware)."""
    if not journal or not journal.strip():
        return False
    j = journal.strip().lower()
    for label in TOP_RADIOLOGY_JOURNALS:
        if label.strip().lower() in j:
            return True
    for needle in _GOLD_EXTRA_NEEDLES:
        if needle in j:
            return True
    return False


def _is_one_point_five_t(scanner_tesla: float | None) -> bool:
    return isinstance(scanner_tesla, (int, float)) and 1.35 <= float(scanner_tesla) <= 1.65


def _ncbi_params(extra: dict[str, Any]) -> dict[str, Any]:
    p = dict(extra)
    p["tool"] = NCBI_TOOL
    if NCBI_EMAIL:
        p["email"] = NCBI_EMAIL
    if NCBI_API_KEY:
        p["api_key"] = NCBI_API_KEY
    return p


def get_pubmed_ids(query: str, api_key: str | None = None) -> list[str]:
    """
    Search PubMed (esearch); return up to 20 PMIDs (retmax=20).
    api_key overrides NCBI_API_KEY env for this call only.
    """
    key = (api_key or NCBI_API_KEY or "").strip() or None
    params = _ncbi_params(
        {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": 20,
        }
    )
    if key:
        params["api_key"] = key

    try:
        resp = requests.get(ESEARCH_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"PubMed esearch failed: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"PubMed esearch returned invalid JSON: {exc}") from exc

    try:
        idlist = data["esearchresult"]["idlist"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected esearch response shape: {data!r}") from exc

    return [str(x) for x in idlist]


def _parse_sort_date(sortpubdate: str) -> float:
    """Parse PubMed esummary sortpubdate / pubdate to a sortable float (higher = more recent)."""
    if not sortpubdate or not str(sortpubdate).strip():
        return 0.0
    s = str(sortpubdate).strip()
    for fmt, n in (
        ("%Y/%m/%d %H:%M", 16),
        ("%Y/%m/%d", 10),
        ("%Y %b %d", 11),
    ):
        try:
            return datetime.strptime(s[:n], fmt).timestamp()
        except ValueError:
            continue
    m = re.match(r"^(\d{4})", s)
    if m:
        return float(m.group(1)) * 1e6
    return 0.0


def _extract_pub_year(value: str) -> int | None:
    m = re.search(r"(19|20)\d{2}", value or "")
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def fetch_pubmed_summaries(pmid_list: list[str], api_key: str | None = None) -> dict[str, dict[str, Any]]:
    """
    ESummary (lightweight): map PMID -> journal display name, sort date, high_impact flag.
    """
    out: dict[str, dict[str, Any]] = {}
    if not pmid_list:
        return out

    key = (api_key or NCBI_API_KEY or "").strip() or None
    params = _ncbi_params(
        {
            "db": "pubmed",
            "id": ",".join(pmid_list),
            "retmode": "json",
        }
    )
    if key:
        params["api_key"] = key

    try:
        resp = requests.get(ESUMMARY_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"PubMed esummary failed: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"PubMed esummary returned invalid JSON: {exc}") from exc

    try:
        result = data["result"]
        uids = result.get("uids") or []
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected esummary response shape: {data!r}") from exc

    for uid in uids:
        article = result.get(uid)
        if not isinstance(article, dict):
            continue
        journal = article.get("fulljournalname") or article.get("source") or "Unknown"
        if isinstance(journal, list):
            journal = journal[0] if journal else "Unknown"
        journal = str(journal).strip() or "Unknown"
        sort_raw = article.get("sortpubdate") or article.get("epubdate") or article.get("pubdate") or ""
        if isinstance(sort_raw, list):
            sort_raw = sort_raw[0] if sort_raw else ""
        sort_raw = str(sort_raw)
        sort_ts = _parse_sort_date(sort_raw)
        year = _extract_pub_year(sort_raw)
        out[str(uid)] = {
            "journal": journal,
            "sort_ts": sort_ts,
            "year": year,
            "high_impact": journal_is_gold(journal),
        }

    return out


def select_top_five_pmids(
    pmids_ordered: list[str],
    summary_by_pmid: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Gold journals first, then most recent (sort_ts descending). Return top 5 rows with metadata.
    """
    rows: list[dict[str, Any]] = []
    for pmid in pmids_ordered:
        meta = summary_by_pmid.get(pmid)
        if not meta:
            rows.append(
                {
                    "pmid": pmid,
                    "journal": "Unknown",
                    "sort_ts": 0.0,
                    "year": None,
                    "high_impact": False,
                }
            )
            continue
        rows.append(
            {
                "pmid": pmid,
                "journal": meta["journal"],
                "sort_ts": float(meta["sort_ts"]),
                "year": meta.get("year"),
                "high_impact": bool(meta["high_impact"]),
            }
        )

    # Gold journals first; within each tier, most recent first (higher sort_ts first).
    rows.sort(key=lambda r: (0 if r["high_impact"] else 1, -r["sort_ts"]))
    return rows[:5]


def _abstract_text_from_element(elem: ET.Element) -> str:
    return "".join(elem.itertext()).strip()


def _pmid_from_article(article: ET.Element) -> str | None:
    for el in article.iter():
        if el.tag.endswith("MedlineCitation"):
            for child in el.iter():
                if child.tag.endswith("PMID") and child.text:
                    return child.text.strip()
    return None


def _abstract_from_article(article: ET.Element) -> str:
    parts: list[str] = []
    for el in article.iter():
        if el.tag.endswith("Abstract"):
            for child in el:
                if child.tag.endswith("AbstractText"):
                    t = _abstract_text_from_element(child)
                    if t:
                        parts.append(t)
            break
    return "\n\n".join(parts)


def fetch_pubmed_abstracts_by_pmid(pmid_list: list[str], api_key: str | None = None) -> dict[str, str]:
    """
    EFetch XML: return mapping PMID -> abstract text (per PubmedArticle).
    """
    if not pmid_list:
        return {}

    key = (api_key or NCBI_API_KEY or "").strip() or None
    params = _ncbi_params(
        {
            "db": "pubmed",
            "id": ",".join(pmid_list),
            "retmode": "xml",
        }
    )
    if key:
        params["api_key"] = key

    try:
        resp = requests.get(EFETCH_URL, params=params, timeout=120)
        resp.raise_for_status()
        xml_content = resp.text
    except requests.RequestException as exc:
        raise RuntimeError(f"PubMed efetch failed: {exc}") from exc

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        raise RuntimeError(f"PubMed efetch returned invalid XML: {exc}") from exc

    by_pmid: dict[str, str] = {}
    for article in root.iter():
        if not article.tag.endswith("PubmedArticle"):
            continue
        pmid = _pmid_from_article(article)
        if not pmid:
            continue
        by_pmid[pmid] = _abstract_from_article(article)

    return by_pmid


def _build_user_rag_content(
    protocol_name: str,
    ranked_rows: list[dict[str, Any]],
    abstract_by_pmid: dict[str, str],
    scanner_tesla: float | None = None,
) -> str:
    blocks: list[str] = []
    for i, row in enumerate(ranked_rows, start=1):
        pmid = row["pmid"]
        journal = row["journal"]
        gold = row["high_impact"]
        abstract = abstract_by_pmid.get(pmid, "").strip() or "(No abstract text in record.)"
        tier = "Gold-standard journal (high impact)" if gold else "Supporting journal"
        blocks.append(
            f"### Paper {i}\n"
            f"PMID: {pmid}\n"
            f"Journal: {journal}\n"
            f"High-impact (gold list): {'yes' if gold else 'no'} — {tier}\n\n"
            f"Abstract:\n{abstract}"
        )
    scanner_note = ""
    if _is_one_point_five_t(scanner_tesla):
        scanner_note = f"{ONE_POINT_FIVE_T_NOTE}\n\n"

    return (
        f"Requested protocol: {protocol_name}\n\n"
        f"{scanner_note}"
        f"I am providing you with {len(ranked_rows)} abstracts. Each is labeled with its source journal. "
        f"Papers from Radiology, JMRI, and AJNR are high-impact gold standards; give their parameters the "
        f"highest weight. If a lower-tier journal contradicts a gold-standard journal, follow the gold-standard.\n\n"
        + "\n\n---\n\n".join(blocks)
    )


def generate_rules_from_pubmed(
    protocol_name: str,
    api_key: str | None = None,
    scanner_tesla: float | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Search PubMed, rank by gold journal + recency, fetch top 5 abstracts, call OpenRouter.
    Returns (validated rules dict, list of source dicts: pmid, journal, high_impact).
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. Set it in your environment before running this script."
        )

    query = f"{protocol_name} MRI guidelines parameters"
    print(f"PubMed query: {query!r}")

    try:
        pmids = get_pubmed_ids(query, api_key=api_key)
    except RuntimeError:
        raise

    print(f"Found PMIDs ({len(pmids)}): {pmids[:8]}{'...' if len(pmids) > 8 else ''}")

    if not pmids:
        raise RuntimeError("No PubMed articles returned for this query. Try a different protocol name.")

    try:
        summary_by_pmid = fetch_pubmed_summaries(pmids, api_key=api_key)
    except RuntimeError:
        raise

    ranked = select_top_five_pmids(pmids, summary_by_pmid)
    top_pmids = [r["pmid"] for r in ranked]
    print(f"Selected top {len(ranked)} PMIDs after gold + recency ranking: {top_pmids}")

    try:
        abstract_by_pmid = fetch_pubmed_abstracts_by_pmid(top_pmids, api_key=api_key)
    except RuntimeError:
        raise

    user_content = _build_user_rag_content(
        protocol_name,
        ranked,
        abstract_by_pmid,
        scanner_tesla=scanner_tesla,
    )

    if not any(abstract_by_pmid.get(p, "").strip() for p in top_pmids):
        print("Warning: no AbstractText extracted from efetch; the model will rely on protocol name and labels.")

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": RAG_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/local/protocol-optimiser-mvp",
        "X-Title": "Protocol Optimiser PubMed RAG",
    }

    print(f"OpenRouter model: {OPENROUTER_MODEL}")
    print("Calling OpenRouter...")

    try:
        resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
        resp.raise_for_status()
        body = resp.json()
    except requests.HTTPError as exc:
        detail = ""
        try:
            detail = resp.text[:500]
        except Exception:
            pass
        raise RuntimeError(f"OpenRouter HTTP error: {exc}. Body (truncated): {detail}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"OpenRouter request failed: {exc}") from exc

    try:
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected OpenRouter response: {body!r}") from exc

    json_str = _extract_json_object(content)
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Model returned invalid JSON: {exc}\nRaw (truncated): {content[:800]}") from exc

    validate_rules_schema(data)

    sources_out = [
        {
            "pmid": r["pmid"],
            "journal": r["journal"],
            "year": r.get("year"),
            "high_impact": r["high_impact"],
        }
        for r in ranked
    ]
    return data, sources_out


def generate_rules_from_literature(
    protocol_name: str,
    api_key: str | None = None,
    scanner_tesla: float | None = None,
) -> dict[str, Any]:
    """Search PubMed, fetch abstracts, call OpenRouter; return validated rules dict only."""
    data, _sources = generate_rules_from_pubmed(
        protocol_name,
        api_key=api_key,
        scanner_tesla=scanner_tesla,
    )
    return data


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate rules.json from PubMed abstracts + OpenRouter.",
    )
    parser.add_argument(
        "protocol_name",
        help='Protocol focus, e.g. "Multiple Sclerosis Brain"',
    )
    parser.add_argument(
        "--scanner-tesla",
        type=float,
        default=None,
        help="Optional scanner field strength in Tesla (e.g. 1.5 or 3.0).",
    )
    args = parser.parse_args()

    print(f"Writing to: {RULES_PATH}")

    try:
        data, sources = generate_rules_from_pubmed(
            args.protocol_name.strip(),
            scanner_tesla=args.scanner_tesla,
        )
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    for s in sources:
        star = "*" if s["high_impact"] else " "
        print(f" [{star}] PMID {s['pmid']} — {s['journal']}")

    try:
        write_rules_file(data)
    except OSError as exc:
        print(f"Error writing rules.json: {exc}", file=sys.stderr)
        return 1

    print("OK: rules.json updated with valid JSON.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
