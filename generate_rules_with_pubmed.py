"""
Phase 7: PubMed retrieval + OpenRouter to generate rules.json (RAG).
Gold-standard journal prioritization and top-5 abstract selection.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from pypdf import PdfReader

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

RAG_SYSTEM_PROMPT = """You are an expert MRI Radiologist. I will provide you with up to five PubMed abstracts, and optionally a user-provided reference document (e.g. society guidelines, site protocol). Each PubMed block is labeled with its PMID, source journal, and whether it is a high-impact gold-standard source.

Weighting rules (critical):
- Papers from Radiology, Journal of Magnetic Resonance Imaging (JMRI), and AJNR are high-impact gold standards; give their reported sequence parameters and timing the HIGHEST weight when synthesizing rules.
- Other journals in our gold list (European Radiology, RadioGraphics, Investigative Radiology, American Journal of Roentgenology) are also strong evidence; weight them highly but below the three above when they conflict with those three.
- If a lower-tier or non-gold journal contradicts a gold-standard journal, FOLLOW THE GOLD-STANDARD source.
- When a user-provided reference document is included and contains explicit TE/TR or sequence parameters, prefer those numbers for site-specific benchmarks; use PubMed gold-standard sources for general consensus when both are parameter-rich.

Act as a Senior Neuroradiology Consultant. Use your reasoning capability to explain the trade-offs between signal-to-noise and scan speed for the current hardware (1.5T vs 3T).

Based on these sources (and your clinical knowledge only where TE/TR numbers are omitted in the text), output the standard MRI sequences and parameters for the requested protocol. You must output ONLY valid JSON matching our exact schema, including clinical_rationale plus study_rules and series_protocols. No markdown, no conversational text.

The JSON MUST use this exact structure and key names:

{
  "clinical_rationale": {
    "summary": "2-4 sentences summarizing the consensus from the PubMed evidence",
    "evidence_strength": "High | Moderate | Low",
    "key_changes": "short paragraph explaining what has changed in recent literature versus older standard protocols, or state that no significant changes were identified if the evidence is stable"
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

clinical_rationale must be present and should read like a concise executive brief.
evidence_strength grading rubric (you MUST follow this, aligned with GRADE certainty methodology):
- "High": user-provided reference document has parameter_density >= 3, OR at least 2 abstracts with parameter_density >= 3 from gold-standard journals (GRADE: high certainty -- consistent, parameter-rich evidence from authoritative sources).
- "Moderate": gold-standard journal abstracts present but parameter_density < 3, OR only non-gold journals provide parameter-dense data, OR user document present with parameter_density < 3 (GRADE: moderate certainty -- evidence available but limited in source authority or specificity).
- "Low": no abstracts have parameter_density >= 2 AND no user document provided, and the output relies primarily on your training knowledge (GRADE: very low certainty -- indirect evidence only).
study_rules must be a non-empty array. series_protocols must be a non-empty object. Use numeric min/max in milliseconds for te_ms and tr_ms. Include target_duration_ms when literature suggests a typical total scan duration benchmark. Output valid JSON only."""


_GOLD_JOURNAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(rf"^\s*{re.escape(label.strip())}\b", re.IGNORECASE)
    for label in TOP_RADIOLOGY_JOURNALS
]


def journal_is_gold(journal: str) -> bool:
    """Return True if the journal string matches our gold list (word-boundary aware)."""
    if not journal or not journal.strip():
        return False
    j = journal.strip().lower()
    for pat in _GOLD_JOURNAL_PATTERNS:
        if pat.search(j):
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


def _multi_strategy_search(protocol_name: str, api_key: str | None = None) -> list[str]:
    """
    Systematic-review-style search: three complementary PubMed queries merged and deduplicated.
    Returns a single PMID list (order preserved, duplicates removed).
    """
    _current_year = datetime.now().year
    _date_filter = f"2018:{_current_year}[dp]"
    queries = [
        (
            f'({protocol_name}) AND ("guidelines" OR "consensus" OR "recommendations") '
            f'AND "Magnetic Resonance Imaging"[MeSH] AND {_date_filter}'
        ),
        (
            f'({protocol_name}) AND ("Magnetic Resonance Imaging/methods"[MeSH]) '
            f'AND ("repetition time" OR "echo time" OR "sequence parameters" OR "flip angle") '
            f"AND {_date_filter}"
        ),
        (
            f'({protocol_name}) AND ("protocol optimization" OR "accelerated" '
            f'OR "acquisition time" OR "scan time") AND MRI AND {_date_filter}'
        ),
    ]
    seen: set[str] = set()
    merged: list[str] = []
    for i, q in enumerate(queries, start=1):
        label = ["Clinical guidelines", "Technical parameters", "Protocol optimization"][i - 1]
        print(f"  Strategy {i} ({label}): {q[:120]}...")
        try:
            ids = get_pubmed_ids(q, api_key=api_key)
        except RuntimeError:
            ids = []
        print(f"    -> {len(ids)} PMIDs")
        for pmid in ids:
            if pmid not in seen:
                seen.add(pmid)
                merged.append(pmid)
    return merged


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


_PARAM_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bTE\s*[=:≈~]?\s*\d",
        r"\bTR\s*[=:≈~]?\s*\d",
        r"\bTI\s*[=:≈~]?\s*\d",
        r"\b(?:TE|TR|TI|TE/TR)\b.*?\d+\s*ms",
        r"\b(?:slice|thickness|voxel|resolution|FOV)\b.*?\d+\.?\d*\s*mm",
        r"\bflip\s*angle\s*[=:≈~]?\s*\d",
        r"\bFOV\s*[=:≈~]\s*\d",
        r"\bmatrix\s*[=:≈~]\s*\d",
        r"\bslice\s+thickness\s*[=:≈~]?\s*\d",
        r"\bb[- ]?value\s*[=:≈~]?\s*\d",
        r"\bvoxel\s+size\s*[=:≈~]?\s*\d",
        r"\bNEX\s*[=:≈~]?\s*\d",
        r"\bNSA\s*[=:≈~]?\s*\d",
        r"\bbandwidth\s*[=:≈~]?\s*\d",
    ]
]


def _parameter_density(abstract_text: str) -> int:
    """Count distinct MRI parameter pattern hits in abstract text."""
    if not abstract_text:
        return 0
    return sum(1 for pat in _PARAM_PATTERNS if pat.search(abstract_text))


def _extract_text_from_pdf_bytes(data: bytes) -> str:
    """Extract text from an in-memory PDF. Returns empty string on failure."""
    try:
        reader = PdfReader(io.BytesIO(data))
        parts = [page.extract_text() or "" for page in reader.pages]
        return "\n".join(parts).strip()
    except Exception:
        return ""


_SUPPLEMENT_CHUNK_SIZE = 5000
_SUPPLEMENT_CHUNK_OVERLAP = 500


def _prepare_supplementary_text(
    raw: str,
    protocol_name: str,
    max_chars: int = 24_000,
) -> str:
    """Truncate or rank-select the most parameter-dense chunks of user-supplied text."""
    text = raw.strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text

    proto_tokens = {t.lower() for t in protocol_name.split() if len(t) > 2}
    step = _SUPPLEMENT_CHUNK_SIZE - _SUPPLEMENT_CHUNK_OVERLAP
    chunks: list[tuple[float, int, str]] = []
    for idx, start in enumerate(range(0, len(text), step)):
        chunk = text[start : start + _SUPPLEMENT_CHUNK_SIZE]
        density = _parameter_density(chunk)
        keyword_bonus = sum(1 for tok in proto_tokens if tok in chunk.lower())
        chunks.append((-density - keyword_bonus * 0.5, idx, chunk))

    chunks.sort()
    selected: list[str] = []
    total = 0
    for _score, _idx, chunk in chunks:
        if total + len(chunk) > max_chars:
            break
        selected.append(chunk)
        total += len(chunk)

    return "\n\n".join(selected)


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

        pmcid = None
        for aid in article.get("articleids", []):
            if isinstance(aid, dict) and aid.get("idtype") == "pmc" and aid.get("value"):
                pmcid = str(aid["value"]).strip()
                break

        out[str(uid)] = {
            "journal": journal,
            "sort_ts": sort_ts,
            "year": year,
            "high_impact": journal_is_gold(journal),
            "pmcid": pmcid,
        }

    return out


def select_top_five_pmids(
    pmids_ordered: list[str],
    summary_by_pmid: dict[str, dict[str, Any]],
    density_by_pmid: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """
    Composite ranking: gold journal > recency > parameter density > date.
    Return top 5 rows with metadata including param_density.
    """
    _density = density_by_pmid or {}
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
                    "param_density": _density.get(pmid, 0),
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
                "param_density": _density.get(pmid, 0),
            }
        )

    _RECENCY_FLOOR = 2018
    rows.sort(key=lambda r: (
        0 if r["high_impact"] else 1,
        0 if isinstance(r.get("year"), int) and r["year"] >= _RECENCY_FLOOR else 1,
        -r["param_density"],
        -r["sort_ts"],
    ))
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


def _fetch_abstracts_chunk(pmid_list: list[str], api_key: str | None = None) -> dict[str, str]:
    """EFetch XML for a single chunk of PMIDs -> mapping PMID -> abstract text."""
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


_EFETCH_CHUNK_SIZE = 20


def fetch_pubmed_abstracts_by_pmid(pmid_list: list[str], api_key: str | None = None) -> dict[str, str]:
    """
    EFetch XML: return mapping PMID -> abstract text.
    Chunks requests into groups of 20 to respect NCBI rate limits.
    """
    if not pmid_list:
        return {}
    merged: dict[str, str] = {}
    for i in range(0, len(pmid_list), _EFETCH_CHUNK_SIZE):
        chunk = pmid_list[i : i + _EFETCH_CHUNK_SIZE]
        merged.update(_fetch_abstracts_chunk(chunk, api_key=api_key))
    return merged


_PMC_CHUNK_SIZE = 10


def _fetch_pmc_methods(pmcid_list: list[str], api_key: str | None = None) -> dict[str, str]:
    """
    Fetch Methods section text from PMC Open Access XML.
    Returns mapping PMCID -> extracted methods text (may be empty if not OA or no methods section).
    """
    if not pmcid_list:
        return {}

    key = (api_key or NCBI_API_KEY or "").strip() or None
    out: dict[str, str] = {}

    for i in range(0, len(pmcid_list), _PMC_CHUNK_SIZE):
        chunk = pmcid_list[i : i + _PMC_CHUNK_SIZE]
        params = _ncbi_params(
            {
                "db": "pmc",
                "id": ",".join(chunk),
                "retmode": "xml",
            }
        )
        if key:
            params["api_key"] = key

        try:
            resp = requests.get(EFETCH_URL, params=params, timeout=120)
            resp.raise_for_status()
            xml_content = resp.text
        except requests.RequestException:
            continue

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            continue

        for article_el in root.iter("article"):
            pmc_tag = article_el.find(".//article-id[@pub-id-type='pmc']")
            pmc_val = None
            if pmc_tag is not None and pmc_tag.text:
                pmc_val = pmc_tag.text.strip()
                if not pmc_val.upper().startswith("PMC"):
                    pmc_val = f"PMC{pmc_val}"

            methods_parts: list[str] = []
            for sec in article_el.iter("sec"):
                sec_type = (sec.get("sec-type") or "").lower()
                title_el = sec.find("title")
                title_text = (title_el.text or "").lower() if title_el is not None else ""
                if "method" in sec_type or "material" in sec_type or "method" in title_text or "material" in title_text:
                    methods_parts.append("".join(sec.itertext()).strip())

            if pmc_val and methods_parts:
                out[pmc_val.upper()] = "\n\n".join(methods_parts)

    return out


def _build_user_rag_content(
    protocol_name: str,
    ranked_rows: list[dict[str, Any]],
    abstract_by_pmid: dict[str, str],
    scanner_tesla: float | None = None,
    pmc_enriched_pmids: set[str] | None = None,
    supplementary_text: str | None = None,
    supplementary_filename: str | None = None,
) -> str:
    _pmc = pmc_enriched_pmids or set()
    blocks: list[str] = []
    for i, row in enumerate(ranked_rows, start=1):
        pmid = row["pmid"]
        journal = row["journal"]
        gold = row["high_impact"]
        density = row.get("param_density", 0)
        abstract = abstract_by_pmid.get(pmid, "").strip() or "(No abstract text in record.)"
        tier = "Gold-standard journal (high impact)" if gold else "Supporting journal"
        source_label = "abstract+methods" if pmid in _pmc else "abstract"
        blocks.append(
            f"### Paper {i}\n"
            f"PMID: {pmid}\n"
            f"Journal: {journal}\n"
            f"High-impact (gold list): {'yes' if gold else 'no'} — {tier}\n"
            f"Parameter density: {density} technical terms detected\n"
            f"Source depth: {source_label}\n\n"
            f"Abstract:\n{abstract}"
        )
    scanner_note = ""
    if _is_one_point_five_t(scanner_tesla):
        scanner_note = f"{ONE_POINT_FIVE_T_NOTE}\n\n"

    supplement_block = ""
    if supplementary_text and supplementary_text.strip():
        fname = supplementary_filename or "user_document"
        sup_density = _parameter_density(supplementary_text)
        supplement_block = (
            f"### User-provided reference document\n"
            f"Filename: {fname}\n"
            f"Parameter density: {sup_density} technical terms detected\n"
            f"Instructions: Prefer explicit TE/TR and timing from this block for "
            f"site-specific benchmarks when they conflict with sparse abstracts.\n\n"
            f"{supplementary_text.strip()}\n\n---\n\n"
        )

    return (
        f"Requested protocol: {protocol_name}\n\n"
        f"{scanner_note}"
        f"{supplement_block}"
        f"I am providing you with {len(ranked_rows)} abstracts. "
        f"Apply the journal weighting rules from your instructions.\n\n"
        + "\n\n---\n\n".join(blocks)
    )


def generate_rules_from_pubmed(
    protocol_name: str,
    api_key: str | None = None,
    scanner_tesla: float | None = None,
    supplementary_text: str | None = None,
    supplementary_filename: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Systematic multi-strategy PubMed search, parameter-density scoring, composite ranking,
    then OpenRouter synthesis. Optional user-supplied reference text is merged into the LLM prompt.
    Returns (validated rules dict, list of source dicts with density metadata).
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. Set it in your environment before running this script."
        )

    # 1. Multi-strategy search
    print("Running multi-strategy PubMed search...")
    pmids = _multi_strategy_search(protocol_name, api_key=api_key)
    print(f"Deduplicated pool: {len(pmids)} PMIDs")

    if not pmids:
        raise RuntimeError("No PubMed articles returned for any search strategy. Try a different protocol name.")

    # 2. Summaries (journal + year metadata)
    summary_by_pmid = fetch_pubmed_summaries(pmids, api_key=api_key)

    # 3. Fetch ALL abstracts (chunked for large pools)
    print(f"Fetching abstracts for {len(pmids)} papers...")
    abstract_by_pmid = fetch_pubmed_abstracts_by_pmid(pmids, api_key=api_key)

    # 3b. PMC Open Access full-text enrichment (Methods sections)
    pmcid_to_pmid: dict[str, str] = {}
    for pmid in pmids:
        meta = summary_by_pmid.get(pmid, {})
        pmcid = meta.get("pmcid")
        if pmcid:
            pmcid_to_pmid[pmcid.upper()] = pmid
    pmc_source_pmids: set[str] = set()
    if pmcid_to_pmid:
        print(f"Fetching PMC Methods sections for {len(pmcid_to_pmid)} papers with PMCIDs...")
        pmc_methods = _fetch_pmc_methods(list(pmcid_to_pmid.keys()), api_key=api_key)
        for pmcid_key, methods_text in pmc_methods.items():
            pmid = pmcid_to_pmid.get(pmcid_key)
            if pmid and methods_text:
                existing = abstract_by_pmid.get(pmid, "")
                abstract_by_pmid[pmid] = (existing + "\n\n[Methods section from PMC full-text]\n" + methods_text).strip()
                pmc_source_pmids.add(pmid)
        print(f"  Enriched {len(pmc_source_pmids)} paper(s) with Methods text.")
    else:
        print("No PMCIDs found in summaries; skipping full-text enrichment.")

    # 4. Parameter-density scoring (now includes Methods text where available)
    density_by_pmid = {pmid: _parameter_density(abstract_by_pmid.get(pmid, "")) for pmid in pmids}
    dense_count = sum(1 for d in density_by_pmid.values() if d >= 2)
    print(f"Parameter density: {dense_count}/{len(pmids)} papers have density >= 2")

    # 5. Composite ranking (gold, recency, density, date)
    ranked = select_top_five_pmids(pmids, summary_by_pmid, density_by_pmid)
    top_pmids = [r["pmid"] for r in ranked]
    print(f"Top {len(ranked)} after composite ranking: {top_pmids}")
    for r in ranked:
        star = "*" if r["high_impact"] else " "
        print(f"  [{star}] PMID {r['pmid']} density={r['param_density']} — {r['journal']}")

    # 6. Filter papers with empty abstracts
    ranked = [r for r in ranked if abstract_by_pmid.get(r["pmid"], "").strip()]
    if not ranked:
        print("Warning: no usable abstracts survived filtering; the model will rely on training knowledge.")
    else:
        print(f"After abstract filter: {len(ranked)} paper(s) with usable abstracts.")

    prepared_supplement = ""
    if supplementary_text and supplementary_text.strip():
        prepared_supplement = _prepare_supplementary_text(supplementary_text, protocol_name)
        sup_density = _parameter_density(prepared_supplement)
        print(f"User reference document: {len(prepared_supplement)} chars, param density={sup_density}")

    user_content = _build_user_rag_content(
        protocol_name,
        ranked,
        abstract_by_pmid,
        scanner_tesla=scanner_tesla,
        pmc_enriched_pmids=pmc_source_pmids,
        supplementary_text=prepared_supplement,
        supplementary_filename=supplementary_filename,
    )

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

    sources_out: list[dict[str, Any]] = []
    if prepared_supplement:
        sources_out.append({
            "source": "user_document",
            "pmid": None,
            "journal": supplementary_filename or "User document",
            "year": None,
            "high_impact": False,
            "param_density": _parameter_density(prepared_supplement),
        })
    sources_out.extend(
        {
            "pmid": r["pmid"],
            "journal": r["journal"],
            "year": r.get("year"),
            "high_impact": r["high_impact"],
            "param_density": r.get("param_density", 0),
        }
        for r in ranked
    )
    return data, sources_out


def generate_rules_from_literature(
    protocol_name: str,
    api_key: str | None = None,
    scanner_tesla: float | None = None,
    supplementary_text: str | None = None,
    supplementary_filename: str | None = None,
) -> dict[str, Any]:
    """Search PubMed, fetch abstracts, call OpenRouter; return validated rules dict only."""
    data, _sources = generate_rules_from_pubmed(
        protocol_name,
        api_key=api_key,
        scanner_tesla=scanner_tesla,
        supplementary_text=supplementary_text,
        supplementary_filename=supplementary_filename,
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
    parser.add_argument(
        "--supplement-file",
        default=None,
        help="Optional path to a PDF or TXT reference document to include in synthesis.",
    )
    args = parser.parse_args()

    supplement_text: str | None = None
    supplement_name: str | None = None
    if args.supplement_file:
        sup_path = Path(args.supplement_file)
        if not sup_path.is_file():
            print(f"Error: supplement file not found: {sup_path}", file=sys.stderr)
            return 1
        raw_bytes = sup_path.read_bytes()
        supplement_name = sup_path.name
        if sup_path.suffix.lower() == ".pdf":
            supplement_text = _extract_text_from_pdf_bytes(raw_bytes)
            if not supplement_text:
                print("Warning: no text extracted from PDF (may be scanned/image-only).", file=sys.stderr)
        else:
            supplement_text = raw_bytes.decode("utf-8", errors="replace")
        if supplement_text:
            print(f"Loaded supplement: {supplement_name} ({len(supplement_text)} chars)")

    print(f"Writing to: {RULES_PATH}")

    try:
        data, sources = generate_rules_from_pubmed(
            args.protocol_name.strip(),
            scanner_tesla=args.scanner_tesla,
            supplementary_text=supplement_text,
            supplementary_filename=supplement_name,
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
