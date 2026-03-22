"""
Streamlit audit dashboard — physics drift, efficiency, clinical grade.
Shows sequence physics and timing only (no patient identifiers).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import streamlit as st

from dicom_parser import parse_dicom
from generate_rules import write_rules_file
from generate_rules_with_pubmed import generate_rules_from_pubmed
from rule_engine import evaluate, load_rules

CONTACT_EMAIL = "arash.atashnama@gmail.com"
ONE_POINT_FIVE_T_NOTE = (
    "Note: The current scanner is 1.5 Tesla. Please ensure the TE/TR ranges are "
    "optimized for 1.5T signal-to-noise ratios, even if the PubMed abstracts focus on 3T."
)

PRIVACY_NOTICE = (
    "Your privacy is our priority. Uploaded files are processed in-memory for analysis and are never "
    "permanently stored on our servers. Once the analysis is complete, the temporary data is purged."
)


def _evidence_badge_html(level: str | None) -> str:
    palette = {
        "high": ("#e8f5e9", "#1b5e20"),
        "moderate": ("#fff8e1", "#8d6e00"),
        "low": ("#ffebee", "#b71c1c"),
    }
    normalized = (level or "").strip().lower()
    bg, fg = palette.get(normalized, ("#eceff1", "#37474f"))
    label = (level or "Unavailable").strip() or "Unavailable"
    return (
        f"<span style='display:inline-block;padding:0.35rem 0.75rem;border-radius:999px;"
        f"background:{bg};color:{fg};font-weight:700'>{label}</span>"
    )


def _build_source_rows(sources: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not isinstance(sources, list):
        return rows
    for src in sources:
        if not isinstance(src, dict):
            continue
        year = src.get("year")
        year_label = str(year) if year is not None else "-"
        rows.append(
            {
                "Year": year_label,
                "Journal": str(src.get("journal") or "Unknown"),
                "Impact Factor ⭐": "⭐" if src.get("high_impact") else "-",
                "PMID": str(src.get("pmid") or "-"),
            }
        )
    return rows


def main() -> None:
    st.set_page_config(
        page_title="Protocol Optimiser",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if "rules_version" not in st.session_state:
        st.session_state.rules_version = 0
    if "last_pmids" not in st.session_state:
        st.session_state.last_pmids = None
    if "last_sync_sources" not in st.session_state:
        st.session_state.last_sync_sources = None
    if "last_sync_error" not in st.session_state:
        st.session_state.last_sync_error = None
    if "last_magnetic_field_strength_t" not in st.session_state:
        st.session_state.last_magnetic_field_strength_t = None
    if "guideline_target_scanner" not in st.session_state:
        st.session_state.guideline_target_scanner = "Not specified"

    # --- Sidebar ---
    with st.sidebar:
        st.header("Guideline Management")
        st.caption("Pull recent PubMed abstracts and regenerate `rules.json` via OpenRouter.")

        protocol_query = st.text_input(
            "Update rules for...",
            value="Multiple Sclerosis",
            help="Used as the clinical topic for PubMed search and AI generation.",
        )

        last_strength = st.session_state.last_magnetic_field_strength_t
        if st.session_state.guideline_target_scanner == "Not specified":
            if isinstance(last_strength, (int, float)) and 1.35 <= float(last_strength) <= 1.65:
                st.session_state.guideline_target_scanner = "1.5 T"
            elif isinstance(last_strength, (int, float)) and 2.8 <= float(last_strength) <= 3.2:
                st.session_state.guideline_target_scanner = "3.0 T"

        scanner_choice = st.selectbox(
            "Guideline target scanner (for PubMed sync)",
            options=["Not specified", "1.5 T", "3.0 T"],
            key="guideline_target_scanner",
            help="Used only for guideline generation. Choose 1.5 T to bias TE/TR ranges toward 1.5T SNR needs.",
        )
        scanner_tesla = {"Not specified": None, "1.5 T": 1.5, "3.0 T": 3.0}[scanner_choice]

        if scanner_tesla == 1.5:
            st.info(ONE_POINT_FIVE_T_NOTE)

        if st.button("Sync with PubMed", type="primary", use_container_width=True):
            st.session_state.last_sync_error = None
            try:
                with st.spinner("Searching PubMed and updating rules..."):
                    data, sources = generate_rules_from_pubmed(
                        protocol_query.strip(),
                        scanner_tesla=scanner_tesla,
                    )
                    write_rules_file(data)
                    st.session_state.last_sync_sources = sources
                    st.session_state.last_pmids = [s["pmid"] for s in sources]
                    st.session_state.rules_version = st.session_state.rules_version + 1
            except Exception as exc:
                st.session_state.last_sync_error = str(exc)

        if st.session_state.last_sync_error:
            st.error(st.session_state.last_sync_error)
        elif st.session_state.last_pmids is not None and st.session_state.last_sync_error is None:
            st.success("Guidelines synced. `rules.json` updated.")
            st.caption("Synced sources (top 5 after journal ranking)")
            sources = st.session_state.last_sync_sources
            if sources and isinstance(sources[0], dict):
                for src in sources:
                    star = "⭐ " if src.get("high_impact") else ""
                    j = src.get("journal") or "Unknown"
                    pid = src.get("pmid", "")
                    year = src.get("year")
                    year_prefix = f"{year} · " if year else ""
                    label = f"{star}**{year_prefix}{j}** — [{pid}](https://pubmed.ncbi.nlm.nih.gov/{pid}/)"
                    st.markdown(f"- {label}")
            else:
                for pmid in st.session_state.last_pmids or []:
                    st.markdown(f"- [{pmid}](https://pubmed.ncbi.nlm.nih.gov/{pmid}/)")

        st.divider()
        st.caption(
            f"Active rules snapshot: **v{st.session_state.rules_version}** "
            "(each upload loads `rules.json` from disk)."
        )

        st.divider()
        st.subheader("Contact & Support")
        st.markdown(
            f"Questions or feedback?  \n[📧 {CONTACT_EMAIL}](mailto:{CONTACT_EMAIL})",
        )

    # --- Main ---
    st.title("Protocol Optimiser")
    st.markdown(
        '<p style="font-size:1.05rem;color:#5a5a5a;margin-bottom:1.25rem;">'
        "Clinical protocol audit — <strong>physics</strong> (TE/TR), "
        "<strong>efficiency</strong> (time vs benchmark), and <strong>grade</strong>. "
        "Sequence metadata only — no patient identifiers.</p>",
        unsafe_allow_html=True,
    )

    rules = load_rules()
    rationale = rules.get("clinical_rationale") if isinstance(rules, dict) else None
    source_rows = _build_source_rows(st.session_state.last_sync_sources)

    st.markdown("## 🧠 Clinical Rationale & Evidence")
    with st.expander("View AI Reasoning & Evidence Sources", expanded=False):
        if isinstance(rationale, dict):
            summary = str(rationale.get("summary") or "").strip()
            key_changes = str(rationale.get("key_changes") or "").strip()
            evidence_strength = str(rationale.get("evidence_strength") or "Unavailable").strip()

            c1, c2 = st.columns([2.5, 1.2])
            with c1:
                st.markdown("#### Executive summary")
                st.write(summary or "No executive summary is available in the current rules snapshot.")
            with c2:
                st.markdown("#### Evidence strength")
                st.markdown(_evidence_badge_html(evidence_strength), unsafe_allow_html=True)

            st.markdown("#### 2024-2026 changes versus older protocols")
            st.write(key_changes or "No literature delta summary is available in the current rules snapshot.")
        else:
            st.info("No AI rationale has been synced yet.")

        st.markdown("#### PubMed evidence sources")
        if source_rows:
            st.table(source_rows)
            st.caption(
                "Impact Factor ⭐ uses the app's gold-journal heuristic: starred journals are treated as "
                "high-impact clinical authorities for protocol synthesis."
            )
        else:
            st.caption(
                "PubMed source provenance appears here after a successful sync in the current session."
            )

    with st.expander("How to use", expanded=False):
        st.markdown(
            """
1. **Sync guidelines** — Use the sidebar to **Sync with PubMed** for your study type (e.g. MS, Stroke).  
2. **Upload** — Drag and drop your `.dcm` file into the uploader below.  
3. **Review results** — Check the **Clinical audit** strip for physics accuracy (TE/TR) and the **Efficiency score** for revenue-oriented optimization signals.
            """
        )

    uploaded = st.file_uploader(
        "Drop a DICOM file here",
        type=["dcm"],
        help="Single slice or instance file; headers are read without loading pixel data.",
        label_visibility="visible",
    )

    st.info(PRIVACY_NOTICE, icon="🔒")

    if uploaded is None:
        st.info("Upload a `.dcm` file to run the audit.")
        st.divider()
        st.caption(f"© Protocol Optimiser · Support: [{CONTACT_EMAIL}](mailto:{CONTACT_EMAIL})")
        st.stop()

    tmp_path: str | None = None
    try:
        suffix = Path(uploaded.name).suffix or ".dcm"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="streamlit_dcm_")
        try:
            os.write(fd, uploaded.getvalue())
        finally:
            os.close(fd)

        parsed = parse_dicom(tmp_path)
        result = evaluate(parsed, rules)
        st.session_state.last_magnetic_field_strength_t = parsed.get("magnetic_field_strength_t")

    finally:
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # --- Sequence parameters (no PHI) ---
    series = parsed.get("series_description") or "Unknown"
    te = parsed.get("te_ms")
    tr = parsed.get("tr_ms")
    field_strength_t = parsed.get("magnetic_field_strength_t")
    te_display = "—" if te is None else f"{float(te):g}"
    tr_display = "—" if tr is None else f"{float(tr):g}"
    hardware_display = "Unknown"
    if isinstance(field_strength_t, (int, float)) and field_strength_t > 0:
        hardware_display = f"{float(field_strength_t):.1f}T"

    acq_ms = parsed.get("acquisition_duration_ms")
    est_ms = parsed.get("duration_estimate_ms")
    dur_label = "Acquisition duration"
    if acq_ms is not None:
        dur_display = f"{acq_ms / 1000.0:.2f} s ({acq_ms:,.0f} ms) — from DICOM"
    elif est_ms is not None:
        dur_display = f"{est_ms / 1000.0:.2f} s ({est_ms:,.0f} ms) — heuristic estimate"
        dur_label = "Duration (estimated)"
    else:
        dur_display = "—"
        dur_label = "Acquisition duration"

    st.subheader("Sequence parameters")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Scan hardware", hardware_display)
    c2.metric("Series description", series)
    c3.metric("TE (ms)", te_display)
    c4.metric("TR (ms)", tr_display)
    c5.metric(dur_label, dur_display)

    with st.expander("Acquisition context (non-PHI)", expanded=False):
        m1, m2, m3 = st.columns(3)
        m1.metric("Matrix rows", parsed.get("matrix_rows") if parsed.get("matrix_rows") else "—")
        m2.metric("Matrix columns", parsed.get("matrix_columns") if parsed.get("matrix_columns") else "—")
        m3.metric("Number of averages", parsed.get("number_of_averages") if parsed.get("number_of_averages") else "—")
        st.caption(
            "Heuristic duration (when tag 0018,9073 is absent) uses TR × NSA × echo-train scaling — "
            "illustrative only."
        )

    if result.get("mapped_series_key") and result.get("mapping_method") == "semantic":
        st.info(f"**Series mapping:** {series!r} → **{result['mapped_series_key']}** (semantic match).")

    if st.session_state.rules_version > 0:
        st.caption(f"Rules snapshot **v{st.session_state.rules_version}** — `rules.json` on disk.")

    st.divider()

    # --- Audit strip ---
    grade = result.get("clinical_grade") or "F"
    eff_score = result.get("efficiency_score")
    tgt = result.get("efficiency_target_ms")
    act = result.get("efficiency_actual_ms")
    ratio = result.get("efficiency_ratio")

    st.markdown("### Clinical audit")
    g1, g2, g3, g4 = st.columns(4)

    _grade_colors = {"A": "🟢", "B": "🔵", "C": "🟠", "F": "🔴"}
    g1.markdown(
        f"**Grade**  \n<span style='font-size:2.5rem;font-weight:700'>{_grade_colors.get(grade, '⚪')} {grade}</span>",
        unsafe_allow_html=True,
    )
    g2.metric(
        "Efficiency score",
        f"{eff_score:.0f}/100" if isinstance(eff_score, (int, float)) else "N/A",
        help="100 = at or better than benchmark acquisition time; lower if slower than target.",
    )
    if tgt is not None and act is not None:
        g3.metric("Target time (benchmark)", f"{float(tgt) / 1000.0:.1f} s")
        g4.metric(
            "Observed duration",
            f"{float(act) / 1000.0:.1f} s",
            delta=f"{(float(ratio) - 1) * 100:.0f}% vs target" if ratio is not None else None,
        )
    else:
        g3.metric("Target time (benchmark)", "—", help="Set `target_duration_ms` per series in rules.json")
        g4.metric("Observed duration", "—")

    if result.get("revenue_opportunity") and result.get("revenue_message"):
        st.warning(result["revenue_message"])
    if result.get("hardware_signal_warning"):
        st.warning(result["hardware_signal_warning"])

    status = result["status"]
    messages = result.get("messages") or []

    st.divider()
    st.subheader("Protocol drift (physics)")

    if status == "pass":
        st.success("**Protocol Drift Check: PASS**", icon="✅")
        for msg in messages:
            st.caption(msg)
    else:
        label = "ERROR" if status == "error" else "FAIL"
        detail = "\n\n".join(messages) if messages else "(no details)"
        st.error(f"**Protocol Drift Check: {label}**\n\n{detail}")

    st.divider()
    st.caption(f"© Protocol Optimiser · Support: [{CONTACT_EMAIL}](mailto:{CONTACT_EMAIL})")


if __name__ == "__main__":
    main()
