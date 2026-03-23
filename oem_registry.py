"""Curated registry of MRI-relevant OEM reference documents for RAG enrichment."""

from __future__ import annotations

from pathlib import Path
from typing import Any

OEM_DOCS_DIR = Path(__file__).resolve().parent / "oem_docs"

# Only MRI-relevant documents with meaningful protocol parameters are listed.
# CT-only documents and broad marketing brochures are intentionally excluded.
OEM_DOCUMENT_REGISTRY: list[dict[str, Any]] = [
    {
        "id": "canon_cardiac_cine",
        "label": "Canon — Cardiac Cine Protocol (1.5T/3T, explicit parameters)",
        "filename": "637392863581738707YV.pdf",
        "manufacturer": "Canon",
        "category": "protocol",
        "body_region": "cardiac",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": True,
        "description": "Full parameter tables for SSFP cine imaging: TR, TE, flip angle, FOV, matrix, slice thickness at both field strengths.",
    },
    {
        "id": "canon_cardiac_lge",
        "label": "Canon — Cardiac LGE Protocol (1.5T/3T, explicit parameters)",
        "filename": "637392863633517720JU.pdf",
        "manufacturer": "Canon",
        "category": "protocol",
        "body_region": "cardiac",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": True,
        "description": "Full parameter tables for 2D and 3D Late Gadolinium Enhancement including TI, TR, TE, flip angle.",
    },
    {
        "id": "canon_cardiac_truesssfp",
        "label": "Canon — Cardiac Planning + trueSSFP (1.5T/3T, explicit parameters)",
        "filename": "637392863728765915UZ.pdf",
        "manufacturer": "Canon",
        "category": "protocol",
        "body_region": "cardiac",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": True,
        "description": "Manual cardiac planning with trueSSFP parameter table: TR, TE, flip angle, matrix, slice thickness.",
    },
    {
        "id": "canon_aice_mr_whitepaper",
        "label": "Canon — AiCE Deep Learning Reconstruction (MRI white paper)",
        "filename": "637271900181629483SK-Advanced-intelligent-Clear-IQ-Engine-AiCE-Translating-the-Power-of-Deep-Learning-to-MR-Image-Reconstruction.pdf",
        "manufacturer": "Canon",
        "category": "technology",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "AiCE DCNN architecture and clinical validation for MRI — context for SNR vs scan-time trade-offs on Canon systems.",
    },
    {
        "id": "canon_vantage_galan_3t",
        "label": "Canon — Vantage Galan 3T System Overview",
        "filename": "MRT-3020-Vantage-Galan-3T-V6-Brochure-MCAMR0170EA.pdf",
        "manufacturer": "Canon",
        "category": "system_overview",
        "body_region": "general",
        "field_strengths": ["3.0T"],
        "has_parameters": False,
        "description": "Vantage Galan 3T capabilities including AiCE, Compressed SPEEDER, gradient specs.",
    },
    {
        "id": "canon_scanner_ops",
        "label": "Canon — Scanner Operation Guide (head/neck MRA workflow)",
        "filename": "637392863510820043AM.pdf",
        "manufacturer": "Canon",
        "category": "workflow",
        "body_region": "head_neck",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "Patient registration, map/shimming scans, head and neck MRA scanning workflow.",
    },
    {
        "id": "ge_air_recon_dl_dwi",
        "label": "GE — AIR Recon DL for DWI (protocol parameters)",
        "filename": "mr-gbl-air-recon-dl-jb17939xx-v3.pdf",
        "manufacturer": "GE",
        "category": "protocol",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": True,
        "description": "Technical white paper: DWI protocol parameters, b-values, TE values, NEX comparisons, scan times with DL reconstruction.",
    },
    {
        "id": "ge_air_edition",
        "label": "GE — SIGNA Works AIR Edition Software Overview",
        "filename": "MR-GBL-SW-AIR-Edition-SS-fl.pdf",
        "manufacturer": "GE",
        "category": "technology",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "AIR x auto slice prescription, AIR Touch, AIR Recon, new sequences (Cube, MP-RAGE, DISCO).",
    },
    {
        "id": "ge_air_iq_edition",
        "label": "GE — SIGNA Works AIR IQ Edition Software Overview",
        "filename": "MR_GBL_SW_AIR_IQ_Edition_FINAL.pdf",
        "manufacturer": "GE",
        "category": "technology",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "AIR Recon DL, oZTEo, DISCO Star, MAGiC DWI — resolution and scan-time improvement examples.",
    },
    {
        "id": "ge_air_recon_dl_sellsheet",
        "label": "GE — AIR Recon DL Sell Sheet",
        "filename": "air_air-recon-dl_sell-sheet_mr__glob_JB16238xx.pdf",
        "manufacturer": "GE",
        "category": "technology",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "Marketing overview of AIR Recon DL with illustrative resolution and scan-time examples.",
    },
    {
        "id": "siemens_xa30_deltalist",
        "label": "Siemens — XA30 New Features (full protocol tables)",
        "filename": "MR_XA30_Deltalist_US-.pdf",
        "manufacturer": "Siemens",
        "category": "protocol",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": True,
        "description": "46-page feature guide with full before/after protocol tables: SMS Dixon, RESOLVE, CS SEMAC, SPACE, TOF, ZOOMit PRO, GOLiver.",
    },
    {
        "id": "siemens_xa30_evolve",
        "label": "Siemens — XA30 Evolve Upgrade Flyer",
        "filename": "IBD_MR_XA30_Evolve_Flyer_USA.pdf",
        "manufacturer": "Siemens",
        "category": "technology",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "High-level overview of XA30 Evolve upgrade features (SMS, Compressed Sensing, Prostate Dot Engine).",
    },
    {
        "id": "philips_mr_workspace",
        "label": "Philips — MR Workspace Application Portfolio",
        "filename": "Brochure_MR_Workspace_and_clinical_application_portfolio.pdf",
        "manufacturer": "Philips",
        "category": "system_overview",
        "body_region": "general",
        "field_strengths": ["1.5T", "3.0T"],
        "has_parameters": False,
        "description": "114-page application portfolio: SmartExams, Compressed SENSE, clinical suites (neuro, cardiac, body, MSK).",
    },
]

# Common DICOM Manufacturer tag values mapped to registry manufacturer names.
_MANUFACTURER_ALIASES: dict[str, str] = {
    "ge medical systems": "GE",
    "ge healthcare": "GE",
    "gems": "GE",
    "siemens healthineers": "Siemens",
    "siemens": "Siemens",
    "philips medical systems": "Philips",
    "philips healthcare": "Philips",
    "philips": "Philips",
    "canon medical systems": "Canon",
    "canon": "Canon",
    "toshiba": "Canon",
    "toshiba medical systems": "Canon",
}


def get_manufacturers() -> list[str]:
    """Return sorted list of unique manufacturers in the registry."""
    return sorted({doc["manufacturer"] for doc in OEM_DOCUMENT_REGISTRY})


def get_docs_for_manufacturer(manufacturer: str) -> list[dict[str, Any]]:
    """Return registry entries filtered by manufacturer."""
    return [doc for doc in OEM_DOCUMENT_REGISTRY if doc["manufacturer"] == manufacturer]


def get_doc_by_id(doc_id: str) -> dict[str, Any] | None:
    """Look up a single registry entry by its id."""
    for doc in OEM_DOCUMENT_REGISTRY:
        if doc["id"] == doc_id:
            return doc
    return None


def resolve_doc_path(doc: dict[str, Any]) -> Path:
    """Return the absolute path to the OEM PDF for a registry entry."""
    return OEM_DOCS_DIR / doc["filename"]


def normalize_manufacturer(dicom_manufacturer: str) -> str | None:
    """Map a raw DICOM Manufacturer string to a registry manufacturer name, or None."""
    if not dicom_manufacturer or dicom_manufacturer == "Unknown":
        return None
    key = dicom_manufacturer.strip().lower()
    if key in _MANUFACTURER_ALIASES:
        return _MANUFACTURER_ALIASES[key]
    for alias, canonical in _MANUFACTURER_ALIASES.items():
        if alias in key or key in alias:
            return canonical
    return None
