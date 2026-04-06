from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from openpyxl import load_workbook

from .cleaning import BLANK_MARKERS, normalize_fault_code
from .config import settings


FAMILY_TO_CATEGORY = {
    "Dash Cam": "Dashcam",
    "Smart Camera": "Smart Cam",
    "Video Doorbell": "VDB",
    "Smart Lock": "Door Lock",
    "GPS Tracker": "Tracker",
    "Air Purifier": "Air Purifier",
    "Smart Plug": "Other",
}


def _key(value: str | None) -> str:
    return (value or "").strip().lower()


@dataclass(frozen=True, slots=True)
class MappingBundle:
    product_to_category: dict[str, str]
    fc2_to_efc: dict[str, str]


def normalize_product_name(product: str | None, canonical_product: str | None = None) -> str:
    raw = (product or "").strip()
    if not raw or raw.lower() in BLANK_MARKERS:
        return "Blank Product"
    if raw == "-" and canonical_product:
        return canonical_product
    return raw


@lru_cache(maxsize=1)
def load_mappings() -> MappingBundle:
    workbook_path = settings.mapping_workbook_path
    if not workbook_path:
        return MappingBundle(product_to_category={}, fc2_to_efc={})
    path = Path(workbook_path)
    if not path.exists():
        return MappingBundle(product_to_category={}, fc2_to_efc={})

    wb = load_workbook(path, read_only=True, data_only=True)

    product_to_category: dict[str, str] = {}
    if "Product Mapping" in wb.sheetnames:
        ws = wb["Product Mapping"]
        for product, category in ws.iter_rows(min_row=2, values_only=True):
            product_key = _key(str(product) if product is not None else None)
            category_text = (str(category).strip() if category is not None else "") or "Other"
            if product_key:
                product_to_category[product_key] = category_text

    fc2_to_efc: dict[str, str] = {}
    if "EFC Mapping" in wb.sheetnames:
        ws = wb["EFC Mapping"]
        for efc, fc2 in ws.iter_rows(min_row=2, values_only=True):
            fc2_key = _key(str(fc2) if fc2 is not None else None)
            efc_text = (str(efc).strip() if efc is not None else "") or "Other"
            if fc2_key:
                fc2_to_efc[fc2_key] = efc_text

    return MappingBundle(product_to_category=product_to_category, fc2_to_efc=fc2_to_efc)


def map_product_category(product: str | None, canonical_product: str | None = None) -> str:
    mappings = load_mappings()
    normalized_product = normalize_product_name(product, canonical_product)
    direct = mappings.product_to_category.get(_key(normalized_product))
    if direct:
        return direct
    if canonical_product:
        fallback = FAMILY_TO_CATEGORY.get(canonical_product)
        if fallback:
            return fallback
    normalized = normalize_fault_code(normalized_product)
    if normalized == "Unclassified":
        return "Blank Product"
    return "Other"


def map_executive_fault_code(fault_code_level_1: str | None, fault_code_level_2: str | None) -> str:
    mappings = load_mappings()
    efc = mappings.fc2_to_efc.get(_key(fault_code_level_2))
    if efc:
        return efc
    fc1 = normalize_fault_code(fault_code_level_1)
    return fc1 if fc1 != "Unclassified" else "Blank"
