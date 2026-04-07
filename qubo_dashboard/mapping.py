from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from openpyxl import load_workbook

from .cleaning import BLANK_MARKERS, normalize_fault_code
from .config import settings


def _key(value: str | None) -> str:
    return " ".join((value or "").strip().split()).lower()


@dataclass(frozen=True, slots=True)
class MappingBundle:
    product_to_category: dict[str, str]
    fc2_to_efc: dict[str, str]


def normalize_mapping_value(value: str | None, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def normalize_product_name(product: str | None, canonical_product: str | None = None) -> str:
    raw = str(product or "").strip()
    if not raw or raw.lower() in BLANK_MARKERS:
        return "Blank Product"
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


def map_product_category(
    product: str | None,
    canonical_product: str | None = None,
    overrides: dict[str, str] | None = None,
) -> str:
    mappings = load_mappings()
    raw_product = str(product or "").strip()
    normalized_product = normalize_product_name(product, canonical_product)
    candidates = [
        _key(raw_product),
        _key(normalized_product),
        _key(canonical_product),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        direct = (overrides or {}).get(candidate)
        if direct:
            return direct
        direct = mappings.product_to_category.get(candidate)
        if direct:
            return direct
    if normalized_product == "Blank Product":
        return "Blank Product"
    return "Other"


def map_executive_fault_code(
    fault_code_level_1: str | None,
    fault_code_level_2: str | None,
    overrides: dict[str, str] | None = None,
) -> str:
    mappings = load_mappings()
    raw_fc2 = str(fault_code_level_2 or "").strip()
    normalized_fc2 = normalize_fault_code(fault_code_level_2)
    for candidate in (_key(raw_fc2), _key(normalized_fc2)):
        if not candidate:
            continue
        direct = (overrides or {}).get(candidate)
        if direct:
            return direct
        efc = mappings.fc2_to_efc.get(candidate)
        if efc:
            return efc
    raw_fc1 = str(fault_code_level_1 or "").strip()
    normalized_fc1 = normalize_fault_code(fault_code_level_1)
    for candidate in (_key(raw_fc1), _key(normalized_fc1)):
        if not candidate:
            continue
        direct = (overrides or {}).get(candidate)
        if direct:
            return direct
        efc = mappings.fc2_to_efc.get(candidate)
        if efc:
            return efc
    return "Others"
