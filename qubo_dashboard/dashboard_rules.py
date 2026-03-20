from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
RULES_DIR = BASE_DIR / "config" / "dashboard_rules"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
        text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
        return json.loads(text)
    except Exception:
        return {}


def load_installation_combos() -> set[tuple[str, str, str]]:
    payload = _load_json(RULES_DIR / "installation_combos.jsonc")
    combos = set()
    for item in payload.get("combos", []):
        if isinstance(item, dict):
            if item.get("enabled", True) is False:
                continue
            combo = (
                str(item.get("fc", "")).strip().lower(),
                str(item.get("fc1", "")).strip().lower(),
                str(item.get("fc2", "")).strip().lower(),
            )
            combos.add(combo)
            continue
        if isinstance(item, list) and len(item) == 3:
            combos.add(tuple(str(part).strip().lower() for part in item))
    return combos


def load_sales_marketing_rules() -> list[str]:
    payload = _load_json(RULES_DIR / "sales_marketing_fc2.jsonc")
    rules = []
    for item in payload.get("keyword_substrings", []):
        if isinstance(item, dict):
            if item.get("enabled", True) is False:
                continue
            value = str(item.get("value", "")).strip().lower()
            if value:
                rules.append(value)
            continue
        value = str(item).strip().lower()
        if value:
            rules.append(value)
    return rules
