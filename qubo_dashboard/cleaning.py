from __future__ import annotations

from dataclasses import dataclass

from .dashboard_rules import load_installation_combos


VALID_CHANNELS = {"Chat", "Phone", "Email", "Web"}
CORE_PRODUCT_FAMILIES = {
    "Dash Cam",
    "Smart Camera",
    "Video Doorbell",
    "Smart Lock",
    "GPS Tracker",
    "Air Purifier",
    "Smart Plug",
}
CHANNEL_ALIASES = {
    "whats app": "Chat",
    "whatsapp": "Chat",
    "chat": "Chat",
    "phone": "Phone",
    "email": "Email",
    "web": "Web",
}
PRODUCT_KEYWORDS = (
    ("Dash Cam", ("dash cam", "dashcam", "dash cam pro", "dashcam pro", "dashplay", "dashcam trio", "dashcam 3 channel", "dashcam 4g", "bike cam")),
    ("Smart Camera", ("cam 360", "bullet cam", "home security camera", "smart cameras", "smart camera", "baby camera", "outdoor camera", "indoor camera", "camera")),
    ("Video Doorbell", ("video door bell", "video doorbell", "door bell", "doorbell", "chime")),
    ("Smart Lock", ("door lock", "smart lock", "rim lock", "lock")),
    ("GPS Tracker", ("gps tracker", "wireless tracker", "tracker")),
    ("Air Purifier", ("air purifier", "purifier")),
    ("Smart Plug", ("plug",)),
)
MODEL_PREFIXES = (
    ("Dash Cam", ("hcasv", "hdt")),
    ("Smart Camera", ("hcp", "hcd", "hco", "hca", "hcm")),
    ("Video Doorbell", ("hta", "htbv")),
    ("Smart Lock", ("hlm",)),
    ("GPS Tracker", ("hcagr",)),
    ("Air Purifier", ("hph",)),
    ("Smart Plug", ("hsp",)),
)
BLANK_MARKERS = {"", "-", "0", "null", "none", "nan", "-none-"}
REAL_DEPARTMENTS = {"Hero Electronix", "Call Center", "Field Service", "Logistics"}
NON_ACTIONABLE_FAULT_CODES = {
    "shiprocket",
    "product enquiry",
    "auto product enquiry",
    "outbound",
    "subscription",
    "order related",
}
NON_ACTIONABLE_FAULT_CODE_L2 = {
    "sales query",
    "happy with features",
    "installation request",
    "installation cost",
    "check service area",
    "commissioning process",
    "installation enquiry",
    "installation",
    "service call",
}


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def is_blank_marker(value: str | None) -> bool:
    if value is None:
        return True
    return value.strip().lower() in BLANK_MARKERS


def normalize_channel(channel: str | None, department: str | None) -> str:
    raw_channel = (channel or "").strip()
    raw_department = (department or "").strip()
    if raw_channel:
        mapped = CHANNEL_ALIASES.get(raw_channel.lower())
        if mapped:
            return mapped
        if raw_channel == "WhatsApp":
            return "Chat"
        if raw_channel in VALID_CHANNELS:
            return raw_channel
    if raw_department == "Email":
        return "Email"
    return "Others"


def normalize_department(department: str | None) -> str:
    raw_department = (department or "").strip()
    if raw_department == "Email":
        return "Miscellaneous"
    if raw_department in REAL_DEPARTMENTS:
        return raw_department
    return "Miscellaneous"


def normalize_fault_code(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw and raw.lower() not in BLANK_MARKERS else "Unclassified"


def normalize_version(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw and raw.lower() not in BLANK_MARKERS else "Unknown"


def normalize_resolution(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw else "Unknown"


def canonical_product(product: str | None, device_model: str | None = None) -> str:
    """Returns product family, 'Miscellaneous' for blank fields, or 'Others' for unrecognised products."""
    raw_product = (product or "").strip()
    raw_model = (device_model or "").strip()
    if (not raw_product or raw_product == "-") and (not raw_model or raw_model == "-"):
        return "Miscellaneous"
    lowered = " ".join([raw_product.lower(), raw_model.lower()]).strip()
    if lowered == "shiprocket":
        return "Logistics / Non-product"
    for family, needles in PRODUCT_KEYWORDS:
        if any(needle in lowered for needle in needles):
            return family
    compact_model = raw_model.lower().replace(" ", "")
    for family, prefixes in MODEL_PREFIXES:
        if any(prefix in compact_model for prefix in prefixes):
            return family
    return "Others"


MODEL_KEYWORDS: dict[str, list[tuple[str, tuple[str, ...]]]] = {
    "Dash Cam": [
        ("Dash Cam 4G", ("dashcam 4g", "dash cam 4g", "4g dashcam")),
        ("Dash Cam Pro", ("dashcam pro", "dash cam pro")),
        ("Dashplay", ("dashplay",)),
        ("Dash Cam Trio", ("dashcam trio", "dash cam trio")),
        ("Dash Cam 3-Channel", ("dashcam 3 channel", "dash cam 3 channel", "3 channel")),
        ("Bike Cam", ("bike cam", "bikecam")),
    ],
    "Smart Camera": [
        ("Smart Camera 360", ("cam 360", "camera 360", "360")),
        ("Bullet Cam", ("bullet cam", "bulletcam")),
        ("Baby Camera", ("baby camera", "baby cam")),
        ("Outdoor Camera", ("outdoor camera", "outdoor cam")),
        ("Indoor Camera", ("indoor camera", "indoor cam")),
    ],
}


def normalize_model(product_family: str, raw_product: str | None, raw_model: str | None) -> str:
    """Return a specific model name within a product family, or a readable raw model fallback."""
    family_models = MODEL_KEYWORDS.get(product_family)
    lowered = " ".join([(raw_product or "").lower(), (raw_model or "").lower()]).strip()
    if family_models:
        for model_name, needles in family_models:
            if any(needle in lowered for needle in needles):
                return model_name
    raw_model_clean = (raw_model or "").strip()
    if raw_model_clean and raw_model_clean.lower() not in BLANK_MARKERS:
        return raw_model_clean
    raw_product_clean = (raw_product or "").strip()
    if raw_product_clean and raw_product_clean.lower() not in BLANK_MARKERS and raw_product_clean != product_family:
        return raw_product_clean
    return product_family


# ── Installation ticket classification ────────────────────────────────────────
# A Field Service ticket is an INSTALLATION visit when its
# (Fault_Code, Fault_Code_Level_1, Fault_Code_Level_2) triple — after lowercasing
# and normalising blank markers to "" — exactly matches one of the tuples in
# INSTALLATION_COMBOS below (AND condition across all three fields).
#
# "" in a tuple position means "blank / unspecified" (source value was -, -None-,
# null, 0, etc. — normalised to "Unclassified" by normalize_fault_code, then
# mapped to "" here for combo matching).
#
# ── HOW TO ADD A NEW INSTALLATION COMBINATION ────────────────────────────────
# 1. Run this query against the Zoho source table:
#      SELECT Fault_Code, Fault_Code_Level_1, Fault_Code_Level_2, COUNT(*) AS cnt
#      FROM <table> WHERE Department_Name = 'Field Service'
#      GROUP BY 1, 2, 3 ORDER BY cnt DESC LIMIT 40;
# 2. Identify the row you want to classify as Installation.
# 3. Append a new 3-tuple to INSTALLATION_COMBOS (all values lowercase, blank → "").
# 4. Re-run the pipeline (pipeline_recreate_tables=true drops and recreates tables).
# ─────────────────────────────────────────────────────────────────────────────

INSTALLATION_COMBOS: tuple[tuple[str, str, str], ...] = (
    # (fault_code, fault_code_level_1, fault_code_level_2) — all lowercase, "" = blank

    # blank FC / Installation form request received
    ("", "installation form request received", "installation form request received"),

    # blank or generic FC / 100 engineer visit for installation / blank or confirmed L2
    ("", "100 engineer visit for installation", ""),
    ("", "100 engineer visit for installation", "installation"),
    ("", "100 engineer visit for installation", "installation form request received"),
    ("", "100 engineer visit for installation", "installation done"),
    ("", "100 engineer visit for installation", "raise installation request"),

    # field related FC / 100 engineer visit for installation
    ("field related", "100 engineer visit for installation", ""),
    ("field related", "100 engineer visit for installation", "installation"),
    ("field related", "100 engineer visit for installation", "installation done"),

    # product-category FCs / 100 engineer visit for installation
    ("product issue", "100 engineer visit for installation", ""),
    ("application", "100 engineer visit for installation", ""),
    ("lock product issue", "100 engineer visit for installation", ""),
    ("auto product issue", "100 engineer visit for installation", ""),
    ("home product issue", "100 engineer visit for installation", ""),
    ("ap product issue", "100 engineer visit for installation", ""),

    # field related FC / installation-type L1
    ("field related", "installation", "raise installation request"),
    ("field related", "installation", "installation enquiry"),
    ("field related", "installation", "installation"),
    ("field related", "installation", "re-installation"),
    ("field related", "installation", "installation done"),
)

# Pre-built frozenset for O(1) look-up (populated once at import time).
_INSTALLATION_COMBOS_SET: frozenset[tuple[str, str, str]] = frozenset(INSTALLATION_COMBOS)


def _fc_key(value: str) -> str:
    """Map a pre-normalised fault-code value to an installation-matching key.

    'Unclassified' (placeholder for blank/missing source values) maps to ""
    so it matches the "" wildcard entries in INSTALLATION_COMBOS.
    """
    return "" if value == "Unclassified" else value.lower()


def is_installation_ticket(fault_code: str, fault_code_l1: str, fault_code_l2: str) -> bool:
    """True when this Field Service ticket is an installation visit.

    The (Fault_Code, Fault_Code_Level_1, Fault_Code_Level_2) combination — after
    lowercasing and mapping blank/unclassified values to "" — must exactly match
    one of the entries in INSTALLATION_COMBOS (AND condition across all three
    fields; no substring matching).

    To extend classification: edit `config/dashboard_rules/installation_combos.json`.
    """
    return (_fc_key(fault_code), _fc_key(fault_code_l1), _fc_key(fault_code_l2)) in load_installation_combos()


def is_actionable_issue(fault_code: str, fault_code_l2: str) -> bool:
    fc = fault_code.strip().lower()
    fc2 = fault_code_l2.strip().lower()
    if fc in NON_ACTIONABLE_FAULT_CODES:
        return False
    if fc2 in NON_ACTIONABLE_FAULT_CODE_L2:
        return False
    if "sales query" in fc2 or "happy with feature" in fc2:
        return False
    return True


@dataclass(slots=True)
class TicketQuality:
    usable_issue: bool
    actionable_issue: bool
    bot_journey: bool
    dropped_in_bot: bool
    missing_issue_outside_bot: bool
    dirty_channel: bool
    reassigned_email_department: bool


def evaluate_quality(
    channel: str,
    department: str,
    fault_code: str,
    fault_code_l1: str,
    fault_code_l2: str,
    bot_action: str | None,
) -> TicketQuality:
    bot_action_text = (bot_action or "").strip().lower()
    dropped_in_bot = "blank chat" in bot_action_text
    bot_journey = channel == "Chat" and bot_action_text not in {"", "-", "0"}
    missing_issue = fault_code == "Unclassified" or fault_code_l2 == "Unclassified"
    usable_issue = not missing_issue and not dropped_in_bot
    installation_match = department == "Field Service" and is_installation_ticket(fault_code, fault_code_l1, fault_code_l2)
    actionable_issue = usable_issue and is_actionable_issue(fault_code, fault_code_l2) and not installation_match
    return TicketQuality(
        usable_issue=usable_issue,
        actionable_issue=actionable_issue,
        bot_journey=bot_journey,
        dropped_in_bot=dropped_in_bot,
        missing_issue_outside_bot=missing_issue and channel != "Chat",
        dirty_channel=False,
        reassigned_email_department=(department == "Miscellaneous"),
    )
