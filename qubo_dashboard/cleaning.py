from __future__ import annotations

from dataclasses import dataclass


VALID_CHANNELS = {"Chat", "Phone", "Email", "WhatsApp", "Web"}
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
    "whats app": "WhatsApp",
    "whatsapp": "WhatsApp",
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
        lowered = raw_channel.lower()
        if lowered in {"bot", "chat", "whats app", "whatsapp"}:
            return "Chat"
        if lowered == "phone":
            return "Phone"
        if lowered == "email":
            return "Email"
        if lowered == "web":
            return "Others"
    if raw_department == "Email":
        return "Email"
    return "Others"


def normalize_department(department: str | None) -> str:
    raw_department = (department or "").strip()
    if raw_department in {"Hero Electronix", "Email", "Call Center"}:
        return "Call Center"
    if raw_department in {"Field Service", "Logistics"}:
        return raw_department
    return "Others"


def normalize_fault_code(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw and raw.lower() not in BLANK_MARKERS else "Unclassified"


def normalize_version(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw and raw.lower() not in BLANK_MARKERS else "Not available in source"


def normalize_resolution(value: str | None) -> str:
    raw = (value or "").strip()
    return raw if raw else "Unknown"


def canonical_product(product: str | None, device_model: str | None = None) -> str:
    raw_product = (product or "").strip()
    raw_model = (device_model or "").strip()
    if not raw_product or raw_product == "-":
        if not raw_model or raw_model == "-":
            return "Others"
    lowered = " ".join([raw_product.lower(), raw_model.lower()]).strip()
    for family, needles in PRODUCT_KEYWORDS:
        if any(needle in lowered for needle in needles):
            return family
    compact_model = raw_model.lower().replace(" ", "")
    for family, prefixes in MODEL_PREFIXES:
        if any(prefix in compact_model for prefix in prefixes):
            return family
    return "Others"


def normalize_bot_action(value: str | None) -> str:
    raw = (value or "").strip()
    lowered = raw.lower()
    if not raw or lowered in BLANK_MARKERS:
        return "No bot action"
    if "blank chat" in lowered:
        return "Blank chat"
    if "bot resolved" in lowered:
        return "Bot resolved"
    if "bot transfer" in lowered or "transferred to agent" in lowered:
        return "Bot transferred to agent"
    return "Other bot/system action"


def is_installation_ticket(fault_code: str, fault_code_l2: str, resolution: str, repair: str | None) -> bool:
    haystack = " ".join([fault_code.lower(), fault_code_l2.lower(), resolution.lower(), (repair or "").lower()])
    return "install" in haystack


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
    raw_channel: str | None,
    raw_department: str | None,
    channel: str,
    department: str,
    fault_code: str,
    fault_code_l1: str,
    fault_code_l2: str,
    bot_action: str | None,
) -> TicketQuality:
    normalized_bot_action = normalize_bot_action(bot_action)
    dropped_in_bot = normalized_bot_action == "Blank chat"
    bot_journey = channel == "Chat" and normalized_bot_action != "No bot action"
    missing_issue = (
        fault_code == "Unclassified"
        or fault_code_l1 == "Unclassified"
        or fault_code_l2 == "Unclassified"
    )
    usable_issue = not missing_issue and not dropped_in_bot
    actionable_issue = usable_issue and is_actionable_issue(fault_code, fault_code_l2) and not is_installation_ticket(fault_code, fault_code_l2, "", None)
    return TicketQuality(
        usable_issue=usable_issue,
        actionable_issue=actionable_issue,
        bot_journey=bot_journey,
        dropped_in_bot=dropped_in_bot,
        missing_issue_outside_bot=missing_issue and channel != "Chat",
        dirty_channel=((raw_channel or "").strip().lower() not in {"", "chat", "phone", "email", "web", "whatsapp", "whats app", "bot"}),
        reassigned_email_department=((raw_department or "").strip() == "Email"),
    )
