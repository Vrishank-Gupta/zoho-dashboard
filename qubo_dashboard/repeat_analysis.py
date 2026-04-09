from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


RESOLVED_STATUS_MARKERS = ("closed", "resolved", "complete", "completed", "cancel", "duplicate")
UNKNOWN_MARKERS = {"", "unknown", "none", "null", "nan", "-", "--"}


@dataclass(slots=True)
class RepeatSourceTicket:
    ticket_id: str
    customer_key: str
    created_at: datetime
    closed_at: datetime | None
    product_category: str
    product_name: str
    product_family: str
    executive_fault_code: str
    fault_code_level_1: str
    fault_code_level_2: str
    resolution: str
    channel: str
    bot_action: str
    status: str


@dataclass(slots=True)
class RepeatEvent:
    customer_key: str
    product_category: str
    product_name: str
    product_family: str
    first_ticket_id: str
    return_ticket_id: str
    first_created_at: datetime
    return_created_at: datetime
    days_to_return: int
    aging_bucket: str
    first_executive_fault_code: str
    first_fault_code_level_1: str
    first_fault_code_level_2: str
    return_executive_fault_code: str
    return_fault_code_level_1: str
    return_fault_code_level_2: str
    first_resolution: str
    return_resolution: str
    first_channel: str
    return_channel: str
    first_bot_action: str
    return_bot_action: str
    first_status: str
    return_status: str
    same_efc: bool
    same_fc2: bool


def build_repeat_events(rows: list[dict[str, Any]]) -> list[RepeatEvent]:
    grouped: dict[tuple[str, str], list[RepeatSourceTicket]] = {}
    for row in rows:
        customer_key = str(row.get("customer_key") or "").strip()
        product_name = str(row.get("product_name") or "").strip()
        if not customer_key or not product_name:
            continue
        key = (customer_key.lower(), product_name.lower())
        grouped.setdefault(key, []).append(_row_to_source_ticket(row))

    events: list[RepeatEvent] = []
    for tickets in grouped.values():
        tickets.sort(key=lambda item: (item.created_at, item.ticket_id))
        last_resolved: RepeatSourceTicket | None = None
        for ticket in tickets:
            if last_resolved and ticket.created_at > last_resolved.created_at:
                days_to_return = max((ticket.created_at.date() - last_resolved.created_at.date()).days, 0)
                events.append(
                    RepeatEvent(
                        customer_key=ticket.customer_key,
                        product_category=ticket.product_category,
                        product_name=ticket.product_name,
                        product_family=ticket.product_family,
                        first_ticket_id=last_resolved.ticket_id,
                        return_ticket_id=ticket.ticket_id,
                        first_created_at=last_resolved.created_at,
                        return_created_at=ticket.created_at,
                        days_to_return=days_to_return,
                        aging_bucket=_aging_bucket(days_to_return),
                        first_executive_fault_code=last_resolved.executive_fault_code,
                        first_fault_code_level_1=last_resolved.fault_code_level_1,
                        first_fault_code_level_2=last_resolved.fault_code_level_2,
                        return_executive_fault_code=ticket.executive_fault_code,
                        return_fault_code_level_1=ticket.fault_code_level_1,
                        return_fault_code_level_2=ticket.fault_code_level_2,
                        first_resolution=last_resolved.resolution,
                        return_resolution=ticket.resolution,
                        first_channel=last_resolved.channel,
                        return_channel=ticket.channel,
                        first_bot_action=last_resolved.bot_action,
                        return_bot_action=ticket.bot_action,
                        first_status=last_resolved.status,
                        return_status=ticket.status,
                        same_efc=last_resolved.executive_fault_code == ticket.executive_fault_code,
                        same_fc2=last_resolved.fault_code_level_2 == ticket.fault_code_level_2,
                    )
                )
            if _is_resolved_like(ticket):
                last_resolved = ticket
    return events


def _row_to_source_ticket(row: dict[str, Any]) -> RepeatSourceTicket:
    return RepeatSourceTicket(
        ticket_id=str(row.get("ticket_id") or ""),
        customer_key=str(row.get("customer_key") or "").strip(),
        created_at=row["created_at"],
        closed_at=row.get("closed_at"),
        product_category=str(row.get("product_category") or "Other"),
        product_name=str(row.get("product_name") or "Other"),
        product_family=str(row.get("product_family") or row.get("canonical_product") or "Other"),
        executive_fault_code=_clean_label(row.get("executive_fault_code"), "Others"),
        fault_code_level_1=_clean_label(row.get("fault_code_level_1"), "Unclassified"),
        fault_code_level_2=_clean_label(row.get("fault_code_level_2"), "Unclassified"),
        resolution=_clean_label(row.get("resolution"), "Unknown"),
        channel=_clean_label(row.get("channel"), "Unknown"),
        bot_action=_clean_label(row.get("bot_action"), "No recorded bot action"),
        status=_clean_label(row.get("status"), "Unknown"),
    )


def _clean_label(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return fallback if text.lower() in UNKNOWN_MARKERS else text


def _is_resolved_like(ticket: RepeatSourceTicket) -> bool:
    if ticket.closed_at is not None:
        return True
    if ticket.resolution.lower() not in UNKNOWN_MARKERS:
        return True
    lowered_status = ticket.status.lower()
    return any(marker in lowered_status for marker in RESOLVED_STATUS_MARKERS)


def _aging_bucket(days_to_return: int) -> str:
    if days_to_return <= 7:
        return "0-7 days"
    if days_to_return <= 15:
        return "8-15 days"
    if days_to_return <= 30:
        return "16-30 days"
    if days_to_return <= 60:
        return "31-60 days"
    if days_to_return <= 90:
        return "61-90 days"
    return "90+ days"
