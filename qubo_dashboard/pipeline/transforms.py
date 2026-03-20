from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..models import TicketRecord


@dataclass(slots=True)
class TicketFacts:
    ticket: TicketRecord
    repeat_flag: bool
    usable_issue: bool
    actionable_issue: bool
    usable_product: bool
    version_covered: bool
    bot_journey: bool
    dropped_in_bot: bool
    missing_issue_outside_bot: bool
    dirty_channel: bool
    reassigned_email_department: bool
    blank_chat_returned_7d: bool
    blank_chat_resolved_7d: bool
    blank_chat_transferred_7d: bool
    blank_chat_blank_again_7d: bool


def build_ticket_facts(tickets: list[TicketRecord]) -> list[TicketFacts]:
    repeat_keyed = defaultdict(list)
    customer_keyed = defaultdict(list)
    for ticket in tickets:
        if ticket.has_repeat_key:
            repeat_keyed[(ticket.device_serial_number or "", ticket.normalized_fault_code)].append(ticket)
        if ticket.customer_key:
            customer_keyed[ticket.customer_key].append(ticket)
    repeat_ids: set[str] = set()
    for items in repeat_keyed.values():
        items.sort(key=lambda item: item.created_at)
        for index in range(1, len(items)):
            if (items[index].created_at - items[index - 1].created_at).days <= 30:
                repeat_ids.add(items[index].ticket_id)
    blank_return_flags: dict[str, tuple[bool, bool, bool, bool]] = {}
    for items in customer_keyed.values():
        items.sort(key=lambda item: item.created_at)
        for index, ticket in enumerate(items):
            if not ticket.is_blank_chat:
                continue
            window_end = ticket.created_at + timedelta(days=7)
            later = [candidate for candidate in items[index + 1 :] if candidate.created_at <= window_end]
            blank_return_flags[ticket.ticket_id] = (
                bool(later),
                any(candidate.is_bot_resolved for candidate in later),
                any(candidate.is_bot_transferred for candidate in later),
                any(candidate.is_blank_chat for candidate in later),
            )
    facts: list[TicketFacts] = []
    for ticket in tickets:
        quality = ticket.quality
        usable_product = True
        version_covered = ticket.normalized_version != "Unknown"
        returned_7d, resolved_7d, transferred_7d, blank_again_7d = blank_return_flags.get(ticket.ticket_id, (False, False, False, False))
        facts.append(
            TicketFacts(
                ticket=ticket,
                repeat_flag=ticket.ticket_id in repeat_ids,
                usable_issue=True,
                actionable_issue=True,
                usable_product=usable_product,
                version_covered=version_covered,
                bot_journey=quality.bot_journey,
                dropped_in_bot=quality.dropped_in_bot,
                missing_issue_outside_bot=quality.missing_issue_outside_bot,
                dirty_channel=quality.dirty_channel,
                reassigned_email_department=quality.reassigned_email_department,
                blank_chat_returned_7d=returned_7d,
                blank_chat_resolved_7d=resolved_7d,
                blank_chat_transferred_7d=transferred_7d,
                blank_chat_blank_again_7d=blank_again_7d,
            )
        )
    return facts


def build_aggregates(ticket_facts: list[TicketFacts]) -> dict[str, list[dict]]:
    return {
        "agg_daily_tickets": agg_daily_tickets(ticket_facts),
        "agg_fc_weekly": agg_fc_weekly(ticket_facts),
        "agg_sw_version": agg_sw_version(ticket_facts),
        "agg_resolution": agg_resolution(ticket_facts),
        "agg_channel": agg_channel(ticket_facts),
        "agg_hourly_heatmap": agg_hourly_heatmap(ticket_facts),
        "agg_replacements": agg_replacements(ticket_facts),
        "agg_bot": agg_bot(ticket_facts),
        "agg_voc_mismatch": agg_voc_mismatch(ticket_facts),
        "agg_anomalies": agg_anomalies(ticket_facts),
        "agg_health_score": agg_health_score(ticket_facts),
        "agg_data_quality": agg_data_quality(ticket_facts),
        "agg_model_breakdown": agg_model_breakdown(ticket_facts),
    }


def agg_daily_tickets(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        key = (
            ticket.created_at.date(),
            ticket.canonical_product,
            ticket.normalized_fault_code,
            ticket.normalized_fault_code_l2,
            ticket.normalized_department,
            ticket.normalized_channel,
            ticket.normalized_version,
        )
        grouped[key].append(fact)
    rows = []
    for key, items in grouped.items():
        count = len(items)
        rows.append(
            {
                "metric_date": key[0],
                "product_family": key[1],
                "fault_code": key[2],
                "fault_code_level_2": key[3],
                "department_name": key[4],
                "channel": key[5],
                "software_version": key[6],
                "tickets": count,
                "field_visit_rate": ratio(sum(1 for item in items if item.ticket.is_field_service), count),
                "repair_field_visit_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), count),
                "installation_field_visit_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Installation"), count),
                "bot_deflection_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), count),
                "bot_transfer_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), count),
                "blank_chat_rate": ratio(sum(1 for item in items if item.dropped_in_bot), count),
                "repeat_rate": ratio(sum(1 for item in items if item.repeat_flag), count),
                "logistics_rate": ratio(sum(1 for item in items if item.ticket.is_logistics), count),
                "handle_time_hours": average([item.ticket.handle_time_minutes / 60 for item in items if item.ticket.handle_time_minutes is not None]),
                "cancelled_existing_ticket_rate": ratio(sum(1 for item in items if item.ticket.is_cancelled_existing_ticket), count),
            }
        )
    return rows


def agg_fc_weekly(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        week_start = ticket.created_at.date() - timedelta(days=ticket.created_at.weekday())
        key = (week_start, ticket.canonical_product, ticket.normalized_fault_code, ticket.normalized_fault_code_l2, ticket.normalized_version)
        grouped[key].append(fact)
    rows = []
    for key, items in grouped.items():
        count = len(items)
        rows.append(
            {
                "week_start": key[0],
                "product_family": key[1],
                "fault_code": key[2],
                "fault_code_level_2": key[3],
                "software_version": key[4],
                "tickets": count,
                "repair_field_visit_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), count),
                "installation_field_visit_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Installation"), count),
                "repeat_rate": ratio(sum(1 for item in items if item.repeat_flag), count),
                "bot_deflection_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), count),
                "bot_transfer_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), count),
                "blank_chat_rate": ratio(sum(1 for item in items if item.dropped_in_bot), count),
                "logistics_rate": ratio(sum(1 for item in items if item.ticket.is_logistics), count),
                "top_symptom": top_text([item.ticket.symptom for item in items]),
                "top_defect": top_text([item.ticket.defect for item in items]),
                "top_repair": top_text([item.ticket.repair for item in items]),
            }
        )
    return rows


def agg_sw_version(ticket_facts: list[TicketFacts]) -> list[dict]:
    if not ticket_facts:
        return []
    latest = max(item.ticket.created_at for item in ticket_facts)
    recent_start = latest - timedelta(days=60)
    previous_start = recent_start - timedelta(days=60)
    grouped_recent = defaultdict(list)
    grouped_previous = defaultdict(list)
    for fact in ticket_facts:
        if not fact.version_covered:
            continue
        ticket = fact.ticket
        key = (ticket.canonical_product, ticket.normalized_version, ticket.normalized_fault_code_l2)
        if ticket.created_at >= recent_start:
            grouped_recent[key].append(fact)
        elif previous_start <= ticket.created_at < recent_start:
            grouped_previous[key].append(fact)
    rows = []
    keys = set(grouped_recent) | set(grouped_previous)
    for key in keys:
        recent = grouped_recent.get(key, [])
        previous = grouped_previous.get(key, [])
        recent_count = len(recent)
        previous_count = len(previous)
        severity = (recent_count * 0.02) + ratio(sum(1 for item in recent if item.ticket.field_visit_type == "Repair"), recent_count or 1) * 120 + ratio(sum(1 for item in recent if item.repeat_flag), recent_count or 1) * 80
        rows.append(
            {
                "as_of_date": latest.date(),
                "product_family": key[0],
                "software_version": key[1],
                "fault_code_level_2": key[2],
                "tickets_60d": recent_count,
                "tickets_prev_60d": previous_count,
                "repair_field_visit_rate": ratio(sum(1 for item in recent if item.ticket.field_visit_type == "Repair"), recent_count or 1),
                "repeat_rate": ratio(sum(1 for item in recent if item.repeat_flag), recent_count or 1),
                "severity_index": severity,
                "coverage_rate": 1.0 if key[1] != "Unknown" else 0.0,
            }
        )
    return rows


def agg_resolution(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        month_start = ticket.created_at.date().replace(day=1)
        key = (month_start, ticket.canonical_product, ticket.normalized_resolution)
        grouped[key].append(fact)
    return [
        {
            "month_start": key[0],
            "product_family": key[1],
            "resolution_code_level_1": key[2],
            "tickets": len(items),
            "bot_deflection_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), len(items)),
            "bot_transfer_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), len(items)),
            "blank_chat_rate": ratio(sum(1 for item in items if item.dropped_in_bot), len(items)),
            "repair_field_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), len(items)),
        }
        for key, items in grouped.items()
    ]


def agg_channel(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        month_start = ticket.created_at.date().replace(day=1)
        key = (month_start, ticket.normalized_channel, ticket.normalized_department)
        grouped[key].append(fact)
    return [
        {
            "month_start": key[0],
            "channel": key[1],
            "department_name": key[2],
            "tickets": len(items),
            "bot_deflection_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), len(items)),
            "bot_transfer_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), len(items)),
            "blank_chat_rate": ratio(sum(1 for item in items if item.dropped_in_bot), len(items)),
            "repair_field_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), len(items)),
            "handle_time_hours": average([item.ticket.handle_time_minutes / 60 for item in items if item.ticket.handle_time_minutes is not None]),
        }
        for key, items in grouped.items()
    ]


def agg_hourly_heatmap(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = Counter()
    for fact in ticket_facts:
        dt = fact.ticket.created_at
        slot = f"{(dt.hour // 4) * 4:02d}:00-{((dt.hour // 4) * 4) + 4:02d}:00"
        grouped[(dt.strftime("%A"), slot)] += 1
    return [{"weekday_name": key[0], "hour_slot_4h": key[1], "tickets": value} for key, value in grouped.items()]


def agg_replacements(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        if not fact.ticket.is_logistics:
            continue
        ticket = fact.ticket
        key = (ticket.created_at.date().replace(day=1), ticket.canonical_product, ticket.normalized_resolution)
        grouped[key].append(fact)
    return [{"month_start": key[0], "product_family": key[1], "resolution_reason": key[2], "tickets": len(items), "estimated_cost": float(len(items) * 450)} for key, items in grouped.items()]


def agg_bot(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    chat_facts = [fact for fact in ticket_facts if fact.ticket.normalized_channel == "Chat"]
    for fact in chat_facts:
        grouped[fact.ticket.canonical_product].append(fact)
    grouped["All Chat"] = chat_facts
    rows = []
    for product_family, items in grouped.items():
        total = len(items)
        blanks = [item for item in items if item.dropped_in_bot]
        blank_count = len(blanks)
        cancelled_count = sum(1 for item in items if item.ticket.is_cancelled_existing_ticket)
        rows.append(
            {
                "product_family": product_family,
                "chat_tickets": total,
                "bot_resolved_tickets": sum(1 for item in items if item.ticket.is_bot_resolved),
                "bot_transferred_tickets": sum(1 for item in items if item.ticket.is_bot_transferred),
                "blank_chat_tickets": blank_count,
                "cancelled_existing_ticket_tickets": cancelled_count,
                "blank_chat_returned_7d": sum(1 for item in blanks if item.blank_chat_returned_7d),
                "blank_chat_resolved_7d": sum(1 for item in blanks if item.blank_chat_resolved_7d),
                "blank_chat_transferred_7d": sum(1 for item in blanks if item.blank_chat_transferred_7d),
                "blank_chat_blank_again_7d": sum(1 for item in blanks if item.blank_chat_blank_again_7d),
                "blank_chat_return_rate": ratio(sum(1 for item in blanks if item.blank_chat_returned_7d), blank_count),
                "blank_chat_recovery_rate": ratio(sum(1 for item in blanks if item.blank_chat_resolved_7d), blank_count),
                "blank_chat_repeat_rate": ratio(sum(1 for item in blanks if item.blank_chat_blank_again_7d), blank_count),
                "bot_resolved_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), total),
                "bot_transferred_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), total),
                "blank_chat_rate": ratio(blank_count, total),
                "cancelled_existing_ticket_rate": ratio(cancelled_count, total),
            }
        )
    return rows


def agg_voc_mismatch(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        key = (ticket.canonical_product, ticket.normalized_fault_code_l2, ticket.defect or "Unknown")
        grouped[key].append(fact)
    rows = []
    for key, items in grouped.items():
        mismatch = sum(1 for item in items if (item.ticket.defect or "").strip().lower() not in key[1].lower())
        rows.append({"product_family": key[0], "fault_code_level_2": key[1], "diagnosed_defect": key[2], "tickets": len(items), "mismatch_rate": ratio(mismatch, len(items))})
    return rows


def agg_anomalies(ticket_facts: list[TicketFacts]) -> list[dict]:
    if not ticket_facts:
        return []
    latest = max(item.ticket.created_at for item in ticket_facts)
    recent_start = latest - timedelta(days=14)
    baseline_start = latest - timedelta(days=74)
    grouped_recent = Counter()
    grouped_baseline = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        key = (ticket.canonical_product, ticket.normalized_fault_code, ticket.normalized_department)
        if ticket.created_at >= recent_start:
            grouped_recent[key] += 1
        elif baseline_start <= ticket.created_at < recent_start:
            grouped_baseline[key].append(ticket)
    rows = []
    for key, current in grouped_recent.items():
        baseline = len(grouped_baseline.get(key, [])) / 60 if grouped_baseline.get(key) else 0.0
        score = ((current / 14) / baseline) if baseline else float(current)
        if score >= 1.2:
            rows.append({"detected_at": latest.date(), "product_family": key[0], "fault_code": key[1], "department_name": key[2], "current_14d": current, "baseline_60d": baseline, "anomaly_score": score})
    return rows


def agg_health_score(ticket_facts: list[TicketFacts]) -> list[dict]:
    daily = defaultdict(list)
    for fact in ticket_facts:
        daily[fact.ticket.created_at.date()].append(fact)
    rows = []
    for day, items in sorted(daily.items()):
        count = len(items)
        repair_rate = ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), count)
        repeat_rate = ratio(sum(1 for item in items if item.repeat_flag), count)
        bot = ratio(sum(1 for item in items if item.ticket.is_bot_resolved), count)
        score = max(0.0, min(10.0, 10 - (repair_rate * 15) - (repeat_rate * 10) + (bot * 2.5)))
        rows.append({"metric_date": day, "health_score": round(score, 2), "repair_field_rate": repair_rate, "repeat_rate": repeat_rate, "bot_deflection_rate": bot})
    return rows


def agg_data_quality(ticket_facts: list[TicketFacts]) -> list[dict]:
    if not ticket_facts:
        return []
    latest = max(item.ticket.created_at for item in ticket_facts).date()
    total = len(ticket_facts)
    return [
        {
            "as_of_date": latest,
            "total_tickets": total,
            "usable_issue_tickets": sum(1 for item in ticket_facts if item.usable_issue),
            "actionable_issue_tickets": sum(1 for item in ticket_facts if item.actionable_issue),
            "blank_fault_code_tickets": sum(1 for item in ticket_facts if item.ticket.normalized_fault_code == "Unclassified"),
            "blank_fault_code_l2_tickets": sum(1 for item in ticket_facts if item.ticket.normalized_fault_code_l2 == "Unclassified"),
            "unknown_product_tickets": sum(1 for item in ticket_facts if item.ticket.canonical_product in {"Miscellaneous", "Blank Chats"}),
            "hero_internal_tickets": sum(1 for item in ticket_facts if item.ticket.is_internal_hero),
            "version_coverage_tickets": sum(1 for item in ticket_facts if item.version_covered),
            "dropped_in_bot_tickets": sum(1 for item in ticket_facts if item.dropped_in_bot),
            "missing_issue_outside_bot_tickets": sum(1 for item in ticket_facts if item.missing_issue_outside_bot),
            "dirty_channel_tickets": sum(1 for item in ticket_facts if item.dirty_channel),
            "email_department_reassigned_tickets": sum(1 for item in ticket_facts if item.reassigned_email_department and item.ticket.normalized_channel == "Email"),
        }
    ]


def agg_model_breakdown(ticket_facts: list[TicketFacts]) -> list[dict]:
    grouped = defaultdict(list)
    for fact in ticket_facts:
        ticket = fact.ticket
        key = (ticket.canonical_product, ticket.canonical_model)
        grouped[key].append(fact)
    rows = []
    for key, items in grouped.items():
        count = len(items)
        rows.append(
            {
                "product_family": key[0],
                "canonical_model": key[1],
                "tickets": count,
                "repair_field_visit_rate": ratio(sum(1 for item in items if item.ticket.field_visit_type == "Repair"), count),
                "repeat_rate": ratio(sum(1 for item in items if item.repeat_flag), count),
                "bot_deflection_rate": ratio(sum(1 for item in items if item.ticket.is_bot_resolved), count),
                "bot_transfer_rate": ratio(sum(1 for item in items if item.ticket.is_bot_transferred), count),
                "blank_chat_rate": ratio(sum(1 for item in items if item.dropped_in_bot), count),
            }
        )
    return rows


def top_text(values: list[str | None]) -> str:
    cleaned = [value.strip() for value in values if value and value.strip()]
    return Counter(cleaned).most_common(1)[0][0] if cleaned else "Unknown"


def ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0
