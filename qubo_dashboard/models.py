from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .cleaning import (
    CORE_PRODUCT_FAMILIES,
    canonical_product,
    evaluate_quality,
    is_installation_ticket,
    normalize_channel,
    normalize_department,
    normalize_fault_code,
    normalize_model,
    normalize_resolution,
    normalize_version,
)


@dataclass(slots=True)
class TicketRecord:
    ticket_id: str
    created_at: datetime
    closed_at: datetime | None
    department_name: str | None
    channel: str | None
    email: str | None
    mobile: str | None
    phone: str | None
    name: str | None
    product: str | None
    device_model: str | None
    fault_code: str | None
    fault_code_level_1: str | None
    fault_code_level_2: str | None
    resolution_code_level_1: str | None
    bot_action: str | None
    software_version: str | None
    device_serial_number: str | None
    number_of_reopen: str | None
    symptom: str | None
    defect: str | None
    repair: str | None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def canonical_product(self) -> str:
        base = canonical_product(self.product, self.device_model)
        if self.is_blank_chat and base == "Miscellaneous":
            return "Blank Chats"
        return base

    @property
    def canonical_model(self) -> str:
        return normalize_model(self.canonical_product, self.product, self.device_model)

    @property
    def is_core_product(self) -> bool:
        return self.canonical_product in CORE_PRODUCT_FAMILIES

    @property
    def normalized_channel(self) -> str:
        return normalize_channel(self.channel, self.department_name)

    @property
    def normalized_department(self) -> str:
        return normalize_department(self.department_name)

    @property
    def normalized_fault_code(self) -> str:
        return normalize_fault_code(self.fault_code)

    @property
    def normalized_fault_code_l2(self) -> str:
        return normalize_fault_code(self.fault_code_level_2)

    @property
    def normalized_version(self) -> str:
        return normalize_version(self.software_version)

    @property
    def normalized_resolution(self) -> str:
        return normalize_resolution(self.resolution_code_level_1)

    @property
    def is_internal_hero(self) -> bool:
        return self.normalized_department == "Hero Electronix"

    @property
    def is_product_health_eligible(self) -> bool:
        return self.is_core_product

    @property
    def is_field_service(self) -> bool:
        return self.normalized_department == "Field Service"

    @property
    def is_logistics(self) -> bool:
        return self.normalized_department == "Logistics" or "replacement" in self.normalized_resolution.lower()

    @property
    def is_bot_resolved(self) -> bool:
        return (self.bot_action or "").strip() == "Bot resolved ticket"

    @property
    def is_bot_transferred(self) -> bool:
        return (self.bot_action or "").strip() == "Bot transferred to agent"

    @property
    def is_blank_chat(self) -> bool:
        return "blank chat" in (self.bot_action or "").lower()

    @property
    def is_cancelled_existing_ticket(self) -> bool:
        return (self.bot_action or "").strip().lower() == "cancelled due to existing ticket"

    @property
    def quality(self):
        return evaluate_quality(
            channel=self.normalized_channel,
            department=self.normalized_department,
            fault_code=self.normalized_fault_code,
            fault_code_l1=normalize_fault_code(self.fault_code_level_1),
            fault_code_l2=self.normalized_fault_code_l2,
            bot_action=self.bot_action,
        )


    @property
    def has_repeat_key(self) -> bool:
        value = (self.device_serial_number or "").strip()
        return bool(value and value not in {"0", "-", ""})

    @property
    def customer_key(self) -> str | None:
        for value in (self.mobile, self.phone, self.email, self.device_serial_number, self.name):
            if value and value.strip() not in {"", "-", "0"}:
                return value.strip()
        return None

    @property
    def issue_key(self) -> tuple[str, str, str]:
        return (self.canonical_product, self.normalized_fault_code, self.normalized_fault_code_l2)

    @property
    def field_visit_type(self) -> str | None:
        if not self.is_field_service:
            return None
        fc_l1 = normalize_fault_code(self.fault_code_level_1)
        return "Installation" if is_installation_ticket(self.normalized_fault_code, fc_l1, self.normalized_fault_code_l2) else "Repair"

    @property
    def handle_time_minutes(self) -> float | None:
        if not self.closed_at:
            return None
        delta = self.closed_at - self.created_at
        minutes = delta.total_seconds() / 60
        return minutes if minutes >= 0 else None


@dataclass(slots=True)
class DashboardFilters:
    date_preset: str = "60d"
    products: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    fault_codes: list[str] = field(default_factory=list)
    channels: list[str] = field(default_factory=list)
    bot_actions: list[str] = field(default_factory=list)
    quick_exclusions: list[str] = field(default_factory=list)
