from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .cleaning import (
    CORE_PRODUCT_FAMILIES,
    canonical_product,
    evaluate_quality,
    is_installation_ticket,
    normalize_bot_action,
    normalize_channel,
    normalize_department,
    normalize_fault_code,
    normalize_resolution,
    normalize_version,
)
from .mapping import map_executive_fault_code, map_product_category, normalize_product_name


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
    status: str | None
    device_serial_number: str | None
    number_of_reopen: str | None
    symptom: str | None
    defect: str | None
    repair: str | None
    first_commissioning_date: datetime | None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def canonical_product(self) -> str:
        return canonical_product(self.product, self.device_model)

    @property
    def is_core_product(self) -> bool:
        return self.canonical_product in CORE_PRODUCT_FAMILIES

    @property
    def product_category(self) -> str:
        return map_product_category(self.product, self.canonical_product)

    @property
    def product_name(self) -> str:
        return normalize_product_name(self.product, self.canonical_product)

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
    def normalized_fault_code_l1(self) -> str:
        return normalize_fault_code(self.fault_code_level_1)

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
    def normalized_bot_action(self) -> str:
        return normalize_bot_action(self.bot_action)

    @property
    def executive_fault_code(self) -> str:
        return map_executive_fault_code(self.fault_code_level_1, self.fault_code_level_2)

    @property
    def is_internal_hero(self) -> bool:
        return (self.department_name or "").strip() == "Hero Electronix"

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
        return self.normalized_bot_action == "Bot resolved"

    @property
    def is_bot_transferred(self) -> bool:
        return self.normalized_bot_action == "Bot transferred to agent"

    @property
    def is_blank_chat(self) -> bool:
        return self.normalized_bot_action == "Blank chat"

    @property
    def quality(self):
        return evaluate_quality(
            raw_channel=self.channel,
            raw_department=self.department_name,
            channel=self.normalized_channel,
            department=self.normalized_department,
            fault_code=self.normalized_fault_code,
            fault_code_l1=self.normalized_fault_code_l1,
            fault_code_l2=self.normalized_fault_code_l2,
            bot_action=self.bot_action,
        )

    @property
    def is_fcr_eligible(self) -> bool:
        return self.normalized_department == "Call Center"

    @property
    def is_fcr_success(self) -> bool:
        return self.is_fcr_eligible and (self.number_of_reopen or "").strip() == "0"

    @property
    def has_repeat_key(self) -> bool:
        value = (self.device_serial_number or "").strip()
        return bool(value and value.lower() not in {"0", "-", "", "null", "none", "nan"})

    @property
    def customer_key(self) -> str | None:
        for value in (self.mobile, self.phone, self.email, self.device_serial_number, self.name):
            if value and value.strip().lower() not in {"", "-", "0", "null", "none", "nan"}:
                return value.strip()
        return None

    @property
    def issue_key(self) -> tuple[str, str, str, str]:
        return (
            self.canonical_product,
            self.normalized_fault_code,
            self.normalized_fault_code_l1,
            self.normalized_fault_code_l2,
        )

    @property
    def is_installation_ticket(self) -> bool:
        return is_installation_ticket(self.normalized_fault_code_l1, self.normalized_fault_code_l2)

    @property
    def field_visit_type(self) -> str | None:
        if not self.is_field_service:
            return None
        return "Installation" if self.is_installation_ticket else "Repair"

    @property
    def handle_time_minutes(self) -> float | None:
        if not self.closed_at:
            return None
        delta = self.closed_at - self.created_at
        minutes = delta.total_seconds() / 60
        return minutes if minutes >= 0 else None

    @property
    def device_age_days(self) -> int | None:
        if not self.first_commissioning_date:
            return None
        delta = self.created_at.date() - self.first_commissioning_date.date()
        return delta.days if delta.days >= 0 else None


@dataclass(slots=True)
class DashboardFilters:
    date_start: str | None = None
    date_end: str | None = None
    exclude_installation: bool = False
    exclude_blank_chat: bool = False
    exclude_unclassified_blank: bool = False
    categories: list[str] = field(default_factory=list)
    products: list[str] = field(default_factory=list)
    departments: list[str] = field(default_factory=list)
    channels: list[str] = field(default_factory=list)
    efcs: list[str] = field(default_factory=list)
    issue_details: list[str] = field(default_factory=list)
    statuses: list[str] = field(default_factory=list)
    bot_actions: list[str] = field(default_factory=list)
    include_fc1: list[str] = field(default_factory=list)
    exclude_fc1: list[str] = field(default_factory=list)
    include_fc2: list[str] = field(default_factory=list)
    exclude_fc2: list[str] = field(default_factory=list)
    include_bot_action: list[str] = field(default_factory=list)
    exclude_bot_action: list[str] = field(default_factory=list)
    product_category_overrides: dict[str, str] = field(default_factory=dict)
    efc_overrides: dict[str, str] = field(default_factory=dict)
