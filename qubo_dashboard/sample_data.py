from __future__ import annotations

from datetime import datetime, timedelta
import random

from .models import TicketRecord


def _dt(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M")


def generate_sample_tickets() -> list[TicketRecord]:
    random.seed(11)
    now = datetime(2026, 3, 13, 12, 0)
    clusters = [
        {
            "product": "Qubo Dash Cam Pro 3K",
            "fault_code": "Product issue",
            "fault_code_level_2": "Intermittent offline",
            "version": "DC_5.14.2",
            "department_bias": ["Call Center", "Field Service", "Call Center", "Field Service", "Email"],
            "channel_bias": ["Phone", "WhatsApp", "Chat", "Phone", "Web"],
            "resolution_bias": ["Issue Escalated", "Troubleshooting done issue resolved", "Reset device", "Resolved"],
            "bot_bias": ["Bot transferred to agent", "Bot transferred to agent", "Bot resolved ticket"],
            "symptom": "Camera LED blinks red and app shows device offline",
            "defect": "Intermittent Wi-Fi reconnect loop after firmware update",
            "repair": "Replaced power cable and reconfigured Wi-Fi",
            "commission_min": 5,
            "commission_max": 80,
            "volume": 48,
        },
        {
            "product": "Qubo Smart Cam 360",
            "fault_code": "Home Product issue",
            "fault_code_level_2": "Video feed issue",
            "version": "SC_4.8.0",
            "department_bias": ["Call Center", "Field Service", "Call Center", "Email"],
            "channel_bias": ["Phone", "Chat", "WhatsApp", "Phone"],
            "resolution_bias": ["Issue Escalated", "Resolved", "Reset device", "Troubleshooting done issue resolved"],
            "bot_bias": ["Bot transferred to agent", "Bot transferred to agent", "Bot resolved ticket"],
            "symptom": "Live feed freezes after 20-30 seconds",
            "defect": "Video encoder instability on SC_4.8.0",
            "repair": "Reflashed firmware and reinstalled app",
            "commission_min": 7,
            "commission_max": 120,
            "volume": 38,
        },
        {
            "product": "Qubo Video Doorbell Pro",
            "fault_code": "Installation",
            "fault_code_level_2": "Installation issue",
            "version": "VDB_3.1.4",
            "department_bias": ["Field Service", "Call Center", "Field Service", "Email"],
            "channel_bias": ["Phone", "WhatsApp", "Phone", "Web"],
            "resolution_bias": ["Installation Form", "Resolved", "TAT informed"],
            "bot_bias": ["Bot transferred to agent", "Bot transferred to agent", "Blank chat after 10 mins"],
            "symptom": "Unable to complete installation at doorstep",
            "defect": "No product defect observed, installation dependency",
            "repair": "Completed installation and mounting",
            "commission_min": 0,
            "commission_max": 20,
            "volume": 22,
        },
        {
            "product": "Qubo Smart Lock Select",
            "fault_code": "Lock Product issue",
            "fault_code_level_2": "Lock pairing issue",
            "version": "SL_2.2.0",
            "department_bias": ["Call Center", "Field Service", "Call Center", "Logistics"],
            "channel_bias": ["Phone", "WhatsApp", "Chat", "Phone"],
            "resolution_bias": ["Reset device", "Issue Escalated", "Replacement approved", "Resolved"],
            "bot_bias": ["Bot transferred to agent", "Bot transferred to agent", "Bot resolved ticket"],
            "symptom": "Lock enters pairing mode but app never completes bind",
            "defect": "BLE handshake failure during lock pairing",
            "repair": "Reset lock and replaced controller board",
            "commission_min": 2,
            "commission_max": 90,
            "volume": 24,
        },
        {
            "product": "Qubo GPS Tracker",
            "fault_code": "Tracker Product issue",
            "fault_code_level_2": "Weak signal strength",
            "version": "GT_1.9.7",
            "department_bias": ["Call Center", "Call Center", "Email", "Call Center"],
            "channel_bias": ["Chat", "Phone", "Web", "WhatsApp"],
            "resolution_bias": ["Features Explained", "Resolved", "Bot resolved ticket"],
            "bot_bias": ["Bot resolved ticket", "Bot resolved ticket", "Bot transferred to agent"],
            "symptom": "Tracker location updates are delayed",
            "defect": "Network environment issue, no hardware fault seen",
            "repair": "Explained signal dependency and reconfigured tracking mode",
            "commission_min": 15,
            "commission_max": 200,
            "volume": 18,
        },
        {
            "product": "Qubo Air Purifier Q600",
            "fault_code": "Product issue",
            "fault_code_level_2": "Auto restart",
            "version": "AP_2.0.4",
            "department_bias": ["Field Service", "Call Center", "Logistics", "Field Service"],
            "channel_bias": ["Phone", "WhatsApp", "Email", "Phone"],
            "resolution_bias": ["Issue Escalated", "Replacement approved", "Resolved", "Reset device"],
            "bot_bias": ["Bot transferred to agent", "Bot transferred to agent", "Blank chat after 10 mins"],
            "symptom": "Unit restarts every few minutes",
            "defect": "Power board resets under load",
            "repair": "Replaced main board",
            "commission_min": 3,
            "commission_max": 45,
            "volume": 16,
        },
        {
            "product": "-",
            "fault_code": "-",
            "fault_code_level_2": "",
            "version": "-",
            "department_bias": ["Hero Electronix", "Hero Electronix", "Call Center"],
            "channel_bias": ["9999988888", "00:03:22", "Chat"],
            "resolution_bias": ["Blank chat after 10 mins", "Cancelled due to existing ticket"],
            "bot_bias": ["Blank chat after 10 mins", "Bot resolved ticket"],
            "symptom": "",
            "defect": "",
            "repair": "",
            "commission_min": 0,
            "commission_max": 0,
            "volume": 20,
        },
    ]

    tickets: list[TicketRecord] = []
    ticket_counter = 926000

    for cluster in clusters:
        for index in range(cluster["volume"]):
            created_at = now - timedelta(days=random.randint(0, 180), hours=random.randint(0, 23), minutes=random.randint(0, 59))
            department = random.choice(cluster["department_bias"])
            resolution = random.choice(cluster["resolution_bias"])
            bot_action = random.choice(cluster["bot_bias"])
            channel = random.choice(cluster["channel_bias"])
            reopen = random.choice(["0", "0", "0", "1", "", "-"])
            closed_at = created_at + timedelta(minutes=random.randint(8, 220)) if random.random() > 0.08 else None
            commission_days = random.randint(cluster["commission_min"], cluster["commission_max"])
            first_commissioning = created_at - timedelta(days=commission_days) if cluster["commission_max"] else None
            serial = f"QUBO-{ticket_counter % 7}{10000 + (index * 17)}" if cluster["product"] != "-" else "-"

            tickets.append(
                TicketRecord(
                    ticket_id=f"TCK-{ticket_counter}",
                    created_at=created_at,
                    closed_at=closed_at,
                    department_name=department,
                    channel=channel,
                    email=None,
                    mobile=None,
                    phone=None,
                    name=None,
                    product=cluster["product"],
                    device_model=None,
                    fault_code=cluster["fault_code"],
                    fault_code_level_1=cluster["fault_code"],
                    fault_code_level_2=cluster["fault_code_level_2"],
                    resolution_code_level_1=resolution,
                    bot_action=bot_action,
                    software_version=cluster["version"],
                    status=random.choice(["Closed", "Open", "Escalated"]),
                    device_serial_number=serial,
                    number_of_reopen=reopen,
                    symptom=cluster["symptom"],
                    defect=cluster["defect"],
                    repair=cluster["repair"] if department == "Field Service" or department == "Logistics" else "Guided troubleshooting",
                    first_commissioning_date=first_commissioning,
                    raw={},
                )
            )
            ticket_counter += 1

    tickets.sort(key=lambda item: item.created_at, reverse=True)
    return tickets
