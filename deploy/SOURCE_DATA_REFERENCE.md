# Source Data Reference

This document is the source-of-truth reference for the raw Zoho ticket table used by the dashboard analytics pipeline.

It is based on direct inspection of the live source table:

- Database: `zoho`
- Table: `Call_Driver_Data_Zoho_FromAug2024`

The goal of this document is to preserve the source semantics, real value behavior, and agreed mapping rules so analytics logic can be rebuilt or audited later without losing context.

## 1. Scope

For the current analytics rebuild:

- MySQL remains the source of truth
- No source rows should be dropped during extraction
- Analytics scope should be restricted to:
  - `Created_Time >= '2026-02-01'`
- ETL should run nightly
- Each nightly run should fetch tickets created on the previous day and refresh analytics tables derived from that day

Important:

- The source table contains older history before February 1, 2026
- It also contains legacy value patterns that differ from current data

## 2. Source Table Columns Needed

The required raw columns for analytics are:

1. `Created_Time`
2. `Product`
3. `Device_Model`
4. `Department_Name`
5. `Fault_Code`
6. `Fault_Code_Level_1`
7. `Fault_Code_Level_2`
8. `Resolution_Code_Level_1`
9. `Symptom`
10. `Defect`
11. `Repair`
12. `Channel`
13. `Bot_Action`
14. `Status`
15. `Device_Serial_Number`
16. `Ticket_Id`

Important validation finding:

- There is currently **no `Software_Version` column** in the inspected source table
- Any firmware/version analytics cannot be built from this source unless:
  - a different source column is identified, or
  - firmware/version is available in another table/source

## 3. Schema Findings

Observed table characteristics:

- `Created_Time` is a real `datetime`
- `Department_Name`, `Product`, `Device_Model`, `Fault_Code`, `Fault_Code_Level_1`, `Fault_Code_Level_2`, `Resolution_Code_Level_1`, `Channel`, `Bot_Action`, `Status`, `Device_Serial_Number`, `Symptom`, `Defect`, and `Repair` are present
- `Modified_Time` exists, but it is stored as `varchar(20)`, not a datetime
- `Ticket_Closed_Time` is also stored as `varchar(20)`
- `Status` is available and should be preserved in the raw fact layer

## 4. Volume Snapshot

Observed from the live source table:

- `total_rows`: `882,694`
- `rows_since_2026_02_01`: `107,661`
- `max_created`: `2026-03-31 23:59:00`
- `min_created`: `0000-00-00 00:00:00`

Important implications:

- Some rows contain invalid zero-datetime values historically
- Extraction and analytics should not assume the full table has clean timestamps

## 5. Department Semantics

### Business meaning

`Department_Name` represents the operational team handling the ticket.

Expected business departments:

- `Call Center`
- `Field Service`
- `Logistics`

Legacy/current special cases:

- `Hero Electronix`
  - old department label
  - should be mapped to `Call Center`
- `Email`
  - not a true department
  - should be mapped to:
    - Department = `Call Center`
    - Channel = `Email`

### Observed live values

All-time counts:

- `Hero Electronix`: `453,831`
- `Call Center`: `222,071`
- `Field Service`: `139,609`
- `Email`: `43,555`
- blank: `21,606`
- `Logistics`: `2,022`

Since `2026-02-01`:

- `Call Center`: `72,802`
- `Field Service`: `17,250`
- `Email`: `16,280`
- `Logistics`: `1,329`
- `Hero Electronix`: not observed in this filtered slice

### Mapping rule

Recommended canonical department mapping:

- `Hero Electronix` -> `Call Center`
- `Email` -> `Call Center`
- `Call Center` -> `Call Center`
- `Field Service` -> `Field Service`
- `Logistics` -> `Logistics`
- blank or anything unexpected -> `Others`

## 6. Channel Semantics

### Business meaning

`Channel` is the incoming support channel.

Expected canonical channels:

- `Phone`
- `Chat`
- `Email`
- `Others`

Chat family should include:

- `Chat`
- `WhatsApp`
- `Whats App`
- `Bot` if it ever appears as a channel value

Current bot behavior in source:

- bot traffic is stored under `Channel = Chat`
- bot-specific behavior is identified via `Bot_Action`
- there is no evidence in the inspected live data that a separate `Bot` channel is currently used

### Observed live values

All-time major values:

- `Chat`: `341,742`
- `Phone`: `224,756`
- `Email`: `141,075`
- `Whats App`: `100,515`
- `Web`: `50,929`
- blank: `21,616`

There are also many bad channel values:

- durations like `1mos 0w`, `2w 2d`
- phone numbers
- status-like strings
- resolution-like strings
- `Dealer`, `Twitter`, `Facebook`, `Video Call`, etc.

Since `2026-02-01`:

- `Chat`: `63,573`
- `Phone`: `20,070`
- `Email`: `17,104`
- `Web`: `6,893`
- `Dealer`: `12`
- plus a small number of dirty values

Notably:

- `Whats App` was not present in the post-February slice checked
- `Bot` was not present as a channel value

### Mapping rule

Recommended canonical channel mapping:

- `Chat`, `WhatsApp`, `Whats App`, `Bot` -> `Chat`
- `Phone` -> `Phone`
- `Email` -> `Email`
- `Web` -> `Others` or `Web`

Decision note:

- `Web` is now explicitly mapped into `Others`
- If separate `Web` reporting matters operationally, keep `Web` as its own canonical value

Everything else:

- blank
- phone numbers
- durations
- accidental text values

should map to:

- `Others`

## 7. Bot Action Semantics

### Business meaning

`Bot_Action` should only be meaningful for chatbot-originated tickets.

Expected business buckets:

- bot resolved
- bot transferred to agent
- blank chat
- no bot action

### Observed live values since `2026-02-01`

- `-`: `43,227`
- `Bot resolved ticket`: `23,615`
- `Blank chat  if user doesnot reply after 10 mins`: `15,060`
- `Bot transferred to agent`: `14,798`
- `Cancelled due to existing ticket`: `10,590`
- `Software update Required`: `360`
- blank: `5`
- very rare junk/string noise rows also exist

### Important findings

- Bot action values are not limited to only the three ideal values
- There are non-bot workflow values present in `Bot_Action`
- Blank chat values are verbose and not standardized exactly
- Case and wording are not fully controlled

### Mapping rule

Recommended canonical bot action groups:

- if text contains `blank chat` -> `Blank chat`
- if text contains `bot resolved` -> `Bot resolved`
- if text contains `bot transfer` or `transferred to agent` -> `Bot transferred to agent`
- if value is `-`, blank, or null -> `No bot action`
- anything else -> `Other bot/system action`

### Interaction with Channel

Observed current behavior:

- when `Bot_Action` is meaningful, `Channel` is typically `Chat`
- when `Bot_Action` is `-` or blank, the ticket is usually not a bot-flow ticket

## 8. Product And Device Model

### Business meaning

- `Product` = customer-facing product name
- `Device_Model` = hardware model identifier

Examples from recent rows:

- `Cam 360`
- `Bullet Cam`
- `Qubo Video Door Bell Pro`
- `Dash Cam Pro 4K Front And Rear`

### Completeness since `2026-02-01`

- missing product: `38,444`
- missing device model: `54,030`

Important implication:

- Product inference must tolerate missing product and missing model
- Bot and installation flows often have partial product capture

## 9. Fault Codes

### Business meaning

All three levels should be preserved:

- `Fault_Code`
- `Fault_Code_Level_1`
- `Fault_Code_Level_2`

### Observed behavior

Recent sample rows show:

- `Fault_Code` often populated for chat flows
- `Fault_Code_Level_1` can be populated even when `Fault_Code` is `-`
- `Fault_Code_Level_2` is frequently missing on incomplete bot/dropoff journeys

Examples from recent rows:

- `Product issue` / `Live monitoring isssue` / `Update Wi-Fi`
- `Home Product issue` / `SD Card issue` / `SD card not detected in app`
- `Installation` / `Installation enquiry` / `Check service area`

### Completeness since `2026-02-01`

- missing `Fault_Code`: `46,893`
- missing `Fault_Code_Level_1`: `25,242`
- missing `Fault_Code_Level_2`: `33,846`

This means:

- lower-level issue coding is materially incomplete
- analytics must not assume all tickets can be issue-classified fully

## 10. Resolution Code 1

### Business meaning

`Resolution_Code_Level_1` is the resolution provided by the agent or workflow.

Observed examples in recent rows:

- `Week Signal Strengh`
- `Bot resolved ticket`
- `Blank chat  if user doesnot reply after 10 mins`
- `-`

Important observation:

- this field sometimes stores actual operational resolution labels
- but it also sometimes mirrors bot workflow outcomes

Completeness since `2026-02-01`:

- missing `Resolution_Code_Level_1`: `28,654`

## 11. Symptom / Defect / Repair

### Business meaning

These fields are expected to be populated mainly by:

- Field Service technicians
- escalated Call Center L3 teams

They represent:

- `Symptom`: what the device is showing
- `Defect`: diagnosis
- `Repair`: suggested or performed resolution

### Actual data behavior

These fields are mostly empty in the current post-February slice.

Completeness since `2026-02-01`:

- missing `Symptom`: `94,659`
- missing `Defect`: `10`
- missing `Repair`: `94,666`

Important anomaly:

- `Defect` appears almost fully populated while `Symptom` and `Repair` are mostly empty
- this should be validated again before assuming the field is truly trustworthy
- some rows may contain placeholder/non-informative defect strings rather than clean diagnoses

## 12. Status

### Business meaning

`Status` is the current ticket state and should be preserved in the raw fact layer.

### Observed live values since `2026-02-01`

Major values:

- `Closed`: `66,172`
- `Cancelled`: `23,517`
- `Open`: `10,275`
- `Duplicate Closed`: `5,763`

Long-tail values include:

- `Troubleshoot Pending L1`
- `Troubleshoot Pending L3`
- `Replacement Dispatched`
- `Replacement`
- `Escalated`
- `Assigned to Installer`
- `Installation Started`

Completeness since `2026-02-01`:

- missing `Status`: `0`

## 13. Device Serial Number

### Business meaning

`Device_Serial_Number` is the unique identifier of the device.

Expected behavior:

- often missing for installation queries
- often missing for general queries
- may be missing for partial bot/dropoff tickets

### Completeness since `2026-02-01`

- missing serial: `71,907`

This is a major limitation for:

- repeat analysis
- device-level linkage

## 14. Blank Chat Behavior

Observed since `2026-02-01`:

- blank chat rows: `15,060`
- blank chat missing product: `5,864`
- blank chat missing fault code level 1: `8,415`
- blank chat missing fault code level 2: `14,214`
- blank chat missing serial: `2,376`

Interpretation:

- many blank-chat tickets still have product selected
- many blank-chat tickets still have fault code level 1 selected
- fault code level 2 is often missing, which matches the business expectation that users may drop before completing all bot prompts
- serial number is not universally missing in blank chats

## 15. Recent Raw Row Behavior

Recent examples confirm the following:

- `Email` appears both as `Department_Name` and `Channel`
- chatbot tickets are stored under `Channel = Chat`
- `Bot_Action` values drive bot classification
- incomplete bot tickets often contain `-` for product and/or fault fields
- installation chats often have:
  - `Fault_Code = Installation`
  - `Fault_Code_Level_1 = Installation enquiry`
  - level 2 issue such as `Installation request` or `Check service area`
- serial number can be:
  - real-looking identifier
  - `-`
  - literal string `Null`

## 16. Canonical Mapping Rules To Use Going Forward

These are the agreed business mappings that analytics should apply.

### Department mapping

- `Hero Electronix` -> `Call Center`
- `Email` -> `Call Center`
- `Call Center` -> `Call Center`
- `Field Service` -> `Field Service`
- `Logistics` -> `Logistics`
- blank/unknown -> `Others`

### Channel mapping

- `Chat`, `WhatsApp`, `Whats App`, `Bot` -> `Chat`
- `Email` -> `Email`
- `Phone` -> `Phone`
- `Web`, `Dealer`, `Twitter`, `Facebook`, `Video Call`, blank, junk values -> `Others`

### Bot action mapping

- contains `blank chat` -> `Blank chat`
- contains `bot resolved` -> `Bot resolved`
- contains `bot transfer` or `transferred to agent` -> `Bot transferred to agent`
- `-`, blank, null -> `No bot action`
- all other values -> `Other bot/system action`

### Serial-number cleaning

Treat the following as missing:

- `NULL`
- `Null`
- ``
- `-`
- `0`

## 17. Important Differences From Older Code

The current codebase assumptions differ from the live-source findings in a few important ways:

- old code treats `Hero Electronix` as a separate department
  - new agreed rule: map it to `Call Center`
- old code treats `Email` department as dirty data
  - new agreed rule: map department to `Call Center` and count channel as `Email`
- old code keeps `WhatsApp` as a separate channel label
  - new agreed rule: fold WhatsApp into `Chat`
- old code expects a `Software_Version` column
  - live source currently does not have one
- old code maps unknown channels to `Unknown / Dirty Data`
  - new agreed rule should use `Others`

## 18. Open Questions

These still need business confirmation:

1. Firmware/version source:
   - where should software version come from, since this table does not have `Software_Version`?
2. Web channel:
   - should `Web` be preserved separately or folded into `Others`?
3. Bot action long-tail values:
   - should values like `Cancelled due to existing ticket` and `Software update Required` be grouped into a meaningful bot outcome bucket?
4. Dirty channels:
   - should all stray values remain in `Others`, or should some be split into a data-quality bucket for monitoring?

## 19. Final Guidance

For extraction:

- pull every row since `2026-02-01`
- do not drop incomplete rows at source-ingest time
- preserve all raw fields listed above

For analytics:

- build canonical mapped fields in the warehouse
- keep both raw and mapped values when practical
- treat the live-source findings in this document as the contract unless newer data inspection proves otherwise
