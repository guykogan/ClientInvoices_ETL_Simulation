# ETL + Analytics: Clients & Invoices

A small, friendly pipeline that:
1) **Ingests** client and invoice CSVs  
2) **Cleans & de-duplicates** them into tidy tables  
3) **Joins** invoices ↔ clients  
4) **Analyzes** totals, month-over-month growth, and “what-if” discount scenarios

---

## Project Structure

- **Driver.py** — Orchestrates the whole run (ingest → transform → model → analyze → save).
- **Ingestor.py** — Finds and loads CSVs from a folder.
- **Transformer.py** — Normalizes columns, parses dates, and collapses duplicates.
- **Model.py** — Left-joins invoices with client attributes on `client_id`.
- **Queries.py** — Reusable analysis helpers (totals, MoM growth, discount & reclass scenarios).

---

## Getting Started

## Input Files

Place your CSVs in the project root (or set a different directory in `Driver.py`):

- **Clients:** `clients_v1.csv`, `clients_v2.csv`, …
- **Invoices:** `invoices_v1.csv`, `invoices_v2.csv`, …

> The ingestor looks for filenames that clearly indicate “clients” or “invoices”.

### Requirements
- Python 3.9+  
- `pandas`

```bash
pip install pandas
```

## Run

```bash
python Driver.py
```

## What You’ll Get

**Cleaned / Modeled Tables → `Outputs/`**

- `Clients_Merged_Cleaned.csv` — One row per client (`client_id`).
- `Invoices_Merged_Cleaned.csv` — One row per invoice (`invoice_id`).
- `Clients_Invoices_Model.csv` — Invoices joined with (`client_id`).

**Analysis Extracts → `Analysis/`**

- `Top5_Invoice_Outstanding.csv` — Top clients by total invoice amount.
- `Month_Per_Month_Invoice_Growth.csv` — Per-client monthly totals, deltas, and % growth (2024–2025).
- `Top5_Invoice_Discounts.csv` — Totals after applying discounts (GROUND 20%, FREIGHT 30%, 2 DAY 50%).
- `Total_Cost_Savings.csv` — Savings per client under EXPRESS→GROUND reclassification.
- `Savings_Over_50percent.csv` — Clients with >50% savings under reclassification.
- `Savings_Over_500k.csv` — Clients with >$500k savings under reclassification.

### Example outputs for the analysis queries

**Top 5 clients by total invoice amount**  
_File: `Analysis/Top5_Invoice_Outstanding.csv`_

| client_id | client_name | total |
| --- | --- | --- |
| C25055 | Stark Partners | 2,244,869.53 |
| C94736 | Red Logistics | 2,218,130.71 |
| C14175 | Wayne Group | 2,151,906.65 |

**Month-over-month invoice growth (2024–2025)**  
_File: `Analysis/Month_Per_Month_Invoice_Growth.csv`_

| client_id | client_name | year_month | total | mom_delta | mom_growth_pct |
| --- | --- | --- | --- | --- | --- |
| C02145 | Zenith Supply | 2024-01 | 14,832.87 | 0.00 | 0.00 |
| C02145 | Zenith Supply | 2024-02 | 94,097.94 | 79,265.07 | 534.39 |
| C02145 | Zenith Supply | 2024-03 | 87,691.18 | -6,406.76 | -6.81 |

**Discount scenario (GROUND 20%, FREIGHT 30%, 2 DAY 50%) — new Top 5**  
_File: `Analysis/Top5_Invoice_Discounts.csv`_

| client_id | client_name | discounted_total |
| --- | --- | --- |
| C25055 | Stark Partners | 1,807,481.38 |
| C94736 | Red Logistics | 1,767,008.48 |
| C14175 | Wayne Group | 1,698,476.65 |

**Reclassification scenario (EXPRESS → GROUND) — savings per client**  
_File: `Analysis/Total_Cost_Savings.csv`_

| client_id | client_name | old_total | discounted_total | savings | percent_savings |
| --- | --- | --- | --- | --- | --- |
| C99892 | Wayne Freight | 1,688,436.31 | 528,160.38 | 1,160,275.93 | 68.73 |
| C33266 | Hooli Enterprises | 1,091,505.86 | 0.00 | 1,091,505.86 | 100.00 |
| C33326 | Massive Group | 905,992.79 | 0.00 | 905,992.79 | 100.00 |

**Clients with >50% savings**  
_File: `Analysis/Savings_Over_50percent.csv`_

| client_id | client_name | old_total | discounted_total | savings | percent_savings |
| --- | --- | --- | --- | --- | --- |
| C33266 | Hooli Enterprises | 1,091,505.86 | 0.00 | 1,091,505.86 | 100.00 |
| C33326 | Massive Group | 905,992.79 | 0.00 | 905,992.79 | 100.00 |
| C78191 | Quantum Enterprises | 1,228,809.75 | 348,828.03 | 879,981.72 | 71.64 |

**Clients with >$500k savings**  
_File: `Analysis/Savings_Over_500k.csv`_

| client_id | client_name | old_total | discounted_total | savings | percent_savings |
| --- | --- | --- | --- | --- | --- |
| C99892 | Wayne Freight | 1,688,436.31 | 528,160.38 | 1,160,275.93 | 68.73 |
| C33266 | Hooli Enterprises | 1,091,505.86 | 0.00 | 1,091,505.86 | 100.00 |
| C33326 | Massive Group | 905,992.79 | 0.00 | 905,992.79 | 100.00 |

### What I’d do differently for production

- **Discover more edge cases via tests**: unit + property tests with synthetic data.
- **Algorithmically improve runtime**: heavier vectorization; consider **Polars**; batch I/O.
- **logging & observability**: structured logs, metrics, basic dashboards/alerts.
- **Config, not code**: paths, thresholds, and regexes in env/config/

### Assumptions & Data Handling

I used pandas throughout: columns were detected via simple heuristics/regex (≥80% pattern match to rename to `client_id`, `client_name`, `status`, `start_date`) and dates normalized to `YYYY-MM-DD`. 
Across file variants, fields were reconciled by standardizing names/values (`Y/N`→`ACTIVE/INACTIVE`, shipment types uppercased with `EXPRESS→GROUND` when reclassing) and then de-duplicated via `groupby` with “first non-null” selection, 
giving `ACTIVE` precedence where conflicts existed. Missing/invalid values were set to `NA` (rows failing key patterns were dropped), and joins relied on `client_id` as the stable key.

### 1) Top 5 clients with the largest total invoice amounts (outstanding)
_Source: `Analysis/Top5_Invoice_Outstanding.csv`_

- **Stark Partners (C25055)** — **2,244,869.53**
- **Red Logistics (C94736)** — **2,218,130.71**
- **Wayne Group (C14175)** — **2,151,906.65**
- **Umbrella Industries (C03366)** — **2,108,671.57**
- **Nimbus Holdings (C77726)** — **2,096,498.84**

### 2) Month-over-month invoice growth per client (2024–2025)
Please see **`Analysis/Month_Per_Month_Invoice_Growth.csv`** for the full per-client, per-month table (totals, MoM delta, MoM %).

### 3) Discount scenario (GROUND 20%, FREIGHT 30%, 2 DAY 50%) — new Top 5 spenders
_Source: `Analysis/Top5_Invoice_Discounts.csv`_

- **Stark Partners (C25055)** — **1,807,481.38**
- **Red Logistics (C94736)** — **1,767,008.48**
- **Wayne Group (C14175)** — **1,698,476.65**
- **Wayne Freight (C99892)** — **1,688,436.31**
- **Umbrella Industries (C03366)** — **1,684,245.55**

### 4) Reclassification scenario (all EXPRESS → GROUND)

**a) Total cost savings opportunity per client**  
See **`Analysis/Total_Cost_Savings.csv`** for the complete client list (with `old_total`, `discounted_total`, `reclass_total`, `savings`, `percent_savings`).

**b) Clients with >50% savings**  
See **`Analysis/Savings_Over_50percent.csv`** Answer is 0.

**c) Clients with >$500k savings**  
See **`Analysis/Savings_Over_500k.csv`**.