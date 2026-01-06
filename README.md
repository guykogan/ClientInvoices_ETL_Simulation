## ETL & Analytics: Clients and Invoices

End-to-end Python pipeline that:

- **Ingests** heterogeneous client and invoice CSVs  
- **Cleans & standardizes** identifiers, dates, and categorical fields  
- **Models** a unified client–invoice dataset  
- **Analyzes** revenue, month-over-month (MoM) growth, and shipping-cost scenarios

This repository is designed to demonstrate practical data-engineering skills: resilient ingestion, heuristic schema detection, vectorized transforms in `pandas`, and clear, reproducible analytics.

---

## Table of Contents

- **Overview**
- **Project Structure**
- **Data & Assumptions**
- **Running the Pipeline**
- **Outputs & Key Findings (Real Results)**
- **Design & Implementation Notes**
- **Next Improvements**

---

## Overview

The goal of this project is to take semi-structured client and invoice files (multiple versions, inconsistent schemas), and turn them into:

- **Cleaned dimensional tables** for clients and invoices  
- **A joined analytical model** (`Clients_Invoices_Model`)  
- **Downstream analysis extracts** answering concrete business questions about revenue concentration, growth, and shipping strategy

The pipeline is intentionally **framework-light**: it is implemented in pure Python + `pandas` so reviewers can focus on data logic, not orchestration boilerplate.

---

## Project Structure

- **`Driver.py`**: Single entry point that orchestrates the full run (ingest → transform → model → analyze → save).
- **`Ingestor.py`**: Scans a directory for CSVs, separates `clients_*` and `invoices_*`, and loads them into `pandas` DataFrames with resilience to malformed rows.
- **`Transformer.py`**:
  - `transform_clients`: infers and cleans client fields (`client_id`, `client_name`, `status`, `start_date`, `tier`).  
  - `transform_invoices`: infers and cleans invoice fields (`invoice_id`, `client_id`, `start_date`, `subtotal`, `tax`, `total`, `shipment_type`).  
  - `combine_single_clients` / `combine_single_invoices`: consolidate multiple partially-overlapping files into single, deduplicated tables.
- **`Model.py`**: `combine` left-joins invoices with client attributes on `client_id`, drops unneeded columns, and returns the final client–invoice analytical model.
- **`Queries.py`**:
  - `invoice_amount_sorted`: total invoice value per client (used for Top 5 lists).  
  - `month_over_month_growth`: MoM totals and growth per client between 2024–2025.  
  - `discount_applied`: applies shipping-type discounts and recomputes totals.  
  - `reclassify_discount`: simulates EXPRESS→GROUND reclassification and computes savings views.
- **Raw data CSVs**: `clients_*.csv`, `invoices_*.csv` (example data used to drive the pipeline).
- **`Outputs/`**: Generated cleaned/model tables (`Clients_Merged_Cleaned.csv`, `Invoices_Merged_Cleaned.csv`, `Clients_Invoices_Model.csv`).
- **`Analysis/`**: Generated analysis extracts answering the business questions below.

---

## Data & Assumptions

- Input CSVs live in the project root (or a directory you configure in `Driver.py`):
  - **Clients:** `clients_*.csv`
  - **Invoices:** `invoices_*.csv`
- The ingestor uses **filename patterns** (`clients_`, `invoices_`) and expects standard CSV.
- Client and invoice schemas can vary by file; the transformer uses **regex-based heuristics** (≥80% column match) to infer:
  - `client_id` (e.g., `^[A-Z][0-9]{5}$`)
  - `client_name` (“First Last” with alphabetic, apostrophes, hyphens)
  - `status` (`ACTIVE` / `INACTIVE` mapped from `active/inactive/y/n`)
  - `start_date` (parsed from multiple common formats → `YYYY-MM-DD`)
  - `tier` (preserved if present, otherwise `NA`)
  - `invoice_id` (e.g., `INV-[A-Z0-9]{7}`)
  - `shipment_type` (`2DAY`, `GROUND`, `FREIGHT`, `EXPRESS`)
- Across versions, conflicting records are resolved by **grouping and taking the first non-null** per key, with `ACTIVE` preferred where applicable.
- Invalid or non-conforming rows (e.g., bad IDs or unparseable dates) are dropped; missing values are represented as `NA`.

---

## Running the Pipeline

### Requirements

- **Python** 3.9+  
- **Python packages** (see `requirements.txt`):
  - `pandas`

Install dependencies (for example, in a virtual environment):

```bash
pip install -r requirements.txt
```

### How to Run

From the project root:

```bash
python Driver.py
```

By default, `Driver.py` looks for `clients_*.csv` and `invoices_*.csv` in the current directory and writes outputs into `Outputs/` and `Analysis/`.

---

## Outputs & Key Findings (Real Results)

### Core Tables (`Outputs/`)

- **`Clients_Merged_Cleaned.csv`**  
  One row per client (`client_id`), containing normalized identifiers, name, status, start date, and tier.

- **`Invoices_Merged_Cleaned.csv`**  
  One row per invoice (`invoice_id`), containing normalized client ID, dates, and financial fields.

- **`Clients_Invoices_Model.csv`**  
  Analytical model: invoices left-joined to client attributes, used as the source for all queries below.

### Analysis Extracts (`Analysis/`)

All numbers below are **real values** produced by running this repository’s code on the included data.
They correspond directly to the CSVs in the `Analysis/` directory.

---

### Business Questions & Answers

- **Q1. Which five clients have the largest total invoice amounts (outstanding)?**  
  **A1.** Stark Partners, Red Logistics, Wayne Group, Umbrella Industries, and Nimbus Holdings, in that order (see details below).

- **Q2. What does month-over-month invoice growth look like per client from 2024–2025?**  
  **A2.** For each client, the pipeline produces a full monthly series with absolute and percentage changes; for example, Zenith Supply grows more than 5× between January and February 2024 before normalizing (example table below).

- **Q3. Under a discount policy (GROUND 20%, FREIGHT 30%, 2DAY 50%), who are the top 5 clients by discounted spend?**  
  **A3.** Stark Partners, Red Logistics, Wayne Group, Wayne Freight, and Umbrella Industries.

- **Q4. If all EXPRESS shipments were re-routed to GROUND and discounts applied, which clients would realize the biggest savings and how large are those savings?**  
  **A4.** Several large shippers would see savings above \$500k, including Stark Partners, Red Logistics, Wayne Group, Umbrella Industries, Nimbus Holdings, Wayne Freight, Hooli Enterprises, Massive Group, Initech Freight, and Vector LLC (see `Savings_Over_500k.csv`).

#### 1) Top 5 clients by total invoice amount (outstanding)

Source: `Analysis/Top5_Invoice_Outstanding.csv`

- **Stark Partners (C25055)** — **2,244,869.53**
- **Red Logistics (C94736)** — **2,218,130.71**
- **Wayne Group (C14175)** — **2,151,906.65**
- **Umbrella Industries (C03366)** — **2,108,671.57**
- **Nimbus Holdings (C77726)** — **2,096,498.84**

This output is produced by `invoice_amount_sorted` in `Queries.py`, which aggregates `total` by (`client_id`, `client_name`) and sorts descending.

#### 2) Month-over-month invoice growth per client (2024–2025)

Source: `Analysis/Month_Per_Month_Invoice_Growth.csv`

Example rows:

| client_id | client_name    | year_month | total     | mom_delta | mom_growth_pct |
|-----------|----------------|-----------:|----------:|----------:|---------------:|
| C02145    | Zenith Supply  | 2024-01    | 14,832.87 |     0.00  |          0.00  |
| C02145    | Zenith Supply  | 2024-02    | 94,097.94 | 79,265.07 |        534.39  |
| C02145    | Zenith Supply  | 2024-03    | 87,691.18 | -6,406.76 |         -6.81  |

Logic is implemented in `month_over_month_growth` in `Queries.py`:

- Coerces `start_date` to datetime and filters the 2024–2025 window.  
- Aggregates total invoice value per client and calendar month.  
- Computes absolute (`mom_delta`) and percentage (`mom_growth_pct`) changes per client.

#### 3) Discount scenario: shipping-type discounts and new Top 5 spenders

Source: `Analysis/Top5_Invoice_Discounts.csv`

Discounts applied at the invoice level:

- **GROUND**: 20% discount (× 0.8)  
- **FREIGHT**: 30% discount (× 0.7)  
- **2DAY**: 50% discount (× 0.5)  
- Other / unknown shipment types: no discount

Top 5 clients by discounted spend:

- **Stark Partners (C25055)** — **1,807,481.38**
- **Red Logistics (C94736)** — **1,767,008.48**
- **Wayne Group (C14175)** — **1,698,476.65**
- **Wayne Freight (C99892)** — **1,688,436.31**
- **Umbrella Industries (C03366)** — **1,684,245.55**

This scenario is implemented via `discount_applied` in `Queries.py`, which maps discounts by `shipment_type`, recomputes totals, and re-aggregates per client.

#### 4) Reclassification scenario: re-route EXPRESS to GROUND and compute savings

Source: `Analysis/Total_Cost_Savings.csv`, `Analysis/Savings_Over_50percent.csv`, `Analysis/Savings_Over_500k.csv`

Business rule:

- Treat all **EXPRESS** shipments as if they had been shipped via **GROUND** instead, then apply the same GROUND/FREIGHT/2DAY discounts as above.  
- Compare new totals to the original spend to quantify savings.

Example rows from the **total savings** table (`Total_Cost_Savings.csv`):

| client_id | client_name        | old_total   | discounted_total | savings      | percent_savings |
|-----------|--------------------|------------:|-----------------:|-------------:|----------------:|
| C99892    | Wayne Freight      | 1,688,436.31| 528,160.38       | 1,160,275.93 | 68.73           |
| C33266    | Hooli Enterprises  | 1,091,505.86| 0.00             | 1,091,505.86 | 100.00          |
| C33326    | Massive Group      |   905,992.79| 0.00             |   905,992.79 | 100.00          |

The same computation is then filtered into two additional views:

- **`Savings_Over_50percent.csv`** — subset of clients whose `percent_savings` > 50%.  
- **`Savings_Over_500k.csv`** — subset of clients with absolute `savings` > 500,000.

These views are produced by `reclassify_discount` in `Queries.py`, which returns:

1. The full savings table.  
2. Clients with **>50% savings**.  
3. Clients with **>$500k savings**.

---

## Design & Implementation Notes

- **Robust schema inference**: Rather than hard-coding column names per file, the transformer uses regexes and threshold-based detection (e.g., ≥80% pattern match) to infer IDs, names, statuses, and dates.
- **Vectorized transformations**: All cleaning and re-aggregation is done in `pandas` using vectorized operations and `groupby` rather than Python loops.
- **Idempotent outputs**: Running `Driver.py` overwrites only generated CSVs in `Outputs/` and `Analysis/`, making the pipeline easy to re-run.
- **Separation of concerns**: Ingestion, transformation, modeling, and analytics live in separate modules, which keeps the ETL flow easy to test and extend.

---

## Next Improvements

- **Testing**: Add unit tests for each transformer/query function and property-based tests for schema inference on synthetic data.
- **Performance**: For very large datasets, consider migrating to **Polars** or a Spark-based backend while preserving the same public interfaces.
- **Configuration**: Move file paths, date ranges, and discount/reclassification rules into a configuration layer (e.g., `YAML` + environment variables).
- **Observability**: Introduce structured logging, simple metrics (row counts, null rates), and basic data-quality checks before and after each stage.