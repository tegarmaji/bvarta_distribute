# bvarta_distribute : Batch Data Pipeline – Medallion Architecture

## Overview
A PySpark batch pipeline that ingests raw user event data, applies data-quality rules, enriches it with a user reference table, and produces analytics-ready aggregates.  
Follows the **Bronze → Silver → Gold** (Medallion) pattern.

## Project Structure
```
**Original Files** :
pipeline.py                  # Main pipeline (all logic lives here)
pipeline_github.py           # Execute the project using Google Colab
config.yaml                  # Path configuration
data/reference/users.csv     # User reference data
data/raw/events/day*.jsonl   # Raw event file

**Generated Files after execution**:
output/bronze/clean/        # output parquet for valid events
output/bronze/quarantine/   # output parquet rejected records
output/silver/              # output parquet Enriched events from Bronze
output/gold/                # output parquet aggregated by event_date & country
```
## Prerequisites
- Python 3.12


## How to run 
1. See demo video on youtube : https://youtu.be/-YOHkbTgZX0

[![Demo on YouTube](http://img.youtube.com/vi/-YOHkbTgZX0/0.jpg)](http://www.youtube.com/watch?v=-YOHkbTgZX0 "pipeline bvarta distribute")

2. Run on Google Colab notebook (Google login required) : https://colab.research.google.com/drive/1DegUDNRH-dHvmsJuselQKkzAf8yxnh3V?usp=sharing


## Assumptions & Technical Choices
Real-world data is rarely clean. So we need a workflow that wouldn't crash just because a timestamp was malformed or a `user_id` was missing. The goal was to create a "defensive" engineering solution that:
* **Never silently drops data:** I read everything as strings initially, so Spark doesnt null out valid values during a strict schema read.
* **Quarantine bad records:** Instead of deleting malformed JSON, I move them to a separate directory for debugging.
* **Handles Late Data:** It uses **Dynamic Partition Overwrite**. If an event arrives two days late, the pipeline only updates that specific historical date partition without touching the rest of the dataset.
* **ANSI Mode:** Disabled `spark.sql.ansi.enabled` to ensures that invalid dates result in `null` values (which we can then quarantine) rather than crashing the entire job.
* **Overwrite vs. Merge:** to handle duplicate data using `Full Partition Overwrite` is much simpler than `MERGE INTO`.
* **How to choose dedup record:** If two rows have the exact same ID and timestamp, the line number acts as the tie-breaker for deduplication.

## How the Pipeline Works
### 1. Ingestion & enforce schema
* **read as raw**: ingest data and identify whether there's corrupt records

### 2. Bronze (data quality & cleaning)
* **Schema Enforcement:** Malformed JSON rows are rejected
* **Deduplication:** We use `event_id` and `event_ts` to ensure we dont double-counting events.
* **Normalizing:** cast string-based numbers (like `"30"`) to float and fill blank/nulls with `0.0` to keep the downstream consistent.

### 3. Silver (Enrichment)
* **Graceful Defaults:** If we cant find user in the reference table, we dont drop the event but just label it as `UNKNOWN`.
* **Derived Features:** added `is_purchase` column flag and calculated `days_since_signup`.

### 4. Gold (The Final Metrics)
The end product is a daily, country-level aggregate table.
* **Metrics:** Tracks total events, total value, total purchases, and unique user counts.

