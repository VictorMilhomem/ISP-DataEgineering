# ISP-RJ Crime Statistics Lakehouse Pipeline

An end-to-end data pipeline built on **Databricks**, **Delta Lake**, and **Unity Catalog** to ingest, clean, normalize, and model historical public safety indicators from the Instituto de Segurança Pública do Rio de Janeiro (ISP-RJ).

---

## Architecture Overview

The pipeline implements a **Medallion Architecture** using PySpark and Structured Streaming:

```
[ISP CSV / Landing Zone]
           │
           ▼
     ┌───────────┐
     │  Bronze   │  Raw Delta ingestion (isp.bronze.timeseries_monthly_since_2003)
     └─────┬─────┘
           │ Structured Streaming + ForeachBatch Upsert
           ▼
     ┌───────────┐
     │  Silver   │  Type casting, null filling, unpivoting (wide-to-long)
     └─────┬─────┘  (isp.silver.timeseries_monthly_since_2003)
           │ Micro-batch Streaming + Window Analytics
           ▼
     ┌───────────┐
     │   Gold    │  Analytical models (fact_monthly_crime_rates & agg_annual_crime_summary)
     └───────────┘

```

---

## Data Layers

### 1. Bronze Layer (`isp.bronze.timeseries_monthly_since_2003`)

* **Source**: `BaseEstadoTaxaMes.csv` containing monthly state-level crime rates from 2003 onwards.


* **Format**: Delta Lake table storing raw records as ingested from source files.
* **Schema**: 57 columns formatted with localized Brazilian decimal strings (e.g., `"4,01"`) across 53 indicator columns.

### 2. Silver Layer (`isp.silver.timeseries_monthly_since_2003`)

* **Type Normalization**: Converts decimal strings (`","` to `"."`) and casts rates to `DoubleType`.
* **Zero-filling**: Imputes `NULL` values in crime metrics with `0.0`.
* **Date Derivation**: Generates `reference_date` (`DateType`) from `ano` and `mes` (`make_date(ano, mes, 1)`).
* **Wide-to-Long Normalization (Unpivot)**: Transforms the 53 disparate indicator columns into two unified columns:
* `ocorrencia` (`StringType`): Identifier of the criminal occurrence / metric.
* `taxa` (`DoubleType`): Rate per 100k inhabitants or reference unit.


* **Primary Key / Natural Grain**: `(ano, mes, ocorrencia)`.
* **Idempotency**: Utilizes Delta `MERGE` inside `foreachBatch` to ensure atomic, duplicate-free streaming updates without stateful memory overhead.

### 3. Gold Layer

* **`isp.gold.fact_monthly_crime_rates`**:
* **Categorization**: Maps individual offenses into analytical dimensions (`Crimes Contra a Vida e CVLI`, `Crimes Contra o Patrimônio (Roubos)`, `Crimes Contra o Patrimônio (Furtos)`, `Crimes Culposos / Acidentes`, `Entorpecentes e Drogas`).
* **Time-Series Metrics**: Calculates rolling 3-month moving averages (`media_movel_3m`) and Year-over-Year variation percentages (`variacao_yoy_perc`).


* **`isp.gold.agg_annual_crime_summary`**:
* Consolidates monthly data into annual views (`taxa_media_mensal`, `taxa_maxima_mensal`, and `taxa_acumulada_anual`).



---

## Project Structure

```
ISP-RJ
├── notebooks/
│   ├── bronze
        ├── extract_raw.ipynb    
        ├── load_bronze.ipynb     # Ingestion from raw volume/storage into Bronze Delta table
│   ├── silver    
        ├── load_silver.ipynb     # Streaming pipeline with unpivot & cleaning logic
│   ├── gold    
        └── load_gold.ipynb       # Dimensional modeling, window metrics & annual aggregations
└── README.md

```

---

## Technical Setup & Configuration

### Prerequisites

* Databricks Runtime (DBR) 13.3+ LTS or higher.
* Unity Catalog enabled workspace.
* A Unity Catalog Volume configured for checkpoint locations:
```
/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/_checkpoints/

```

### Namespace Hierarchy

* **Catalog**: `isp`

* **Schemas**: `bronze`, `silver`, `gold`

* **Volumes**: `isp_volumes`

---


### Consumption Examples (Gold Layer)

**Monthly Time Series with Moving Average**:

```sql
SELECT 
    reference_date,
    categoria_ocorrencia,
    ocorrencia,
    taxa,
    media_movel_3m,
    variacao_yoy_perc
FROM isp.gold.fact_monthly_crime_rates
WHERE ocorrencia = 'letalidade_violenta'
ORDER BY reference_date DESC;

```

**Annual Comparative Summary**:

```sql
SELECT 
    ano,
    categoria_ocorrencia,
    ocorrencia,
    taxa_media_mensal,
    taxa_acumulada_anual
FROM isp.gold.agg_annual_crime_summary
WHERE ano >= 2020
ORDER BY ano DESC, taxa_acumulada_anual DESC;

```