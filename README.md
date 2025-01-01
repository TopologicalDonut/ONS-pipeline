# ONS CPI Data Pipeline

A Python-based pipeline for extracting, transforming, and loading Consumer Price Index (CPI) data from the UK Office for National Statistics (ONS).

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#overview">Overview</a>
    </li>
    <li>
      <a href="#setup-and-usage">Setup and Usage</a>
    </li>
    <li>
      <a href="#project-structure">Project Structure</a>
    </li>
    <li>
      <a href="#features">Features</a>
    </li>
    <li>
      <a href="#data-model">Data Model</a>
    </li>
    <li>
      <a href="#configuration">Configuration</a>
    </li>
    <li>
      <a href="#validation-rules">Validation Rules</a>
    </li>
    <li>
      <a href="#error-handling">Error Handling</a>
    </li>
  </ol>
</details>

## Overview

This pipeline:
1. Scrapes CPI item indices data from the ONS website
2. Processes and validates the data
3. Loads it into a DuckDB database for analysis

## Setup and Usage

Currently only an `environment.yml` is available for conda users. A `requirements.txt` will be made soon for pip + virtualenv.

1. Create and activate the conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate ons-cpi
   ```

2. Run the pipeline:
   ```bash
   python -m src.run_pipeline
   ```

When the pipeline runs, it creates necessary directories for data storage, database files, and logs.

## Project Structure

```
.
├─ .gitignore
├─ README.md
├─ environment.yml        
├─ run_pipeline.py            
├─ src
│  ├─ __init__.py
│  ├─ database.py
│  ├─ logger.py
│  ├─ processor.py
│  ├─ reader.py
│  └─ scraper.py
└─ tests
   ├─ __init__.py
   ├─ test_ONS_database.py
   └─ test_ONS_validation.py
```

## Features

- **Smart Web Scraping**: 
  - Rate-limited requests with automatic backoff
  - Handles both current and historical data files
  - Deduplicates data across yearly archives and individual files
  - Supports CSV, XLSX, and ZIP formats

- **Robust Data Processing**:
  - Validates dates, item IDs, descriptions, and index values
  - Configurable uniqueness constraints on column combinations (e.g. no duplicate date/item_id pairs)
  - Saves validation issues for review
  - Uses Polars for efficient data manipulation

- **Database Storage**:
  - Uses DuckDB for local analytics
  - Implements tables for items and their corresponding price indices
  - Handles data updates with duplicate detection

## Data Model

The database uses two main tables:

- `items`: Contains CPI item information
  - `item_id` (PRIMARY KEY)
  - `item_desc`

- `cpi_data`: Contains price indices
  - `date`
  - `item_id` (FOREIGN KEY)
  - `item_index`

## Configuration

Configurations can be modified in their respective files:

- `const.py`: Directory paths and project structure
- `scraper.py`: Web scraping parameters (rate limits, file types)
- `processor.py`: Column selection and data validation rules
- `database.py`: Database schema and table configurations

## Validation Rules

The pipeline enforces several default data quality rules:

1. No null values in any required field
2. Dates must be in YYYYMM format and valid
3. Item descriptions must not have leading/trailing whitespace
4. Item indices must be valid non-negative floats
5. No duplicate date/item_id pairs

These rules can be modified for those familiar with polars syntax. Failed validations are logged and saved for review.

## Error Handling

- All errors are logged to `logs/ons_cpi.log`
- Validation issues are saved to `data/validation_problems/`
