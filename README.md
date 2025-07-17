# BigQuery ETL & Monitoring Framework

This project is a modular Python-based ETL framework built for working with **Google BigQuery**. It covers **data ingestion**, **logging**, **table validation**, and **alerting**” all designed to monitor the health and integrity of your datasets.

The project supports dynamic, config-based execution, and is easy to extend or adapt to new datasets, schemas, or monitoring logic.

## Project Highlights

- Load and validate datasets into BigQuery using Python
- Monitor table freshness and row counts
- Detect anomalies or failures in ETL runs via logs
- Dynamic SQL-based KPI and table health checks
- Modular structure powered by JSON config files

##  Main Components

### Core Scripts

| Script                | Purpose                                                                 |
|-----------------------|-------------------------------------------------------------------------|
|  [my_etl.py](./bi/jobs/my_etl/my_etl.py) | Main ETL pipeline: extract, load, and log data into BigQuery            |
| `logs_monitoring.py`  | Checks ETL logs and flags issues or missed jobs                         |
| `table_monitoring.py` | Verifies table freshness and row counts                                 |
| `table_validation.py` | Validates schema and expected structure of BigQuery tables              |
| `dataset_validation.py` | Cross-table consistency checks                                         |
| `kpis_monitoring.py`  | Runs custom SQL KPIs and generates alerts if thresholds are breached    |


## How to Run

Each script accepts arguments like `--etl-name`, `--etl-action`, and `--days-back`. Example usage:

```bash
python bi/jobs/my_etl/my_etl.py --etl-name animals --etl-action daily --days-back 2
python bi/jobs/my_etl/logs_monitoring.py --etl-name log --etl-action daily
```

## Folder Structure

```
bi/
jobs/
my_etl/
config/         # JSON config files for datasets, KPIs, logs, etc.
queries/        # SQL templates for alerting
scheduler/      # Shell scripts for automating jobs
utilities/      # Helper functions and formatting tools
*.py            # Main ETL and monitoring scripts
temp/
data/               # Input files (from API or cloud)
logs/               # Monitoring results, alerts, log messages
```

