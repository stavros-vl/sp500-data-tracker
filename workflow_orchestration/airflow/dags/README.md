# S&P 500 YFinance Data Pipeline

This DAG fetches S&P 500 financial data from Yahoo Finance and uploads it to Google Cloud Storage (GCS). The data is then loaded into a BigQuery table. The pipeline is scheduled to run daily, fetching data for the previous day.

## DAG Overview

### Tasks

1. **yfinance_to_gcs**:
   - Fetches S&P 500 financial data for the previous day from Yahoo Finance.
   - Saves the data to a local CSV file.
   - Uploads the CSV file to a GCS bucket in both the `latest` and `historical` folders.

2. **gcs_to_bq_table**:
   - Loads the CSV file from GCS into a BigQuery table.
   - Appends the new data to the existing table.

### Task Dependencies

- `yfinance_to_gcs` >> `gcs_to_bq_table`

## Data Pipeline Details

### Data Fetching

- The `yfinance_to_gcs` function fetches the S&P 500 data from Yahoo Finance for the previous day.
- The list of S&P 500 companies is scraped from Wikipedia.

### Data Processing

- The fetched data is processed to ensure that the `Volume` field is always treated as a float.
- An `ingestion_timestamp` column is added to the data to record the time when the data was ingested. This can be used for further deduping in case of multiple runs on the same day.

### Data Storage

- The processed data is saved to a local CSV file, which is then compressed into a gzip file.
- The gzip file is uploaded to a GCS bucket in two locations:
  - `latest/sp500_finance_data.csv.gz` (latest data)
  - `historical/YYYYMMDD/sp500_finance_data_YYYYMMDDHHMMSS.csv.gz` (historical data)

### BigQuery Loading

- The CSV file from the `latest` folder in GCS is loaded into a BigQuery table.
- The data is appended to the existing table, allowing for incremental data loading.
