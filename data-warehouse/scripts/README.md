# Scripts

This folder contains the following scripts: 
- `gcs_utils`: used to define a utility function for loading data from local to GCS bucket.
- `extract_upload`: contains functions that can be used to upload data from wikipedia and yfinance API.

## How to run

The extract_upload.py contains two functions that are parameterized using [argparse](https://docs.python.org/3/library/argparse.html):

Run using a command like this:

```
python extract_upload.py yfinance_to_gcs --bucket sp500-tracker-terrabucket --start_date 2023-01-01 --end_date 2024-07-06
```