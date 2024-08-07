# Workflow Orchestration

Contains the Airflow installation that is used to ingest data from the [yfinance](https://pypi.org/project/yfinance/) API/python package and the [Wikipedia S&P500 list]
(https://en.wikipedia.org/wiki/List_of_S%26P_500_companies.)

High-level flow:

```
(yfinance API) --> DOWNLOAD --> (csv) --> UPLOAD TO GCS --> UPLOAD TO BIGQUERY --> (table in BQ)
```

(web) → DOWNLOAD → (csv) → INGEST → (Postgres)


See the [Airflow page](https://github.com/stavros-vl/sp500-data-tracker/tree/main/workflow_orchestration/airflow) for how to run.


