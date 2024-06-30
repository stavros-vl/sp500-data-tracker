-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `sp500-data-tracker.sp500_tables.sp500_finance_data_20240420`
OPTIONS (
  format = "CSV",
  uris = ['gs://sp500-tracker-terrabucket/sp500_finance_data_20240420.csv.gz']
);

-- Creating external table for wiki data
CREATE OR REPLACE EXTERNAL TABLE `sp500-data-tracker.sp500_tables.sp500_wiki_data_v1`
OPTIONS (
  format = "CSV",
  uris = ['gs://sp500-tracker-terrabucket/sp500_wiki_data_20240420.csv.gz']
);

-- Create standard table for wiki data from CSV in GCS
LOAD DATA OVERWRITE `sp500-data-tracker.sp500_tables.sp500_wiki_data_v2`
FROM FILES (
  format = 'CSV',
  uris = ['gs://sp500-tracker-terrabucket/sp500_wiki_data_20240420.csv.gz']
);


