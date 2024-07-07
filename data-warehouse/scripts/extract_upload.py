import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from gcs_utils import upload_to_gcs
import argparse

BUCKET = os.environ.get("GCP_GCS_BUCKET", "sp500-tracker-terrabucket")

def wikipedia_to_gcs(bucket_name):
    """
    Scrapes S&P 500 ticker data from Wikipedia and uploads it to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket to upload the data to.
    """
    # Get s&p500 info from wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Get all the data of S&P 500 tickers in a dataframe
    sp500_tickers = pd.read_html(url)[0]

    # Get current date
    date = datetime.now().strftime('%Y%m%d')

    # Save to local csv
    local_file_path = 'sp500_wiki_data.csv'
    sp500_tickers.to_csv(local_file_path, index=False)

    # Upload to GCS
    upload_to_gcs(bucket_name, f'sp500_wiki_data_{date}.csv', 'sp500_wiki_data.csv')


def yfinance_to_gcs(bucket_name, start_date, end_date):

    # Get s&p500 info from wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Get all the data of S&P 500 tickers in a dataframe
    sp500_tickers = pd.read_html(url)[0]

    # Get the list of S&P 500 tickers
    sp500_symbols = sp500_tickers.Symbol.to_list()

    # Download data for each ticker
    sp500_data = {}
    for symbol in sp500_symbols:
        try:
            data = yf.download(symbol, start=start_date, end=end_date)
            sp500_data[symbol] = data
            print(f"Downloaded data for {symbol}")
        except Exception as e:
            print(f"Error downloading data for {symbol}: {str(e)}")
            

    # Concatenate data for all tickers into a single DataFrame
    sp500_df = pd.concat(sp500_data.values(), keys=sp500_data.keys(), names=['Ticker'])

    sp500_df.reset_index(inplace=True)  # Ensure 'Ticker' and 'Date' are columns
    sp500_df['Date'] = sp500_df['Date'].dt.strftime('%Y-%m-%d')  # Format 'Date' column

    # Add ingestion timestamp column
    ingestion_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sp500_df['ingestion_timestamp'] = ingestion_timestamp

    # Save to local CSV
    date = datetime.now().strftime('%Y%m%d')
    local_file_path = f'sp500_finance_data_{date}.csv.gz'
    sp500_df.to_csv(local_file_path)

    # Upload to GCS
    object_name = f'sp500_finance_data_{date}.csv.gz'
    upload_to_gcs(bucket_name, object_name, local_file_path)

# Get yesterday's date
yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def main():
    parser = argparse.ArgumentParser(description="Run financial data scripts with specified parameters.")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for wikipedia_to_gcs
    parser_wiki = subparsers.add_parser('wikipedia_to_gcs', help="Run the wikipedia_to_gcs function.")
    parser_wiki.add_argument('--bucket', type=str, default=BUCKET, help='The GCS bucket name.')

    # Subparser for yfinance_to_gcs
    parser_yfinance = subparsers.add_parser('yfinance_to_gcs', help="Run the yfinance_to_gcs function.")
    parser_yfinance.add_argument('--bucket', type=str, default=BUCKET, help='The GCS bucket name.')
    parser_yfinance.add_argument('--start_date', type=str, required=True, help='Start date for downloading financial data (format: YYYY-MM-DD).')
    parser_yfinance.add_argument('--end_date', type=str, help='End date for downloading financial data (format: YYYY-MM-DD).')

    args = parser.parse_args()

    if args.command == 'wikipedia_to_gcs':
        wikipedia_to_gcs(args.bucket)
    elif args.command == 'yfinance_to_gcs':
        end_date = args.end_date if args.end_date else (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        yfinance_to_gcs(args.bucket, args.start_date, end_date)

if __name__ == "__main__":
    main()




        
        
