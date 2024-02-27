import os

import yfinance as yf
import boto3
import pandas as pd

BUCKET_NAME = os.environ.get('BUCKET_NAME', None)
CANDLE_S3_PREFIX = os.environ.get('CANDLE_S3_PREFIX', None)

s3 = boto3.client('s3')


def main(event, context):
    print(event)
    # Ticker names for Magnificent 7 and NASDAQ index (^IXIC)
    mag_seven_tickers = [
        'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', '^IXIC'
    ]

    # Download data from Yahoo!Finance as pandas dataframe
    multi_ticker_yahoo_df = yf.download(
        tickers=mag_seven_tickers,
        period='max',
        interval='1d'
    )

    # Transform Yahoo!Finance dataframe to candle dataframe
    # to make it inline with Glue table schema
    candle_df = multi_ticker_yahoo_df_to_candle_df(multi_ticker_yahoo_df)

    # Define the s3 key for the parquet file and save it to s3
    candle_df_s3_key = f'{CANDLE_S3_PREFIX}/data.parquet'
    save_df_as_parquet(
        s3_client=s3,
        bucket_name=BUCKET_NAME,
        dataframe=candle_df,
        s3_key=candle_df_s3_key,
        parquet_file_path='/tmp/data.parquet'
    )


def multi_ticker_yahoo_df_to_candle_df(yahoo_df):
    candle_df = yahoo_df.copy(deep=True)
    # yahoo_df has multi-level columns like ('Open', 'AAPL'), ('Open', 'MSFT').
    # We need to extract symbol, which is at index = 1 in multi-level column
    # and set it as a new column

    candle_df = candle_df.stack(level=1)
    candle_df = candle_df.rename_axis(['timestamp', 'symbol']).reset_index()
    # Now we rename the columns to match the Glue table schema
    candle_df.columns = [
        'timestamp', 'symbol', 'adj close',
        'close', 'high', 'low', 'open', 'volume'
    ]

    # Drop the 'adj close' column since it is not in the Glue table schema
    candle_df = candle_df.drop(columns=['adj close'])

    # Convert the timestamp to datetime64[us]
    # and volume to int to match the Glue table schema
    candle_df['timestamp'] = pd.to_datetime(candle_df['timestamp'])
    candle_df['timestamp'] = candle_df['timestamp'].astype('datetime64[us]')
    candle_df['volume'] = candle_df['volume'].astype('int')

    return candle_df


def save_df_as_parquet(
        s3_client,
        bucket_name,
        s3_key,
        dataframe,
        parquet_file_path
):
    if not bucket_name:
        raise ValueError('bucket_name is not set')
    if not s3_key:
        raise ValueError('s3_key is not set')
    _s3_key = s3_key.replace('//', '/')
    dataframe.to_parquet(parquet_file_path, compression='snappy')
    s3_client.upload_file(parquet_file_path, bucket_name, _s3_key)

