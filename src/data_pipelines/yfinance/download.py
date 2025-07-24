# generate a mock parquet file with AAPL, MSFT open and close prices from 1st march 2025 to 1st may 2025
# it should be <date>, <open>, <close> format

import pandas as pd
from datetime import datetime, timedelta

# load the entire parquet file in sqlite db

# you can use yfinance to fetch real data

import yfinance as yf


def fetch_from_yfinance(tickers: list[str], start_date, end_date):
    """
    Fetch historical stock data from Yahoo Finance.
    """
    stock_data: list[pd.DataFrame] = []
    for ticker in tickers:
        ticker_data = yf.download(ticker, start=start_date, end=end_date)
        df = pd.DataFrame(ticker_data.to_records())
        df['Ticker'] = ticker
        rename_map = {c: eval(c)[0] for c in df.columns if c.startswith('(')}
        df = df.rename(columns=rename_map).reset_index().sort_values(by='Date')
        stock_data.append(df)

    df = pd.concat(
        stock_data
    )[["Date", "Ticker", "Open", "Close", "High", "Low", "Volume"]].sort_values(by='Date')
    
    return df

def save_parquet(data: pd.DataFrame, filename: str):
    """
    Save DataFrame to a Parquet file.
    """
    print(data)
    data.to_parquet(filename, index=False)

def load_db(filename: str):
    """
    Load data from a Parquet file into a SQLite database.
    """
    import sqlite3

    conn = sqlite3.connect('stock_data.db')
    data = pd.read_parquet(filename)
    data.to_sql('stock_data', conn, if_exists='fail', index=False)
    conn.close()

def read_db():
    """
    Read data from the SQLite database and print it.
    """
    import sqlite3

    db_file_path = 'stock_data.db'
    import os
    
    if not os.path.exists(db_file_path):
        print(f"Database file {db_file_path} does not exist.")
        return
    
    print(f"reading {os.path.abspath(db_file_path)}")
    
    conn = sqlite3.connect('stock_data.db')
    query = "SELECT * FROM stock_data"
    data = pd.read_sql_query(query, conn)
    conn.close()
    
    print(data)

def main():
    data = fetch_from_yfinance(
        tickers=['^SPX'],
        start_date=datetime(2025, 3, 1),
        end_date=datetime(2025, 5, 1)
    )
    print(data.head())

    data["Ticker"] = data["Ticker"].str.replace('^', '')

    print(data.head())
    save_parquet(data, 'index_data.parquet')



if __name__ == "__main__":
    # load_db('stock_data.parquet')
    # read_db();
    main()