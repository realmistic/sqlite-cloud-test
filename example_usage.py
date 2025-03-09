import pandas as pd
import yfinance as yf
from db_connection import read_data, write_data

def fetch_stock_data(symbols, period="1mo"):
    """
    Fetch stock data using yfinance and return as a DataFrame.
    
    Args:
        symbols: List of stock symbols
        period: Time period to fetch (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        
    Returns:
        pandas DataFrame with stock data
    """
    all_data = []
    
    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period)
        
        # Reset index to make Date a column
        hist = hist.reset_index()
        
        # Add symbol column
        hist['Symbol'] = symbol
        
        all_data.append(hist)
    
    # Combine all data
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

def main():
    # Example 1: Fetch stock data and save to database
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    stock_data = fetch_stock_data(symbols, period="1mo")
    
    if not stock_data.empty:
        print(f"\nFetched {len(stock_data)} rows of stock data")
        print(stock_data.head())
        
        # Save to database using pandas to_sql via our write_data function
        write_data(stock_data, 'stocks')
        print("Stock data saved to database")
    
    # Example 2: Read data from database
    try:
        # Read the data we just wrote using pandas read_sql via our read_data function
        result = read_data("SELECT * FROM stocks WHERE Symbol = 'AAPL' ORDER BY Date DESC LIMIT 5")
        print("\nRecent Apple stock data from database:")
        print(result)
        
        # Example query: Calculate average closing price by symbol
        avg_prices = read_data("""
            SELECT Symbol, AVG(Close) as AvgClose 
            FROM stocks 
            GROUP BY Symbol
        """)
        print("\nAverage closing prices:")
        print(avg_prices)
        
    except Exception as e:
        print(f"Error reading from database: {str(e)}")
        print("Make sure you have set the correct SQLITECLOUD_URL in the .env file")

if __name__ == "__main__":
    main()
