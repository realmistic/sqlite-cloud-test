# SQLite Cloud with yfinance

This project demonstrates how to use SQLite Cloud with yfinance to fetch, store, and query financial data using pandas.

## Setup

1. Make sure you have Python installed (3.8+ recommended)
2. The project uses a virtual environment (.venv) with the following packages:
   - yfinance: For fetching financial data
   - sqlitecloud: For connecting to SQLite Cloud
   - pandas: For data manipulation and analysis
   - python-dotenv: For loading environment variables

## Configuration

1. Create a virtual environment:
   ```
   python -m venv .venv  # On macOS/Linux/Windows
   ```

2. Activate the virtual environment:
   ```
   source .venv/bin/activate  # On macOS/Linux
   .venv\Scripts\activate     # On Windows
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Copy `.env.sample` to `.env` and set your SQLite Cloud connection URL:
   ```
   cp .env.sample .env
   ```

5. Edit the `.env` file with your actual credentials:
   ```
   SQLITECLOUD_URL=sqlitecloud://username:password@host:port/database
   ```

## Files

- `requirements.txt`: Lists the required packages
- `db_connection.py`: Contains functions for connecting to SQLite Cloud and reading/writing data using pandas
- `example_usage.py`: Demonstrates how to use the functionality
- `.env.sample`: Template for environment variables (copy to `.env` and add your credentials)
- `.gitignore`: Specifies files that should be ignored by Git (including `.env` with credentials)

## Usage

### Basic Database Operations

```python
from db_connection import read_data, write_data
import pandas as pd

# Read data from the database
df = read_data("SELECT * FROM stocks LIMIT 10")

# Write data to the database
data = {
    'symbol': ['AAPL', 'MSFT', 'GOOGL'],
    'price': [150.25, 300.75, 2500.50]
}
df = pd.DataFrame(data)
write_data(df, 'stocks')
```

### Running the Example

```bash
python example_usage.py
```

This will:
1. Fetch stock data for Apple, Microsoft, and Google
2. Save the data to the SQLite Cloud database
3. Query and display the data from the database

## Functions

### `get_db_connection()`

Establishes a connection to the SQLite Cloud database using the URL from the `.env` file.

### `read_data(query)`

Executes a SQL query and returns the results as a pandas DataFrame.

### `write_data(df, table_name, if_exists='replace')`

Writes a pandas DataFrame to a table in the database. The `if_exists` parameter controls what happens if the table already exists:
- 'replace': Drop the table and create a new one (default)
- 'append': Add data to the existing table
- 'fail': Raise an error if the table exists

## Implementation Details

This project uses:
- sqlitecloud library for direct connection to SQLite Cloud
- pandas for data manipulation and conversion between SQL and DataFrames
- yfinance for fetching financial data
- python-dotenv for environment variable management

## Version Control

The `.gitignore` file is configured to exclude:
- Virtual environment directories (`.venv/`, `venv/`)
- Python cache files (`__pycache__/`, `*.pyc`)
- Environment variables file (`.env`) to prevent committing sensitive credentials
