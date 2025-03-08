# CsvToIceberg
inserting the records from csv file to iceberg table 

## Table of Contents
1. [Introduction](#introduction)
2. [Dependencies](#dependencies)
3. [Line-by-Line Code Explanation](#line-by-line-code-explanation)
   - [Import Statements](#import-statements)
   - [Configuration Loading Function](#configuration-loading-function)
   - [Table Column Retrieval Function](#table-column-retrieval-function)
   - [Main Data Insertion Function](#main-data-insertion-function)
   - [Command-Line Interface](#command-line-interface)
4. [Configuration File Structure](#configuration-file-structure)
5. [Usage Examples](#usage-examples)
6. [Data Type Handling](#data-type-handling)
7. [Error Handling and Edge Cases](#error-handling-and-edge-cases)
8. [Performance Considerations](#performance-considerations)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [Appendix: SQL Queries](#appendix-sql-queries)

## Introduction

This script provides a robust solution for loading CSV data into Trino/Iceberg tables. The key distinguishing feature is that it retrieves and uses the target table's column structure directly from Trino's information schema, ensuring that data is correctly mapped regardless of CSV headers. The script supports configuration through external files and provides various command-line options for flexibility.

## Dependencies

The script requires the following Python packages:
- `pandas`  
- `trino`  
- `pyyaml` 
- `json` 
- `argparse` 
- `os` 
- `datetime` 

Installation command:
```bash
pip install pandas trino pyyaml
```

## Line-by-Line Code Explanation

### Import Statements

```python
import pandas as pd
import trino
import argparse
import yaml
import json
import os
from datetime import datetime
```

- `pandas as pd`: Used for reading and manipulating CSV data with DataFrames
- `trino`: Python client for connecting to The database
- `argparse`: For parsing command-line arguments
- `yaml`: For reading YAML configuration files
- `json`: For reading JSON configuration files
- `os`: For file path operations and existence checks
- `datetime`: For handling date conversions

### Configuration Loading Function

```python
def load_config(config_path):
    """Load connection configuration from YAML or JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    file_ext = os.path.splitext(config_path)[1].lower()
    
    if file_ext == '.yaml' or file_ext == '.yml':
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    elif file_ext == '.json':
        with open(config_path, 'r') as file:
            return json.load(file)
    else:
        raise ValueError(f"Unsupported config file format: {file_ext}. Use .yaml, .yml, or .json")
```

- **Line 1-2**: Function definition with docstring
- **Line 3-4**: Checks if the config file exists using `os.path.exists()`, raising a `FileNotFoundError` if not found
- **Line 6**: Extracts the file extension using `os.path.splitext()` and converts to lowercase
- **Line 8-10**: For YAML files (`.yaml` or `.yml` extensions):
  - Opens the file in read mode
  - Uses `yaml.safe_load()` to parse the YAML content into a Python dictionary
- **Line 11-13**: For JSON files (`.json` extension):
  - Opens the file in read mode
  - Uses `json.load()` to parse the JSON content into a Python dictionary
- **Line 14-15**: Raises a `ValueError` for unsupported file formats

### Table Column Retrieval Function

```python
def get_table_columns(cursor, table_name):
    """Get column names from the existing table."""
    # Parse table_name to extract schema if provided (schema.table_name format)
    if '.' in table_name:
        schema, table = table_name.split('.')
    else:
        # Use the connection's default schema
        schema = None
        table = table_name
    
    # Query to get columns from the table
    schema_condition = f"AND table_schema = '{schema}'" if schema else ""
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table}' {schema_condition}
    ORDER BY ordinal_position
    """
    
    cursor.execute(query)
    columns = [row[0] for row in cursor.fetchall()]
    
    if not columns:
        raise ValueError(f"Table {table_name} not found or has no columns")
    
    return columns
```

- **Line 1-2**: Function definition with docstring
- **Line 4-9**: Table name parsing logic:
  - **Line 4-5**: If the table name contains a period (`.`), it splits it into schema and table components
  - **Line 6-9**: If no schema is specified, sets schema to None and uses the table name as-is
- **Line 12**: Creates a schema condition string for the SQL query, only if a schema was specified
- **Line 13-18**: Constructs a SQL query to retrieve column names from Trino's information_schema:
  - Selects column_name from information_schema.columns
  - Filters by the table name
  - Adds a schema condition if a schema was specified
  - Orders columns by their ordinal_position to maintain the correct column order
- **Line 20**: Executes the SQL query using the provided cursor
- **Line 21**: Uses list comprehension to extract column names from each row of the result set
- **Line 23-24**: Raises a ValueError if no columns were found (empty list)
- **Line 26**: Returns the list of column names

### Main Data Insertion Function

```python
def insert_csv_to_iceberg(csv_path, table_name, config_path):
    """Insert CSV data into an Iceberg table using only the table's columns."""
    # Load connection configuration
    config = load_config(config_path)
    trino_config = config.get('trino', {})
    
    # Load CSV data with headers (but we'll ignore the header names)
    df = pd.read_csv(csv_path)
    
    # Trino connection settings from config
    conn = trino.dbapi.connect(
        host=trino_config.get('host', 'localhost'),
        port=trino_config.get('port', 8080),
        user=trino_config.get('user', 'trino'),
        catalog=trino_config.get('catalog', 'iceberg'),
        schema=trino_config.get('schema', 'default'),
    )
    cursor = conn.cursor()
    
    # Get column names from the table
    table_columns = get_table_columns(cursor, table_name)
    print(f"Using table columns: {table_columns}")
    
    # Ensure we don't try to use more columns than exist in the CSV
    if len(table_columns) > len(df.columns):
        print(f"Warning: Table has {len(table_columns)} columns but CSV only has {len(df.columns)} columns")
        table_columns = table_columns[:len(df.columns)]
    
    # Rename CSV columns to be positional (0, 1, 2, ...) for consistent handling
    df.columns = range(len(df.columns))
    
    # Convert date columns if needed
    for col_idx, col_name in enumerate(table_columns):
        if 'date' in col_name.lower() and col_idx < len(df.columns):
            try:
                df[col_idx] = pd.to_datetime(df[col_idx]).dt.date
            except:
                # Not a valid date column or conversion failed, skip
                pass
    
    # Prepare the column parts for the SQL query
    table_column_str = ', '.join([f'"{col}"' for col in table_columns])
    placeholder_str = ', '.join(['?' for _ in table_columns])
    
    # Insert data into Iceberg table
    rows_inserted = 0
    for _, row in df.iterrows():
        # Extract values for the columns we're using
        values = [row[i] if i < len(row) else None for i in range(len(table_columns))]
        
        query = f"""
        INSERT INTO {table_name} ({table_column_str})
        VALUES ({placeholder_str})
        """
        cursor.execute(query, tuple(values))
        rows_inserted += 1
    
    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Data from {csv_path} inserted into {table_name} successfully!")
    print(f"Inserted {rows_inserted} rows with {len(table_columns)} columns")
```

- **Line 1-2**: Function definition with docstring
- **Line 4-5**: Loads the configuration using the `load_config()` function and extracts the Trino-specific configuration
  - Uses `.get('trino', {})` to provide an empty dict default if 'trino' key doesn't exist
- **Line 8**: Loads the CSV file into a pandas DataFrame using `pd.read_csv()`
  - Includes headers but their names will be ignored later
- **Line 11-17**: Establishes a connection to Trino using configuration values:
  - Uses `.get()` with defaults for each connection parameter:
    - host (default: 'localhost')
    - port (default: 8080)
    - user (default: 'trino')
    - catalog (default: 'iceberg')
    - schema (default: 'default')
- **Line 18**: Creates a cursor for executing SQL commands
- **Line 21-22**: Retrieves table columns using the `get_table_columns()` function and prints them
- **Line 25-27**: Handles cases where the table has more columns than the CSV:
  - Prints a warning message
  - Truncates `table_columns` to match the number of columns in the CSV
- **Line 30**: Renames CSV DataFrame columns to numeric indices (0, 1, 2, ...) for position-based mapping
- **Line 33-38**: Attempts to convert date columns:
  - Iterates through table columns with their indices
  - Checks if the column name contains 'date' (case-insensitive)
  - Verifies the column index is within range of the DataFrame
  - Uses `pd.to_datetime()` and `.dt.date` to convert to Python date objects
  - Silently skips conversion if it fails
- **Line 41-42**: Prepares SQL query components:
  - `table_column_str`: Comma-separated list of quoted column names
  - `placeholder_str`: Comma-separated list of question marks for parameterized query
- **Line 45-55**: Inserts data row by row:
  - Iterates through DataFrame rows
  - Extracts values corresponding to table columns, using None for missing values
  - Constructs an INSERT query with the table name, columns, and placeholders
  - Executes the query with the values as a tuple
  - Increments the rows_inserted counter
- **Line 58-60**: Finalizes the database operation:
  - Commits all pending transactions
  - Closes the cursor and connection objects
- **Line 62-63**: Prints success messages with insertion statistics

### Command-Line Interface

```python
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert CSV data into an Iceberg table in Trino using table schema.")
    parser.add_argument("csv_file", nargs="?", default="purchases.csv", 
                        help="Path to the CSV file (default: purchases.csv)")
    parser.add_argument("table_name", nargs="?", default="stocks_transactions", 
                        help="Target Iceberg table name (default: stocks_transactions)")
    parser.add_argument("--config", "-c", default="trino_config.yaml", 
                        help="Path to configuration file (default: trino_config.yaml)")
    
    args = parser.parse_args()
    
    insert_csv_to_iceberg(args.csv_file, args.table_name, args.config)
```

- **Line 1**: Main execution guard ensuring this code only runs when the script is executed directly
- **Line 2**: Creates an ArgumentParser object with a description of the script's purpose
- **Line 3-4**: Adds the `csv_file` positional argument:
  - `nargs="?"` makes it optional
  - `default="purchases.csv"` sets the default value
  - `help` provides usage information
- **Line 5-6**: Adds the `table_name` positional argument:
  - `nargs="?"` makes it optional
  - `default="stocks_transactions"` sets the default value
  - `help` provides usage information
- **Line 7-8**: Adds the `--config` (or `-c`) optional argument:
  - `default="trino_config.yaml"` sets the default value
  - `help` provides usage information
- **Line 10**: Parses command-line arguments into the `args` object
- **Line 12**: Calls the main function with the parsed arguments

## Configuration File Structure

### YAML Format

```yaml
trino:
  host: localhost
  port: 8080
  user: your_username
  catalog: iceberg
  schema: your_database
  # Optional parameters:
  # http_scheme: https
  # auth: None, KERBEROS, CERTIFICATE, etc.
  # password: your_password (if applicable)
  # verify: true/false (for SSL verification)
```

### JSON Format

```json
{
  "trino": {
    "host": "localhost",
    "port": 8080,
    "user": "your_username",
    "catalog": "iceberg",
    "schema": "your_database"
  }
}
```

Required configuration keys:
- `trino.host`: Hostname or IP address of the Trino server
- `trino.port`: Port number for the Trino server (typically 8080)
- `trino.user`: Username for Trino connection
- `trino.catalog`: Catalog name (e.g., 'iceberg')
- `trino.schema`: Default schema/database name

Optional configuration keys that can be added if needed:
- `trino.http_scheme`: HTTP protocol to use (http or https)
- `trino.auth`: Authentication type
- `trino.password`: Password (if required)
- `trino.verify`: Whether to verify SSL certificates (boolean)

## Usage Examples

### Basic Usage

```bash
python csv_to_iceberg.py purchases.csv stocks_transactions
```

This will:
1. Load data from `purchases.csv`
2. Insert into `stocks_transactions` table
3. Use the default config file (`trino_config.yaml`)

### Specifying Config File

```bash
python csv_to_iceberg.py sales_data.csv sales_records --config production_trino.json
```

This will:
1. Load data from `sales_data.csv`
2. Insert into `sales_records` table
3. Use connection settings from `production_trino.json`

### Using Defaults

```bash
python csv_to_iceberg.py
```

This will:
1. Load data from the default CSV file (`purchases.csv`)
2. Insert into the default table (`stocks_transactions`)
3. Use the default config file (`trino_config.yaml`)

### Using Schema-Qualified Table Name

```bash
python csv_to_iceberg.py orders.csv retail.customer_orders
```

This will:
1. Load data from `orders.csv`
2. Insert into the `customer_orders` table in the `retail` schema
3. Use the default config file

## Data Type Handling

The script handles data types in the following ways:

1. **Date Columns**:
   - Columns with names containing "date" (case-insensitive) are automatically converted to date objects
   - Uses `pd.to_datetime()` followed by `.dt.date` to convert to Python `datetime.date` objects
   - Failed conversions are silently skipped (column remains as-is)

2. **String Columns**:
   - CSV string data is passed as-is to Trino
   - Trino handles the conversion to VARCHAR or other string types

3. **Numeric Columns**:
   - Pandas automatically detects numeric columns in the CSV
   - These are passed as Python numeric types to Trino
   - Trino handles the conversion to appropriate numeric types (INTEGER, DECIMAL, etc.)

4. **Null Values**:
   - CSV null values (`NA`, `N/A`, empty fields) are converted to Python `None`
   - These are passed as SQL NULL values to Trino

5. **Missing Data**:
   - If the CSV has fewer columns than the table, NULL values are used for missing columns
   - If the table has fewer columns than the CSV, extra CSV columns are ignored

## Error Handling and Edge Cases

The script handles various error conditions and edge cases:

1. **Missing Configuration File**:
   - `FileNotFoundError` is raised if the specified config file doesn't exist
   - Error message includes the path that was not found

2. **Unsupported Configuration Format**:
   - `ValueError` is raised if the config file extension is not `.yaml`, `.yml`, or `.json`
   - Error message suggests supported formats

3. **Table Not Found**:
   - `ValueError` is raised if the specified table doesn't exist or has no columns
   - Error message includes the table name that was not found

4. **Column Count Mismatch**:
   - Warning message is printed if the table has more columns than the CSV
   - Table columns are truncated to match CSV column count
   - Does not halt execution

5. **Date Conversion Failures**:
   - Date conversion exceptions are caught and silently ignored
   - The original data is preserved in these cases
   - Does not halt execution

6. **Database Connection Issues**:
   - Trino connection errors are not explicitly caught
   - These will propagate to the caller with the original error message

7. **SQL Execution Errors**:
   - SQL errors during execution are not explicitly caught
   - These will propagate to the caller with the original error message

## Performance Considerations

1. **Row-by-Row Insertion**:
   - The script inserts data row-by-row, which is not optimal for large datasets
   - For very large CSV files, consider implementing batch inserts

2. **Memory Usage**:
   - The entire CSV is loaded into memory using pandas
   - For extremely large files, you might need to implement chunked reading

3. **Date Conversion**:
   - Date conversion is done for all rows at once using pandas vectorized operations
   - This is efficient but assumes all values in a "date" column are valid dates

4. **Connection Management**:
   - A single connection is used for the entire operation
   - The connection is properly closed after all operations are complete

## Troubleshooting Guide

### Connection Issues

- **Error**: `Connection refused`
  - **Solution**: Verify host and port in config file
  - **Check**: Is Trino server running? Is the port correct?

- **Error**: `Authentication failed`
  - **Solution**: Check username/password in config
  - **Check**: Do you have the correct access permissions?

### Table Schema Issues

- **Error**: `Table X not found or has no columns`
  - **Solution**: Verify table name and schema
  - **Check**: Does the table exist? Is the catalog/schema correct?

- **Warning**: `Table has X columns but CSV only has Y columns`
  - **Solution**: Add missing columns to CSV or adjust table schema
  - **Check**: Does the CSV structure match the table structure?

### Data Type Issues

- **Error**: `Cannot cast X to Y`
  - **Solution**: Verify data types in CSV match table schema
  - **Check**: Are there non-numeric values in numeric columns?

- **Error**: `Invalid date format`
  - **Solution**: Check date formats in CSV
  - **Check**: Are dates in a format pandas can parse?

### CSV Format Issues

- **Error**: `Error tokenizing data`
  - **Solution**: Check CSV format, delimiters, and quotes
  - **Check**: Is the CSV properly formatted?

- **Error**: `Unnamed column X`
  - **Solution**: Verify CSV has headers if expected
  - **Check**: Does the CSV have the expected number of columns?

## Appendix: SQL Queries

### Column Retrieval Query

```sql
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = '{table}' [AND table_schema = '{schema}']
ORDER BY ordinal_position
```

This query:
- Retrieves column names from Trino's information_schema
- Filters by table name and optionally schema name
- Orders columns by their defined position in the table

### Insert Query

```sql
INSERT INTO {table_name} ({column1}, {column2}, ...)
VALUES (?, ?, ...)
```

This query:
- Inserts data into the specified table
- Uses parameterized queries with `?` placeholders for values
- Each placeholder is replaced with the corresponding value from the CSV
