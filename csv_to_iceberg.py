import pandas as pd
import trino
import argparse
import yaml
import json
import os
from datetime import datetime

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

# Argument parsing with default values
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