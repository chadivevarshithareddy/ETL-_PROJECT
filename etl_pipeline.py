import pandas as pd
import sqlite3

# Extract: Load data from CSV
def extract_data(file_path):
    return pd.read_csv(file_path)

# Transform: Process the data
def transform_data(df):
    df.dropna(inplace=True)  # Remove missing values
   # df.dropna(how='all', inplace=True) #If You Want to Remove Rows Only When All Values Are Missing
    df['Total'] = df['Quantity'] * df['Price']  # Add a new column for total sales
    return df

# Load: Save data to SQLite
def load_data(df, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql('sales', conn, if_exists='replace', index=False)
    conn.close()

def main():
    # File and database paths
    csv_file = 'sales_data.csv'
    sqlite_db = 'sales_data.db'

    # Run ETL
    df = extract_data(csv_file)
    df = transform_data(df)
    load_data(df, sqlite_db)
    print('ETL process completed successfully!')

if __name__ == '__main__':
    main()
