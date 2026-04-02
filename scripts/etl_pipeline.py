import argparse
import os
import time

import pandas as pd
import logging
import sqlite3

logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_data():
    logging.info("Loading raw data")
    return pd.read_csv("data/raw/global_ecommerce_sales.csv")


def clean_data(df):
    logging.info("Cleaning data")
    df.rename(columns={
        'Order_ID': 'order_id',
        'Order_Date': 'order_date',
        'Customer_Name': 'customer_name',
        'Customer_Segment': 'customer_segment',
        'Country': 'country',
        'Region': 'region',
        'Product_Category': 'product_category',
        'Product_Name': 'product_name',
        'Quantity': 'quantity',
        'Unit_Price': 'price',
        'Discount_Percent': 'discount',
        'Total_Sales': 'total_amount',
        'Profit': 'profit'
    }, inplace=True)

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    df = df[(df['quantity'] > 0) & (df['price'] > 0)]

    df.drop_duplicates(subset=['order_id', 'product_name'], inplace=True)

    df = df.dropna(subset=['order_id', 'price', 'quantity'])

    return df


def create_tables(df):
    logging.info("Creating dimension and fact tables")

    dim_customer = df[['customer_name', 'customer_segment', 'country', 'region']].drop_duplicates()
    dim_customer['customer_id'] = dim_customer.index + 1

    dim_product = df[['product_name', 'product_category']].drop_duplicates()
    dim_product['product_id'] = dim_product.index + 1

    df = df.merge(dim_customer, on=['customer_name', 'customer_segment', 'country', 'region'], how='left')
    df = df.merge(dim_product, on=['product_name', 'product_category'], how='left')

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    dim_date = df[['order_date']].drop_duplicates()
    dim_date['Year'] = dim_date['order_date'].dt.year
    dim_date['Month'] = dim_date['order_date'].dt.month

    fact_sales = df[['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'price', 'total_amount', 'profit']]

    return dim_customer, dim_product, dim_date, fact_sales


def save_data(dim_customer, dim_product, dim_date, fact_sales):
    logging.info("Saving processed data")

    dim_customer.to_csv("data/processed/dim_customer.csv", index=False)
    dim_product.to_csv("data/processed/dim_product.csv", index=False)
    dim_date.to_csv("data/processed/dim_date.csv", index=False)
    fact_sales.to_csv("data/processed/fact_sales.csv", index=False)

def create_schema():
    logging.info("Creating database schema")

    conn = sqlite3.connect("data/processed/retail.db")
    cursor = conn.cursor()

    with open("sql/schema.sql", "r") as file:
        schema_sql = file.read()

    cursor.executescript(schema_sql)

    conn.commit()
    conn.close()

def load_to_sqlite(dim_customer, dim_product, dim_date, fact_sales):
    logging.info("Loading data into SQLite database")

    conn = sqlite3.connect("data/processed/retail.db")

    dim_customer.to_sql("dim_customer", conn, if_exists="append", index=False)
    dim_product.to_sql("dim_product", conn, if_exists="append", index=False)
    dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
    fact_sales.to_sql("fact_sales", conn, if_exists="append", index=False)

    conn.close()

def validate_data(df):
    logging.info("Starting data validation")

    if df.shape[0] == 0:
        raise ValueError("No data available after cleaning")

    if df['order_id'].isnull().sum() > 0:
        raise ValueError("Null values found in order_id")

    if (df['quantity'] <= 0).any():
        raise ValueError("Invalid quantity detected")

    if (df['price'] <= 0).any():
        raise ValueError("Invalid price detected")

    duplicates = df.duplicated(subset=['order_id', 'product_name']).sum()

    if duplicates > 0:
        logging.warning(f"{duplicates} duplicate records found")

    logging.info("Data validation passed")

def schedule_pipeline(interval_seconds = 600):
    while True:
        try:
            logging.info("Scheduled run triggered")
            run_pipeline()
        except Exception as e:
            logging.error(f"Scheduled run failed: {e}")

        time.sleep(interval_seconds)

def run_pipeline():
    db_name = "data/processed/retail.db"

    if os.path.exists(db_name):
        logging.info("Existing database found. Removing for fresh load.")
        os.remove(db_name)
        
    df = load_data()
    df = clean_data(df)
    validate_data(df)

    dim_customer, dim_product, dim_date, fact_sales = create_tables(df)
    save_data(dim_customer, dim_product, dim_date, fact_sales)
    create_schema()
    load_to_sqlite(dim_customer, dim_product, dim_date, fact_sales)

    logging.info("ETL Pipeline Completed Successfully")

def main():
    try:
        logging.info("ETL Pipeline Started")

        parser = argparse.ArgumentParser()
        parser.add_argument("--run", action="store_true")
        parser.add_argument("--schedule", action="store_true")

        args = parser.parse_args()

        if args.schedule:
            logging.info("Pipeline triggered via scheduler")
            schedule_pipeline(10)
        elif args.run:
            logging.info("Pipeline triggered manually")
            run_pipeline()
            logging.info("ETL Pipeline Completed Successfully")
        else:
            logging.info("No action taken. Use --run to execute the pipeline.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()