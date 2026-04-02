from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from extract import extract_dataset
from transform import transform_dataset1, transform_dataset2

load_dotenv(dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env'))

def load_to_postgres(df, table_name):
    print(f"[INFO] Loading data into {table_name}...")

    engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

    try:
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',  # change to 'append' later
            index=False
        )

        print(f"[INFO] Data loaded successfully into {table_name}")

    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")

if __name__ == "__main__":
    extracted_df_1 = extract_dataset("./data/raw/global_ecommerce_sales.csv")
    transformed_df_1 = transform_dataset1(extracted_df_1)
    load_to_postgres(transformed_df_1, "fact_sales")
    
    extracted_df_2_customers = extract_dataset("./data/raw/df_Customers.csv")
    extracted_df_2_orders = extract_dataset("./data/raw/df_Orders.csv")
    extracted_df_2_orderitems = extract_dataset("./data/raw/df_OrderItems.csv")
    extracted_df_2_products = extract_dataset("./data/raw/df_Products.csv")
    transformed_df_2 = transform_dataset2(extracted_df_2_orders, extracted_df_2_orderitems, extracted_df_2_customers, extracted_df_2_products)
    load_to_postgres(transformed_df_2, "fact_sales_extended")