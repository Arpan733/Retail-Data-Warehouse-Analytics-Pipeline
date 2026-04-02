import pandas as pd

def extract_dataset(path):
    try:
        print(f"[INFO] Extracting dataset from {path}")
        df = pd.read_csv(path)
        print(f"[INFO] Dataset extracted with shape: {df.shape}")

        return df
    except Exception as e:
        print(f"[ERROR] Failed to load dataset: {e}")

        return None

# if __name__ == "__main__":
#     # df = extract_dataset("./data/raw/global_ecommerce_sales.csv")
#     df = extract_dataset("./data/raw/df_Orders.csv")
    
#     print("[info] Displaying first few rows:")
#     print(df.head())

#     print("[info] Dataset columns:")
#     print(df.columns)
    
#     print("[info] Dataset info:")
#     df.info()