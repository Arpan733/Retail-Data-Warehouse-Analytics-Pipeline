import pandas as pd
import extract

def transform_dataset1(df):
    print("[INFO] Transforming dataset 1...")
    print(f"[INFO] Dataset shape before transformation: {df.shape}")

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

    print(f"[INFO] Transformation complete: {df.shape}")
    return df

def transform_dataset2(orders, orderitems, customers, products):
    print("[INFO] Transforming dataset 2...")
    print(f"[INFO] Orders shape before join: {orders.shape}")

    orderitems = pd.merge(orderitems, products, on = 'product_id', how = 'left')
    orders = pd.merge(orders, orderitems, on = 'order_id', how = 'left')
    orders = pd.merge(orders, customers, on = 'customer_id', how = 'left')

    print(f"[INFO] Orders shape after join: {orders.shape}")

    orders = orders[[
        'order_id',
        'customer_id',
        'product_id',
        'order_purchase_timestamp',
        'price',
        'product_category_name',
        'customer_city',
        'customer_state'
    ]]

    orders['order_date'] = pd.to_datetime(orders['order_purchase_timestamp'], errors='coerce')

    orders = orders[(orders['price'] > 0)]

    orders.drop_duplicates(subset=['order_id', 'product_id'], inplace=True)
    
    orders.dropna(subset=['order_id', 'price'], inplace=True)
    
    print(f"[INFO] Transformation complete: {orders.shape}")
    return orders

if __name__ == "__main__":
    extracted_df = extract.extract_dataset("./data/raw/global_ecommerce_sales.csv")
    transformed_dataset1 = transform_dataset1(extracted_df)

    extracted_orders = extract.extract_dataset("./data/raw/df_Orders.csv")
    extracted_orderitem = extract.extract_dataset("./data/raw/df_OrderItems.csv")
    extracted_customers = extract.extract_dataset("./data/raw/df_Customers.csv")
    extracted_products = extract.extract_dataset("./data/raw/df_Products.csv")
    transformed_dataset2 = transform_dataset2(extracted_orders, extracted_orderitem, extracted_customers, extracted_products)

    print("[info] Transformed dataset info:")
    transformed_dataset1.info()

    print("[info] Transformed dataset 2 info:")
    transformed_dataset2.info()