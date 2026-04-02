CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    customer_segment TEXT,
    country TEXT,
    region TEXT
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    product_category TEXT
);

CREATE TABLE dim_date (
    order_date DATE PRIMARY KEY,
    year INTEGER,
    month INTEGER
);

CREATE TABLE fact_sales (
    order_id TEXT,
    customer_id INTEGER,
    product_id INTEGER,
    order_date DATE,
    quantity INTEGER,
    price REAL,
    total_amount REAL,
    profit REAL,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);