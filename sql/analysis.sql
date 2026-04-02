SELECT SUM(total_amount) AS total_revenue
FROM fact_sales;

SELECT dc.region, SUM(fs.total_amount) AS revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.region
ORDER BY revenue DESC;

SELECT dp.product_name, SUM(fs.total_amount) AS revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY revenue DESC
LIMIT 10;