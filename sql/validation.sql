SELECT order_id, COUNT(*)
FROM fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT *
FROM fact_sales
WHERE customer_id IS NULL OR product_id IS NULL;

SELECT COUNT(*) FROM fact_sales;