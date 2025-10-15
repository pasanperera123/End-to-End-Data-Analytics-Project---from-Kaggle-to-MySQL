-- It's better to define the table with correct data types and then import the data.
CREATE TABLE IF NOT EXISTS orders_db.orders_tb (
  order_id INT PRIMARY KEY,
  order_date DATE,
  ship_mode VARCHAR(20),
  segment VARCHAR(20),
  country VARCHAR(20),
  city VARCHAR(20),
  state VARCHAR(20),
  postal_code VARCHAR(20),
  region VARCHAR(20),
  category VARCHAR(20),
  sub_category VARCHAR(20),
  product_id VARCHAR(50),
  quantity INT,
  discount DECIMAL(7,2),
  sale_price DECIMAL(7,2),
  profit DECIMAL(7,2)
);

SELECT *
FROM orders_db.orders_tb;

-- 1. Top 10 highest revenue-generating products across the dataset. --
SELECT product_id, SUM(sale_price) AS sales
FROM orders_db.orders_tb
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;

-- 2. Top 10 highest-selling products per region, using window functions and partitioning. --

-- Step 1: Calculate total sales per region and product
WITH cte_sales AS (
SELECT region, product_id, SUM(sale_price) AS sales
FROM orders_db.orders_tb
GROUP BY region, product_id
),
-- Step 2: Rank products within each region based on total sales
cte_ranked AS (
SELECT region, product_id, sales, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) AS rn
FROM cte_sales
)
-- Step 3: Select top 5 products per region
SELECT region, product_id, sales, rn
FROM cte_ranked
WHERE rn <= 5;

-- BY Combining aggregation and ranking, can perform the same query with 1 CTE.
WITH cte_sales AS (
SELECT region, product_id, SUM(sale_price) AS sales, ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(sale_price) DESC) AS rn
FROM orders_db.orders_tb
GROUP BY region, product_id
)
SELECT region, product_id, sales, rn
FROM cte_sales
WHERE rn <= 5;


-- 3. Month-over-month sales comparisons for 2022 vs. 2023, to track growth trends and seasonal performance. --

SELECT DISTINCT YEAR(order_date)
FROM orders_db.orders_tb;

WITH cte_monthly_sales AS(
SELECT YEAR(order_date) AS year, MONTH(order_date) AS month, SUM(sale_price) AS sales
FROM orders_db.orders_tb
GROUP BY year, month
)
SELECT month
,SUM(CASE WHEN year=2022 THEN sales ELSE 0 end) AS sales_2022
,SUM(CASE WHEN year=2023 THEN sales ELSE 0 end) AS sales_2023
FROM cte_monthly_sales
GROUP BY month
ORDER BY month;

-- The first GROUP BY (in the CTE) aggregates daily → monthly sales per year
-- The second GROUP BY (outside) aggregates yearly rows → single monthly comparison row


-- 4. Identifying the month with the highest sales for each category, enabling targeted inventory and marketing decisions. --

WITH cte_category AS (
SELECT category, DATE_FORMAT(order_date, '%Y%m') AS order_year_month, SUM(sale_price) AS sales
FROM orders_db.orders_tb
GROUP BY category, order_year_month
),
cte_ranked AS (
SELECT category, order_year_month, sales, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
FROM cte_category
)
SELECT category, order_year_month, sales, rn
FROM cte_ranked
WHERE rn = 1;

-- 5. Sub-category with the highest profit growth in 2023 compared to 2022, highlighting areas of exceptional performance. --

WITH cte_sales AS(
SELECT sub_category, YEAR(order_date) AS order_year, SUM(sale_price) AS sales
FROM orders_db.orders_tb
GROUP BY sub_category, order_year
)
, cte_A AS (
SELECT sub_category
,SUM(CASE WHEN order_year=2022 THEN sales ELSE 0 end) AS sales_2022
,SUM(CASE WHEN order_year=2023 THEN sales ELSE 0 end) AS sales_2023
FROM cte_sales
GROUP BY sub_category
ORDER BY sub_category
)
SELECT *
,(sales_2023 - sales_2022)*100/sales_2022 AS growth
FROM cte_A
ORDER BY growth DESC
LIMIT 1;














