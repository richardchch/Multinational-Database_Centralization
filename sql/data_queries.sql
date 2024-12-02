-- Query 1: Most Physical Stores by Country
-- Fetch the top 3 countries with the most physical stores
SELECT country_code, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY COUNT(*) DESC
LIMIT 3;

-- Query 2: Most Physical Stores by Locality
-- Fetch the top 7 localities with the most physical stores
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- Query 3: Total Sales by Month
-- Calculate total sales (sum of product_price * product_quantity) by month
SELECT 
      SUM(p.product_price * o.product_quantity) AS total_sales,
      dt.month
FROM orders_table o
JOIN dim_products p ON p.product_code = o.product_code
JOIN dim_date_times dt ON dt.date_uuid = o.date_uuid
GROUP BY dt.month
ORDER BY total_sales DESC;

-- Query 4: Sales by Location (Online vs Offline)
-- Count sales by location (Web or Offline)
SELECT 
    COUNT(o.store_code) AS number_of_sales, 
    SUM(o.product_quantity) AS product_quantity_count,
    CASE
        WHEN d.store_type = 'Web' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM orders_table o
JOIN dim_store_details d 
    ON d.store_code = o.store_code
GROUP BY 
    CASE
        WHEN d.store_type = 'Web' THEN 'Web'
        ELSE 'Offline'
    END
ORDER BY location DESC;

-- Query 5: Store-Type Sales Percentage
-- Calculate the percentage of sales by store type
SELECT 
    d.store_type AS store_type,
    SUM(o.product_quantity * p.product_price) AS total_sales,
    ROUND((COUNT(o.store_code) * 100.0 / SUM(COUNT(o.store_code)) OVER ()), 2) AS sales_made_percentage
FROM orders_table o
JOIN dim_store_details d ON o.store_code = d.store_code
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY d.store_type
ORDER BY total_sales DESC;

-- Query 6: All-Time Sales by Month
-- Fetch top 10 months with the highest total sales
SELECT 
      SUM(o.product_quantity * p.product_price) AS total_sales,
      d.year, d.month
FROM orders_table o
JOIN dim_date_times d ON o.date_uuid = d.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY d.year, d.month
ORDER BY total_sales DESC
LIMIT 10;

-- Query 7: Staff Headcount by Country
-- Fetch the total staff numbers by country
SELECT sum(staff_numbers) AS total_staff_numbers,
       country_code
FROM dim_store_details
WHERE store_code NOT LIKE '%WEB%'
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Query 8: Sales by Store-Type in Germany
-- Calculate sales by store type in Germany
SELECT 
      SUM(o.product_quantity * p.product_price) AS total_sales,
      s.store_type, s.country_code
FROM orders_table o
JOIN dim_products p ON o.product_code = p.product_code
JOIN dim_store_details s ON o.store_code = s.store_code
WHERE s.country_code = 'DE'
GROUP BY s.store_type, s.country_code
ORDER BY total_sales;

-- Query 9: Average Sale Time-Difference by Year
-- Calculate the average time difference between sales in a year
WITH sales_with_time_diff AS (
    SELECT 
        year,
        TO_TIMESTAMP(
            CONCAT(year, '-', month, '-', day, ' ', timestamp), 
            'YYYY-MM-DD HH24:MI:SS'
        ) AS current_sale,
        LEAD(
            TO_TIMESTAMP(
                CONCAT(year, '-', month, '-', day, ' ', timestamp), 
                'YYYY-MM-DD HH24:MI:SS'
            )
        ) OVER (
            PARTITION BY year
            ORDER BY TO_TIMESTAMP(
                CONCAT(year, '-', month, '-', day, ' ', timestamp), 
                'YYYY-MM-DD HH24:MI:SS'
            )
        ) AS next_sale
    FROM dim_date_times
),
time_differences AS (
    SELECT 
        year,
        EXTRACT(EPOCH FROM (next_sale - current_sale)) AS time_diff_seconds
    FROM sales_with_time_diff
    WHERE next_sale IS NOT NULL
)
SELECT 
    year,
    CONCAT(
        FLOOR(AVG(time_diff_seconds) / 3600), ' hours, ',
        FLOOR((AVG(time_diff_seconds) % 3600) / 60), ' minutes, ',
        FLOOR(AVG(time_diff_seconds) % 60), ' seconds, ',
        FLOOR((AVG(time_diff_seconds) * 1000) % 1000), ' milliseconds'
    ) AS actual_time_taken
FROM time_differences
GROUP BY year
ORDER BY year DESC;
