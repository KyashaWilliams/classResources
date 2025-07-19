SELECT * FROM `mgmt599-kyashawilliams-lab1.assignment1_eda.superstore` LIMIT 1000;
-- run this in BigQuery to verify data loaded correctly
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT customer) as unique_customers,
  MIN(Order_Date) as earliest_order,
  MAX(Order_Date) as latest_order
FROM `mgmt599-kyashawilliams-lab1.assignment1_eda.superstore`;
-- Prompt 1: handling missing values
SELECT
  COUNTIF(order_id IS NULL) AS null_order_id,
  COUNTIF(order_date IS NULL) AS null_order_date,
  COUNTIF(ship_date IS NULL) AS null_ship_date,
  COUNTIF(customer IS NULL) AS null_customer,
  COUNTIF(manufactory IS NULL) AS null_manufactory,
  COUNTIF(product_name IS NULL) AS null_product_name,
  COUNTIF(segment IS NULL) AS null_segment,
  COUNTIF(category IS NULL) AS null_category,
  COUNTIF(subcategory IS NULL) AS null_subcategory,
  COUNTIF(region IS NULL) AS null_region,
  COUNTIF(zip IS NULL) AS null_zip,
  COUNTIF(city IS NULL) AS null_city,
  COUNTIF(state IS NULL) AS null_state,
  COUNTIF(country IS NULL) AS null_country,
  COUNTIF(discount IS NULL) AS null_discount,
  COUNTIF(profit IS NULL) AS null_profit,
  COUNTIF(quantity IS NULL) AS null_quantity,
  COUNTIF(sales IS NULL) AS null_sales,
  COUNTIF(profit_margin IS NULL) AS null_profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
SELECT
  *
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  order_id IS NOT NULL AND order_date IS NOT NULL;
SELECT
  *
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  order_id IS NOT NULL AND order_date IS NOT NULL AND ship_date IS NOT NULL AND customer IS NOT NULL AND
  manufactory IS NOT NULL AND product_name IS NOT NULL AND segment IS NOT NULL AND category IS NOT NULL AND
  subcategory IS NOT NULL AND region IS NOT NULL AND zip IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND
  country IS NOT NULL AND discount IS NOT NULL AND profit IS NOT NULL AND quantity IS NOT NULL AND sales IS NOT NULL AND
  profit_margin IS NOT NULL;
SELECT
  order_id,
  order_date,
  ship_date,
  customer,
  manufactory,
  product_name,
  segment,
  category,
  subcategory,
  region,
  zip,
  city,
  state,
  country,
  COALESCE(discount, 0) AS discount,
  profit,
  quantity,
  sales,
  profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
SELECT
  t1.* EXCEPT (profit),
  COALESCE(t1.profit, avg_profit.avg_val) AS profit_imputed
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore AS t1,
  (
    SELECT
      AVG(profit) AS avg_val
    FROM
      `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
    WHERE
      profit IS NOT NULL
  ) AS avg_profit;
SELECT
  t1.* EXCEPT (sales),
  COALESCE(t1.sales, median_sales.median_val) AS sales_imputed
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore AS t1,
  (
    SELECT
      APPROX_QUANTILES(sales, 2)[OFFSET(1)] AS median_val
    FROM
      `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
    WHERE
      sales IS NOT NULL
  ) AS median_sales;
SELECT
  order_id,
  order_date,
  ship_date,
  customer,
  COALESCE(manufactory, 'Unknown') AS manufactory,
  product_name,
  segment,
  category,
  subcategory,
  region,
  zip,
  city,
  state,
  country,
  discount,
  profit,
  quantity,
  sales,
  profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
SELECT
  t1.* EXCEPT (city),
  COALESCE(t1.city, mode_city.mode_val) AS city_imputed
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore AS t1,
  (
    SELECT
      city AS mode_val
    FROM
      `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
    WHERE
      city IS NOT NULL
    GROUP BY city
    ORDER BY COUNT(*) DESC
    LIMIT 1
  ) AS mode_city;
CREATE OR REPLACE TABLE `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore_cleaned AS
SELECT
  order_id,
  order_date,
  ship_date,
  customer,
  COALESCE(manufactory, 'Unknown') AS manufactory,
  product_name,
  segment,
  category,
  subcategory,
  region,
  zip,
  COALESCE(city, (
      SELECT
        city
      FROM
        `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
      WHERE
        city IS NOT NULL
      GROUP BY city
      ORDER BY COUNT(*) DESC
      LIMIT 1)) AS city,
  state,
  country,
  COALESCE(discount, 0) AS discount,
  COALESCE(profit, (
      SELECT
        AVG(profit)
      FROM
        `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
      WHERE
        profit IS NOT NULL
    )) AS profit,
  quantity,
  COALESCE(sales, (
      SELECT
        APPROX_QUANTILES(sales, 2)[OFFSET(1)]
      FROM
        `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
      WHERE
        sales IS NOT NULL
    )) AS sales,
  profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  order_id IS NOT NULL AND order_date IS NOT NULL AND ship_date IS NOT NULL AND customer IS NOT NULL AND
  product_name IS NOT NULL AND segment IS NOT NULL AND category IS NOT NULL AND subcategory IS NOT NULL AND
  region IS NOT NULL AND zip IS NOT NULL AND state IS NOT NULL AND country IS NOT NULL AND quantity IS NOT NULL AND
  profit_margin IS NOT NULL;
-- Prompt 2: converting dates to appropriate format
SELECT
  order_id,
  FORMAT_DATE('%m-%d-%Y', order_date) AS order_date_formatted,
  FORMAT_DATE('%m-%d-%Y', ship_date) AS ship_date_formatted,
  customer,
  manufactory,
  product_name,
  segment,
  category,
  subcategory,
  region,
  zip,
  city,
  state,
  country,
  discount,
  profit,
  quantity,
  sales,
  profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
-- Prompt 3: please give a query to identify outliers
SELECT
  t1.*
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore AS t1,
  (
    SELECT
      APPROX_QUANTILES(sales, 4)[OFFSET(1)] AS q1_sales,
      APPROX_QUANTILES(sales, 4)[OFFSET(3)] AS q3_sales
    FROM
      `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
    WHERE
      sales IS NOT NULL
  ) AS sales_quartiles
WHERE
  t1.sales IS NOT NULL AND (t1.sales < sales_quartiles.q1_sales - 1.5 * (sales_quartiles.q3_sales - sales_quartiles.q1_sales) OR
  t1.sales > sales_quartiles.q3_sales + 1.5 * (sales_quartiles.q3_sales - sales_quartiles.q1_sales));
  -- Prompt 4: check for duplicate records 
SELECT
  order_id,
  order_date,
  ship_date,
  customer,
  manufactory,
  product_name,
  segment,
  category,
  subcategory,
  region,
  zip,
  city,
  state,
  country,
  discount,
  profit,
  quantity,
  sales,
  profit_margin,
  COUNT(*) AS duplicate_count
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY order_id, order_date, ship_date, customer, manufactory, product_name, segment, category, subcategory,
  region, zip, city, state, country, discount, profit, quantity, sales, profit_margin
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, order_id, product_name;
-- Prompt 5: average values for all numerical columns
SELECT
  AVG(zip) AS avg_zip,
  AVG(discount) AS avg_discount,
  AVG(profit) AS avg_profit,
  AVG(quantity) AS avg_quantity,
  AVG(sales) AS avg_sales,
  AVG(profit_margin) AS avg_profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
-- Prompt 6: average values of numeric columns for a specific year like 2022
SELECT
  AVG(zip) AS avg_zip,
  AVG(discount) AS avg_discount,
  AVG(profit) AS avg_profit,
  AVG(quantity) AS avg_quantity,
  AVG(sales) AS avg_sales,
  AVG(profit_margin) AS avg_profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  EXTRACT(YEAR FROM order_date) = 2022;
-- PROMPT 7: find the maximum and minimum by month
SELECT
  FORMAT_DATE('%Y-%m', order_date) AS order_year_month,
  MIN(sales) AS min_sales,
  MAX(sales) AS max_sales,
  MIN(profit) AS min_profit,
  MAX(profit) AS max_profit,
  MIN(quantity) AS min_quantity,
  MAX(quantity) AS max_quantity,
  MIN(discount) AS min_discount,
  MAX(discount) AS max_discount,
  MIN(profit_margin) AS min_profit_margin,
  MAX(profit_margin) AS max_profit_margin
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY order_year_month
ORDER BY order_year_month;
-- Promt 8: counting the number of trading days
SELECT
  COUNT(DISTINCT order_date) AS number_of_trading_days
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore;
-- PROMPT 9: calculate high trading volume days
SELECT
  Date,
  Volume
FROM
  `mgmt599-kyashawilliams-lab1`.lab1_eda.nvda_stocks
WHERE
  Volume > (
    SELECT
      AVG(Volume)
    FROM
      `mgmt599-kyashawilliams-lab1`.lab1_eda.nvda_stocks
  )
ORDER BY Volume DESC;
-- PROMPT 10: calculate daily percentage count
SELECT
  order_date,
  SUM(sales) AS daily_sales,
  LAG(SUM(sales), 1) OVER (ORDER BY order_date) AS previous_day_sales,
  (SUM(sales) - LAG(SUM(sales), 1) OVER (ORDER BY order_date)) / LAG(SUM(sales), 1) OVER (ORDER BY order_date) * 100 AS daily_sales_percentage_change
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  order_date
ORDER BY
  order_date;
-- PROMPT 11: calculate the stock performance over time
SELECT
  Date,
  Close,
  LAG(Close, 1) OVER (
    ORDER BY Date) AS previous_day_close,
  (Close - LAG(Close, 1) OVER (
    ORDER BY Date)) / LAG(Close, 1) OVER (
    ORDER BY Date) * 100 AS daily_percentage_change
FROM
  `mgmt599-kyashawilliams-lab1`.lab1_eda.nvda_stocks
ORDER BY Date;
-- Prompt 12: performance vary by location region
SELECT
  region,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(profit / sales) AS average_profit_ratio -- Assuming sales is not zero
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  region
ORDER BY
  total_sales DESC;
-- Prompt 13: performance vary by location state
SELECT
  state,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(profit / sales) AS average_profit_ratio
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  state
ORDER BY
  total_sales DESC
LIMIT 10;
-- Prompt 14: performance vary by location city
SELECT
  city,
  state, -- Including state to differentiate cities with the same name in different states
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(profit / sales) AS average_profit_ratio
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  city,
  state
ORDER BY
  total_profit DESC
LIMIT 10;
-- Prompt 15: key operational factors and how they impact success
-- Inventory Management: top 10 Products
SELECT
  product_name,
  SUM(quantity) AS total_quantity_sold,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  product_name
ORDER BY
  total_quantity_sold DESC
LIMIT 10;
-- Inventory Management: categories with lowest profit per item sold
SELECT
  category,
  SUM(quantity) AS total_quantity_sold,
  SUM(profit) AS total_profit,
  SUM(profit) / SUM(quantity) AS avg_profit_per_item
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  category
HAVING
  SUM(quantity) > 0 -- Avoid division by zero
ORDER BY
  avg_profit_per_item ASC
LIMIT 5;
-- Pricing and Discounting: impact of discount on profitability 
SELECT
  CASE
    WHEN discount = 0 THEN 'No Discount'
    WHEN discount > 0 AND discount <= 0.1 THEN 'Low Discount (0-10%)'
    WHEN discount > 0.1 AND discount <= 0.3 THEN 'Medium Discount (10-30%)'
    ELSE 'High Discount (>30%)'
  END AS discount_group,
  COUNT(*) AS total_transactions,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(profit) AS avg_profit_per_transaction,
  AVG(sales) AS avg_sales_per_transaction
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  discount_group
ORDER BY
  discount_group;
-- Pricing and Discounting: products sold at a loss due to discount 
SELECT
  product_name,
  category,
  subcategory,
  sales,
  profit,
  discount
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  profit < 0 AND discount > 0
ORDER BY
  profit ASC
LIMIT 10;
-- Product Assortment and Category Management: sales and profit contribution by category 
SELECT
  category,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  SUM(sales) / (SELECT SUM(sales) FROM `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore) * 100 AS sales_contribution_percent,
  SUM(profit) / (SELECT SUM(profit) FROM `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore WHERE profit > 0) * 100 AS profit_contribution_percent -- Only sum positive profits for percentage
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  category
ORDER BY
  total_sales DESC;
-- Customer Segment: performance by customer segment 
SELECT
  segment,
  COUNT(DISTINCT customer) AS unique_customers,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(sales) AS avg_sales_per_transaction,
  AVG(profit) AS avg_profit_per_transaction
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  segment
ORDER BY
  total_sales DESC;
-- Logistics and Shipping Effeciency: average shipping duration by ship mode
SELECT
  ship_date,
  AVG(DATE_DIFF(ship_date, order_date, DAY)) AS avg_shipping_days
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  ship_date IS NOT NULL AND order_date IS NOT NULL
GROUP BY
  ship_date
ORDER BY
  avg_shipping_days ASC;
--- Supply Chain Management (considering manufactury performance): top 10 manufacturers by profit 
SELECT
  manufactory,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  COUNT(DISTINCT product_name) AS unique_products_supplied
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
WHERE
  manufactory IS NOT NULL -- Exclude rows where manufacturer is unknown
GROUP BY
  manufactory
ORDER BY
  total_profit DESC
LIMIT 10;
-- Store/Regional Performance Management: regional profitability overview
SELECT
  region,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  AVG(profit / sales) AS average_profit_margin_percent
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  region
ORDER BY
  total_profit DESC;
-- Prompt 16: expand or consolidate
-- Top 10 States/Cities by Profit (Identifies Most Profitable Areas)
SELECT
  state,
  city,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(profit) / SUM(sales) AS profit_margin_percentage -- Assuming sales is not zero
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  state,
  city
HAVING
  SUM(sales) > 0 -- Exclude locations with no sales to prevent division by zero in profit_margin_percentage
ORDER BY
  total_profit DESC
LIMIT 10;
-- Regions/States with High Average Order Value or Profit Per Customer
SELECT
  region,
  state,
  COUNT(DISTINCT customer) AS unique_customers,
  SUM(sales) AS total_sales,
  SUM(sales) / COUNT(DISTINCT customer) AS average_sales_per_customer,
  SUM(profit) AS total_profit,
  SUM(profit) / COUNT(DISTINCT customer) AS average_profit_per_customer
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  region,
  state
HAVING
  COUNT(DISTINCT customer) > 0 -- Exclude divisions by zero
ORDER BY
  average_profit_per_customer DESC
LIMIT 10;
-- Growth Trends (Requires data spanning multiple years)
SELECT
  EXTRACT(YEAR FROM order_date) AS order_year,
  region,
  state,
  SUM(sales) AS annual_sales,
  SUM(profit) AS annual_profit
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  order_year,
  region,
  state
ORDER BY
  order_year, region, state;
-- Growth Trends (Requires data spanning multiple years): year-over-year percentage change for sales and profit
WITH
  AnnualPerformance AS (
    SELECT
      EXTRACT(YEAR FROM order_date) AS order_year,
      region,
      state,
      SUM(sales) AS annual_sales,
      SUM(profit) AS annual_profit
    FROM
      `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
    GROUP BY order_year, region, state
  )
SELECT
  order_year,
  region,
  state,
  annual_sales,
  LAG(annual_sales, 1) OVER (PARTITION BY region, state
    ORDER BY order_year) AS previous_year_sales,
  CASE
    WHEN LAG(annual_sales, 1) OVER (PARTITION BY region, state
      ORDER BY order_year) IS NULL THEN NULL
    WHEN LAG(annual_sales, 1) OVER (PARTITION BY region, state
      ORDER BY order_year) = 0 THEN 'N/A (Previous Sales Zero)'
    ELSE FORMAT("%0.2f%%", (annual_sales - LAG(annual_sales, 1) OVER (PARTITION BY region, state
        ORDER BY order_year)) / LAG(annual_sales, 1) OVER (PARTITION BY region, state
        ORDER BY order_year) * 100)
  END AS yoy_sales_growth_percent,
  annual_profit,
  LAG(annual_profit, 1) OVER (PARTITION BY region, state
    ORDER BY order_year) AS previous_year_profit,
  CASE
    WHEN LAG(annual_profit, 1) OVER (PARTITION BY region, state
      ORDER BY order_year) IS NULL THEN NULL
    WHEN LAG(annual_profit, 1) OVER (PARTITION BY region, state
      ORDER BY order_year) = 0 THEN 'N/A (Previous Profit Zero)'
    ELSE FORMAT("%0.2f%%", (annual_profit - LAG(annual_profit, 1) OVER (PARTITION BY region, state
        ORDER BY order_year)) / LAG(annual_profit, 1) OVER (PARTITION BY region, state
        ORDER BY order_year) * 100)
  END AS yoy_profit_growth_percent
FROM
  AnnualPerformance
ORDER BY region, state, order_year;
-- Bottom 10 States/Cities by Profit (Identifies Unprofitable Areas)
SELECT
  state,
  city,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(profit) / SUM(sales) AS profit_margin_percentage -- Will be negative if total_profit is negative
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  state,
  city
HAVING
  SUM(sales) > 0 -- Exclude locations with no sales
ORDER BY
  total_profit ASC
LIMIT 10;
-- Areas with High Sales but Low/Negative Profit Margin
SELECT
  region,
  state,
  city,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit,
  SUM(profit) / SUM(sales) AS profit_margin_percentage
FROM
  `mgmt599-kyashawilliams-lab1`.assignment1_eda.superstore
GROUP BY
  region,
  state,
  city
HAVING
  SUM(sales) > 10000 -- Set a reasonable sales threshold to filter out tiny locations
  AND (SUM(profit) / SUM(sales)) < 0.05 -- Less than 5% profit margin
ORDER BY
  profit_margin_percentage ASC
LIMIT 10;


