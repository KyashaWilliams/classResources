SELECT * FROM `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks` LIMIT 1000;

-- Handling missing values.
-- Step 1: Filling missing values: Replacing missing values with 0.
SELECT
  Date,
  COALESCE(Open, 0.0) AS Open_cleaned,
  COALESCE(High, 0.0) AS High_cleaned,
  COALESCE(Low, 0.0) AS Low_cleaned,
  COALESCE(Close, 0.0) AS Close_cleaned,
  COALESCE(Volume, 0) AS Volume_cleaned,
  COALESCE(Dividends, 0.0) AS Dividends_cleaned,
  COALESCE(`Stock Splits`, 0.0) AS Stock_Splits_cleaned
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`;
  -- Step 2: Convert Date column to appropriate data type
SELECT
  CAST(Date AS DATE) AS cleaned_date,
  Open,
  High,
  Low,
  Close,
  Volume,
  Dividends,
  `Stock Splits`
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`;
  -- Step 3: Please give a query to identify outliers
  WITH
  Quartiles AS (
    SELECT
      PERCENTILE_CONT(t.Volume, 0.25) OVER () AS Q1,
      PERCENTILE_CONT(t.Volume, 0.75) OVER () AS Q3
    FROM
      `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks` AS t
  ),
  IQR_Bounds AS (
    SELECT
      Q1,
      Q3,
      (Q3 - Q1) AS IQR,
      Q1 - 1.5 * (Q3 - Q1) AS Lower_Bound,
      Q3 + 1.5 * (Q3 - Q1) AS Upper_Bound
    FROM
      Quartiles
  )
SELECT
  t.Date,
  t.Open,
  t.High,
  t.Low,
  t.Close,
  t.Volume,
  t.Dividends,
  t.`Stock Splits`,
  'Volume Outlier' AS Outlier_Type,
  b.Lower_Bound,
  b.Upper_Bound
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks` AS t,
  IQR_Bounds AS b
WHERE
  t.Volume < b.Lower_Bound OR t.Volume > b.Upper_Bound;
  -- Step 4: Check for duplicate records
  SELECT
  Date,
  Open,
  High,
  Low,
  Close,
  Volume,
  Dividends,
  `Stock Splits`,
  COUNT(*) AS num_duplicates
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`
GROUP BY
  Date,
  Open,
  High,
  Low,
  Close,
  Volume,
  Dividends,
  `Stock Splits`
HAVING
  num_duplicates > 1;

-- Step 5: Average values for all numerical columns
SELECT
  AVG(Open) AS Average_Open,
  AVG(High) AS Average_High,
  AVG(Low) AS Average_Low,
  AVG(Close) AS Average_Close,
  AVG(Volume) AS Average_Volume,
  AVG(Dividends) AS Average_Dividends,
  AVG(`Stock Splits`) AS Average_Stock_Splits
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`;
  -- Step 6: Average values of numeric columns for a specific year like 2023
  SELECT
  AVG(Open) AS Average_Open,
  AVG(High) AS Average_High,
  AVG(Low) AS Average_Low,
  AVG(Close) AS Average_Close,
  AVG(Volume) AS Average_Volume,
  AVG(Dividends) AS Average_Dividends,
  AVG(`Stock Splits`) AS Average_Stock_Splits
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`
WHERE
  EXTRACT(YEAR FROM Date) = 2023;
 -- Step 7: "Find the maximum and minimum by month"
 SELECT
  EXTRACT(YEAR FROM Date) AS Year,
  EXTRACT(MONTH FROM Date) AS Month,
  MAX(Open) AS Max_Open,
  MIN(Open) AS Min_Open,
  MAX(High) AS Max_High,
  MIN(High) AS Min_High,
  MAX(Low) AS Max_Low,
  MIN(Low) AS Min_Low,
  MAX(Close) AS Max_Close,
  MIN(Close) AS Min_Close,
  MAX(Volume) AS Max_Volume,
  MIN(Volume) AS Min_Volume,
  MAX(Dividends) AS Max_Dividends,
  MIN(Dividends) AS Min_Dividends,
  MAX(`Stock Splits`) AS Max_Stock_Splits,
  MIN(`Stock Splits`) AS Min_Stock_Splits
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`
GROUP BY Year, Month
ORDER BY Year, Month;
-- Step 8: Counting the number of trading days 
 SELECT
  COUNT(DISTINCT Date) AS Number_of_Trading_Days
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`;
-- Step 9: Calculate high trading volume days
WITH
  Quartiles AS (
    SELECT
      PERCENTILE_CONT(t.Volume, 0.25) OVER () AS Q1,
      PERCENTILE_CONT(t.Volume, 0.75) OVER () AS Q3
    FROM
      `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks` AS t
  ),
  IQR_Bounds AS (
    SELECT
      Q1,
      Q3,
      (Q3 - Q1) AS IQR,
      Q3 + 1.5 * (Q3 - Q1) AS Upper_Bound
    FROM
      Quartiles
  )
SELECT
  t.Date,
  t.Open,
  t.High,
  t.Low,
  t.Close,
  t.Volume,
  t.Dividends,
  t.`Stock Splits`
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks` AS t,
  IQR_Bounds AS b
WHERE
  t.Volume > b.Upper_Bound
ORDER BY t.Date;
-- Step 10: Calculate daily percentage count
SELECT
  Date,
  Close,
  LAG(Close, 1) OVER (
    ORDER BY Date) AS Previous_Day_Close,
  (Close - LAG(Close, 1) OVER (
    ORDER BY Date)) / LAG(Close, 1) OVER (
    ORDER BY Date) * 100 AS Daily_Percentage_Change
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`
ORDER BY Date;
-- Step 11: Calculate the stock performance over time
SELECT
  Date,
  Close,
  -- Calculate the closing price on the first day in the dataset
  FIRST_VALUE(Close) OVER (ORDER BY Date ASC) AS Initial_Close_Price,
  -- Calculate the cumulative percentage change from the initial close price
  (Close - FIRST_VALUE(Close) OVER (ORDER BY Date ASC)) / FIRST_VALUE(Close) OVER (ORDER BY Date ASC) * 100 AS Cumulative_Percentage_Change
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`
ORDER BY
  Date;
-- Run this in BigQuery to verify data loaded correctly
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT Date) AS unique_trading_days,
  MIN(Date) AS earliest_date,
  MAX(Date) AS latest_date
FROM
  `mgmt599-kyashawilliams-lab1.lab1_eda.nvda_stocks`;

