-- create a database to store everything related to weather
CREATE OR REPLACE DATABASE WEATHER_DB;

-- create a small warehouse to run queries (auto pauses when not in use to save cost)
CREATE OR REPLACE WAREHOUSE WEATHER_WH 
  WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;

-- create 3 layers: bronze (raw), silver (cleaned), gold (aggregated)
CREATE SCHEMA WEATHER_DB.BRONZE; 
CREATE SCHEMA WEATHER_DB.SILVER; 
CREATE SCHEMA WEATHER_DB.GOLD;

-- raw table where JSON data will be ingested as-is
CREATE OR REPLACE TABLE WEATHER_DB.BRONZE.RAW_WEATHER (
    raw_content VARIANT,  -- full JSON payload
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- when the row was loaded
);

-- silver layer: clean and structure the raw data
-- also filtering out unrealistic temperatures
CREATE OR REPLACE DYNAMIC TABLE WEATHER_DB.SILVER.CLEANED_WEATHER
TARGET_LAG = '1 minute'
WAREHOUSE = WEATHER_WH
AS
SELECT
    raw_content:city::STRING as city,
    raw_content:country::STRING as country,
    raw_content:temperature::FLOAT as temp,
    raw_content:humidity::FLOAT as humidity,
    raw_content:timestamp::TIMESTAMP as recorded_at
FROM WEATHER_DB.BRONZE.RAW_WEATHER
WHERE temp BETWEEN -60 AND 60; -- basic sanity check

-- gold layer: aggregate metrics for reporting
-- grouping by city and time window (per minute)
CREATE OR REPLACE DYNAMIC TABLE WEATHER_DB.GOLD.CITY_PERFORMANCE
TARGET_LAG = '1 minute'
WAREHOUSE = WEATHER_WH
AS
SELECT
    city,
    AVG(temp) as avg_temp,
    MAX(temp) as max_temp,
    AVG(humidity) as avg_humidity,
    DATE_TRUNC('minute', recorded_at) as time_window
FROM WEATHER_DB.SILVER.CLEANED_WEATHER
GROUP BY city, time_window;

-- quick check: latest raw data coming in
SELECT * FROM WEATHER_DB.BRONZE.RAW_WEATHER 
ORDER BY ingested_at DESC 
LIMIT 10;

-- check cleaned data (now structured with proper columns)
SELECT 
    city, 
    temp, 
    humidity, 
    recorded_at 
FROM WEATHER_DB.SILVER.CLEANED_WEATHER 
ORDER BY recorded_at DESC 
LIMIT 10;

-- check aggregated results in gold layer
SELECT * FROM WEATHER_DB.GOLD.CITY_PERFORMANCE 
ORDER BY time_window DESC 
LIMIT 10;

-- overall average temperature per city
SELECT 
    city, 
    AVG(avg_temp) as overall_avg 
FROM WEATHER_DB.GOLD.CITY_PERFORMANCE 
GROUP BY city 
ORDER BY overall_avg DESC 
LIMIT 5;

-- overall average humidity per city
SELECT 
    city, 
    AVG(avg_humidity) as overall_avg_humidity 
FROM WEATHER_DB.GOLD.CITY_PERFORMANCE 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 5;

-- again checking if new raw data is flowing in
SELECT * FROM WEATHER_DB.BRONZE.RAW_WEATHER 
ORDER BY ingested_at DESC 
LIMIT 10;

-- list all tables in the database
SHOW TABLES IN DATABASE WEATHER_DB;


-- list tables in silver layer to verify structure
SHOW TABLES IN SCHEMA WEATHER_DB.SILVER;
