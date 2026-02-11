-- 1. DATABASE SETUP
CREATE DATABASE project_food_delivery;
USE project_food_delivery;

-- Enable local infile loading
SET GLOBAL local_infile = 1;

-- 2. RAW TABLE CREATION
CREATE TABLE couriers_raw (
    courier_id VARCHAR(50),
    courier_name VARCHAR(100),
    vehicle_type VARCHAR(50),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    is_active VARCHAR(10)
);
CREATE TABLE customers_raw (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    email VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(100),
    created_at VARCHAR(50)
);
CREATE TABLE order_items_raw (
    order_item_id VARCHAR(50),
    order_id VARCHAR(50),
    menu_code VARCHAR(50),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    line_amount VARCHAR(50)
);
CREATE TABLE deliveries_raw (
    delivery_id VARCHAR(50),
    order_id VARCHAR(50),
    courier_id VARCHAR(50),
    assign_time VARCHAR(50),
    drop_time VARCHAR(50),
    distance_km VARCHAR(50)
);
CREATE TABLE orders_raw (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    order_datetime VARCHAR(50),
    status VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100)
);
CREATE TABLE restaurants_raw (
    restaurant_id VARCHAR(50),
    restaurant_name VARCHAR(150),
    cuisine VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    rating VARCHAR(50),
    is_active VARCHAR(10)
);

-- 3. LOAD RAW DATA (update path as needed)
LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/couriers_merged.csv'
INTO TABLE couriers_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/customers_merged.csv'
INTO TABLE customers_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/restaurants_merged.csv'
INTO TABLE restaurants_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/orders_merged.csv'
INTO TABLE orders_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- ⚠️ Note: file is 'order_items_merged.csv' — not 'order_item_merged.csv'
LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/order_items_merged.csv'
INTO TABLE order_items_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/Users/User/Documents/project_3/food_delivery_capstone_v2/data/merged/deliveries_merged.csv'
INTO TABLE deliveries_raw
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- 4. STAGING COPIES (for cleaning)
CREATE TABLE couriers_stg AS SELECT * FROM couriers_raw;
CREATE TABLE customers_stg AS SELECT * FROM customers_raw;
CREATE TABLE restaurants_stg AS SELECT * FROM restaurants_raw;
CREATE TABLE orders_stg AS SELECT * FROM orders_raw;
CREATE TABLE order_items_stg AS SELECT * FROM order_items_raw;
CREATE TABLE deliveries_stg AS SELECT * FROM deliveries_raw;

-- 5. DATA CLEANING
set sql_safe_updates=0;
-- === CUSTOMERS ===
UPDATE customers_stg
SET 
    first_name = NULLIF(TRIM(first_name), ''),
    last_name = NULLIF(TRIM(last_name), ''),
    gender = NULLIF(TRIM(gender), ''),
    email = NULLIF(TRIM(email), ''),
    city = NULLIF(TRIM(city), ''),
    state = NULLIF(TRIM(state), ''),
    created_at = NULLIF(TRIM(created_at), '');

UPDATE customers_stg
SET created_at = NOW()
WHERE 
    -- Trim whitespace, then truncate to seconds (drop .micro)
    STR_TO_DATE(
        SUBSTRING_INDEX(TRIM(created_at), '.', 1), 
        '%Y-%m-%d %H:%i:%s'
    ) > NOW()
    OR created_at REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)? *$';

UPDATE customers_stg
SET created_at = STR_TO_DATE(SUBSTRING_INDEX(created_at, '.', 1), '%Y-%m-%d %H:%i:%s')
WHERE created_at REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}';

UPDATE customers_stg
SET 
    city = COALESCE(NULLIF(TRIM(city), ''), 'Unknown'),
    state = COALESCE(NULLIF(TRIM(state), ''), 'Unknown');

-- === COURIERS ===
UPDATE couriers_stg
SET 
    courier_name = NULLIF(TRIM(courier_name), ''),
    vehicle_type = NULLIF(TRIM(vehicle_type), ''),
    phone = NULLIF(TRIM(phone), ''),
    city = NULLIF(TRIM(city), ''),
    state = NULLIF(TRIM(state), ''),
    is_active = NULLIF(TRIM(is_active), '');

-- Clean phone: digits only
UPDATE couriers_stg
SET phone = REGEXP_REPLACE(phone, '[^0-9]', '') 
WHERE phone IS NOT NULL;

UPDATE couriers_stg
SET 
    courier_name = COALESCE(courier_name, 'Unknown'),
    vehicle_type = COALESCE(vehicle_type, 'Unknown'),
    phone = CASE 
        WHEN phone IS NULL OR LENGTH(phone) NOT BETWEEN 7 AND 15 THEN 'Unknown'
        ELSE phone
    END,
    city = COALESCE(NULLIF(TRIM(city), ''), 'Unknown'),
    state = COALESCE(NULLIF(TRIM(state), ''), 'Unknown');

UPDATE couriers_stg
SET is_active = CASE 
    WHEN LOWER(is_active) IN ('yes','true','1','t') THEN TRUE
    WHEN LOWER(is_active) IN ('no','false','0','f') THEN FALSE
    ELSE FALSE
END;

-- === RESTAURANTS ===
UPDATE restaurants_stg
SET 
    restaurant_name = NULLIF(TRIM(restaurant_name), ''),
    cuisine = NULLIF(TRIM(cuisine), ''),
    city = NULLIF(TRIM(city), ''),
    state = NULLIF(TRIM(state), ''),
    rating = NULLIF(TRIM(rating), ''),
    is_active = NULLIF(TRIM(is_active), '');

UPDATE restaurants_stg
SET rating = CASE 
    WHEN rating REGEXP '^[0-9]+(\\.[0-9]+)?$' AND CAST(rating AS DECIMAL(3,1)) BETWEEN 0 AND 5 
    THEN CAST(rating AS DECIMAL(2,1))
    ELSE NULL
END;

UPDATE restaurants_stg
SET is_active = CASE 
    WHEN LOWER(is_active) IN ('yes','true','1','t') THEN TRUE
    WHEN LOWER(is_active) IN ('no','false','0','f') THEN FALSE
    ELSE FALSE
END;

UPDATE restaurants_stg
SET 
    city = COALESCE(NULLIF(TRIM(city), ''), 'Unknown'),
    state = COALESCE(NULLIF(TRIM(state), ''), 'Unknown');

-- === ORDERS ===
UPDATE orders_stg
SET 
    customer_id = NULLIF(TRIM(customer_id), ''),
    restaurant_id = NULLIF(TRIM(restaurant_id), ''),
    status = NULLIF(TRIM(status), ''),
    city = NULLIF(TRIM(city), ''),
    state = NULLIF(TRIM(state), '');

-- Parse datetime
UPDATE orders_stg
SET order_datetime = CASE
    WHEN order_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$'
    THEN STR_TO_DATE(SUBSTRING_INDEX(order_datetime, '.', 1), '%Y-%m-%d %H:%i:%s')
    ELSE NULL
END;

UPDATE orders_stg
SET 
    status = UPPER(COALESCE(status, 'UNKNOWN')),
    city = COALESCE(NULLIF(TRIM(city), ''), 'Unknown'),
    state = COALESCE(NULLIF(TRIM(state), ''), 'Unknown');

-- Remove orphans
DELETE FROM orders_stg WHERE customer_id = '__ORPHAN__' OR restaurant_id = '__ORPHAN__' OR order_id = '__ORPHAN__';
DELETE FROM order_items_stg WHERE order_id = '__ORPHAN__';
DELETE FROM deliveries_stg WHERE order_id = '__ORPHAN__' OR courier_id = '__ORPHAN__';

-- === ORDER ITEMS ===
UPDATE order_items_stg
SET 
    quantity = CASE WHEN quantity REGEXP '^[0-9]+$' THEN CAST(quantity AS UNSIGNED) ELSE 0 END,
    unit_price = CASE WHEN unit_price REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(unit_price AS DECIMAL(10,2)) ELSE 0.0 END,
    line_amount = CASE WHEN line_amount REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(line_amount AS DECIMAL(10,2)) ELSE 0.0 END;

-- Deduplicate
WITH RankedItems AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, menu_code 
               ORDER BY order_item_id
           ) AS rn
    FROM order_items_stg
)
DELETE FROM order_items_stg
WHERE order_item_id IN (
    SELECT order_item_id FROM RankedItems WHERE rn > 1
);

-- === DELIVERIES ===
UPDATE deliveries_stg
SET 
    assign_time = CASE 
        WHEN assign_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$' 
        THEN STR_TO_DATE(SUBSTRING_INDEX(assign_time, '.', 1), '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END,
    drop_time = CASE 
        WHEN drop_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$' 
        THEN STR_TO_DATE(SUBSTRING_INDEX(drop_time, '.', 1), '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END,
    distance_km = CASE 
        WHEN distance_km REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(distance_km AS DECIMAL(5,2))
        ELSE 0.0
    END;

-- Remove invalid deliveries
DELETE FROM deliveries_stg 
WHERE assign_time IS NULL OR drop_time IS NULL OR drop_time < assign_time;

-- Deduplicate
WITH RankedDeliveries AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, courier_id 
               ORDER BY delivery_id
           ) AS rn
    FROM deliveries_stg
)
DELETE FROM deliveries_stg
WHERE delivery_id IN (
    SELECT delivery_id FROM RankedDeliveries WHERE rn > 1
);

-- 6. AUDIT LOG TABLE
CREATE TABLE etl_audit_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    step VARCHAR(100),
    table_name VARCHAR(50),
    records_in INT,
    records_out INT,
    errors_handled INT,
    description TEXT,
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log row counts
INSERT INTO etl_audit_log (step, table_name, records_in, records_out, errors_handled, description)
VALUES
('Initial Load', 'couriers_raw', (SELECT COUNT(*) FROM couriers_raw), (SELECT COUNT(*) FROM couriers_stg), 0, 'Raw → Staging'),
('Initial Load', 'customers_raw', (SELECT COUNT(*) FROM customers_raw), (SELECT COUNT(*) FROM customers_stg), 0, 'Raw → Staging'),
('Initial Load', 'orders_raw', (SELECT COUNT(*) FROM orders_raw), (SELECT COUNT(*) FROM orders_stg), 0, 'Raw → Staging'),
('Initial Load', 'order_items_raw', (SELECT COUNT(*) FROM order_items_raw), (SELECT COUNT(*) FROM order_items_stg), 0, 'Raw → Staging'),
('Initial Load', 'deliveries_raw', (SELECT COUNT(*) FROM deliveries_raw), (SELECT COUNT(*) FROM deliveries_stg), 0, 'Raw → Staging'),
('Initial Load', 'restaurants_raw', (SELECT COUNT(*) FROM restaurants_raw), (SELECT COUNT(*) FROM restaurants_stg), 0, 'Raw → Staging');

-- 7. DIMENSION TABLES (SCHEMA)
CREATE TABLE dim_customer (
    sk_customer INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    email VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    created_at DATETIME,
    valid_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME DEFAULT NULL,
    current_flag BOOLEAN NOT NULL DEFAULT TRUE,
    INDEX idx_biz_key (customer_id),
    INDEX idx_scd (current_flag, valid_to)
);

CREATE TABLE dim_restaurant (
    sk_restaurant INT AUTO_INCREMENT PRIMARY KEY,
    restaurant_id VARCHAR(50) NOT NULL,
    restaurant_name VARCHAR(150),
    cuisine VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    rating DECIMAL(2,1),
    is_active BOOLEAN,
    valid_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME DEFAULT NULL,
    current_flag BOOLEAN NOT NULL DEFAULT TRUE,
    INDEX idx_biz_key (restaurant_id),
    INDEX idx_scd (current_flag, valid_to)
);

CREATE TABLE dim_courier (
    sk_courier INT AUTO_INCREMENT PRIMARY KEY,
    courier_id VARCHAR(50) NOT NULL,
    courier_name VARCHAR(100),
    vehicle_type VARCHAR(50),
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    is_active BOOLEAN,
    valid_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to DATETIME DEFAULT NULL,
    current_flag BOOLEAN NOT NULL DEFAULT TRUE,
    INDEX idx_biz_key (courier_id),
    INDEX idx_scd (current_flag, valid_to)
);

CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day TINYINT,
    month TINYINT,
    year SMALLINT,
    quarter TINYINT,
    week TINYINT,
    day_of_week TINYINT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_location (
    location_id CHAR(32) PRIMARY KEY,  -- MD5 hash
    city VARCHAR(100),
    state VARCHAR(100),
    INDEX idx_loc (city, state)
);

-- 8. FACT TABLES
CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    sk_customer INT NOT NULL,
    sk_restaurant INT NOT NULL,
    date_id DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL DEFAULT 0.0,
    quantity INT NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 1,
    FOREIGN KEY (sk_customer) REFERENCES dim_customer(sk_customer),
    FOREIGN KEY (sk_restaurant) REFERENCES dim_restaurant(sk_restaurant),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE fact_deliveries (
    delivery_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    sk_courier INT NOT NULL,
    delivery_date DATE NOT NULL,
    distance_km DECIMAL(5,2) NOT NULL DEFAULT 0.0,
    delivery_time_minutes INT NOT NULL DEFAULT 0,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (sk_courier) REFERENCES dim_courier(sk_courier),
    FOREIGN KEY (delivery_date) REFERENCES dim_date(date_id)
);

-- 9. POPULATE DIM_DATE (2020–2026)
SET SESSION cte_max_recursion_depth = 3000;
INSERT INTO dim_date (date_id, day, month, year, quarter, week, day_of_week, is_weekend)
WITH RECURSIVE dates AS (
    SELECT DATE('2020-01-01') AS d
    UNION ALL
    SELECT DATE_ADD(d, INTERVAL 1 DAY)
    FROM dates
    WHERE d < '2026-12-31'
)
SELECT 
    d,
    DAY(d),
    MONTH(d),
    YEAR(d),
    QUARTER(d),
    WEEK(d, 3),
    DAYOFWEEK(d),
    CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END
FROM dates;

-- 10. POPULATE DIM_LOCATION

INSERT INTO dim_location (location_id, city, state)
SELECT 
    MD5(CONCAT(UPPER(TRIM(city_clean)), '|', UPPER(TRIM(state_clean)))) AS location_id,
    TRIM(city_clean) AS city,
    TRIM(state_clean) AS state
FROM (
    SELECT city AS city_clean, state AS state_clean FROM customers_stg WHERE city != 'Unknown'
    UNION
    SELECT city AS city_clean, state AS state_clean FROM restaurants_stg WHERE city != 'Unknown'
    UNION
    SELECT city AS city_clean, state AS state_clean FROM couriers_stg WHERE city != 'Unknown'
    UNION
    SELECT city AS city_clean, state AS state_clean FROM orders_stg WHERE city != 'Unknown'
) loc
WHERE city_clean IS NOT NULL AND TRIM(city_clean) != ''
GROUP BY city_clean, state_clean;

-- 11. SCD TYPE 2 ETL — DIM_CUSTOMER
-- Step 1: Detect changes & expire old versions
UPDATE dim_customer d
JOIN (
    SELECT s.customer_id
    FROM customers_stg s
    INNER JOIN dim_customer d ON s.customer_id = d.customer_id
    WHERE d.current_flag = TRUE
      AND (
           s.first_name != d.first_name OR s.first_name IS NULL XOR d.first_name IS NULL OR
           s.last_name != d.last_name OR s.last_name IS NULL XOR d.last_name IS NULL OR
           s.city != d.city OR s.city IS NULL XOR d.city IS NULL OR
           s.state != d.state OR s.state IS NULL XOR d.state IS NULL OR
           s.email != d.email OR s.email IS NULL XOR d.email IS NULL
          )
) changed ON d.customer_id = changed.customer_id
SET 
    d.valid_to = NOW(),
    d.current_flag = FALSE;

-- Step 2: Insert new/changed records
INSERT INTO dim_customer (
    customer_id, first_name, last_name, gender, email, city, state, created_at,
    valid_from, valid_to, current_flag
)
SELECT 
    s.customer_id,
    s.first_name,
    s.last_name,
    s.gender,
    CONCAT(LEFT(s.email, 3), '***', SUBSTRING(s.email, LOCATE('@', s.email))) AS email,
    s.city,
    s.state,
    STR_TO_DATE(s.created_at, '%Y-%m-%d %H:%i:%s') AS created_at,
    NOW() AS valid_from,
    NULL AS valid_to,
    TRUE AS current_flag
FROM customers_stg s
LEFT JOIN dim_customer d ON s.customer_id = d.customer_id AND d.current_flag = TRUE
WHERE d.customer_id IS NULL  -- new
   OR d.customer_id IN (
        SELECT customer_id FROM (
            SELECT s2.customer_id
            FROM customers_stg s2
            JOIN dim_customer d2 ON s2.customer_id = d2.customer_id
            WHERE d2.current_flag = TRUE
              AND (
                   s2.first_name != d2.first_name OR s2.first_name IS NULL XOR d2.first_name IS NULL OR
                   s2.last_name != d2.last_name OR s2.last_name IS NULL XOR d2.last_name IS NULL OR
                   s2.city != d2.city OR s2.city IS NULL XOR d2.city IS NULL OR
                   s2.state != d2.state OR s2.state IS NULL XOR d2.state IS NULL OR
                   s2.email != d2.email OR s2.email IS NULL XOR d2.email IS NULL
                  )
        ) AS changed
    );

-- 12. SCD TYPE 2 — DIM_RESTAURANT
UPDATE dim_restaurant d
JOIN (
    SELECT s.restaurant_id
    FROM restaurants_stg s
    INNER JOIN dim_restaurant d ON s.restaurant_id = d.restaurant_id
    WHERE d.current_flag = TRUE
      AND (
           s.restaurant_name != d.restaurant_name OR s.cuisine != d.cuisine OR
           s.city != d.city OR s.state != d.state OR
           s.rating != d.rating OR s.is_active != d.is_active
          )
) changed ON d.restaurant_id = changed.restaurant_id
SET 
    d.valid_to = NOW(),
    d.current_flag = FALSE;

INSERT INTO dim_restaurant (
    restaurant_id, restaurant_name, cuisine, city, state, rating, is_active,
    valid_from, valid_to, current_flag
)
SELECT 
    s.restaurant_id,
    s.restaurant_name,
    s.cuisine,
    s.city,
    s.state,
    s.rating,
    s.is_active,
    NOW(),
    NULL,
    TRUE
FROM restaurants_stg s
LEFT JOIN dim_restaurant d ON s.restaurant_id = d.restaurant_id AND d.current_flag = TRUE
WHERE d.restaurant_id IS NULL
   OR d.restaurant_id IN (
        SELECT restaurant_id FROM (
            SELECT s2.restaurant_id
            FROM restaurants_stg s2
            JOIN dim_restaurant d2 ON s2.restaurant_id = d2.restaurant_id
            WHERE d2.current_flag = TRUE
              AND (
                   s2.restaurant_name != d2.restaurant_name OR s2.cuisine != d2.cuisine OR
                   s2.city != d2.city OR s2.state != d2.state OR
                   s2.rating != d2.rating OR s2.is_active != d2.is_active
                  )
        ) AS changed
    );

-- 13. SCD TYPE 2 — DIM_COURIER
UPDATE dim_courier d
JOIN (
    SELECT s.courier_id
    FROM couriers_stg s
    INNER JOIN dim_courier d ON s.courier_id = d.courier_id
    WHERE d.current_flag = TRUE
      AND (
           s.courier_name != d.courier_name OR s.vehicle_type != d.vehicle_type OR
           s.phone != d.phone OR s.city != d.city OR s.state != d.state OR
           s.is_active != d.is_active
          )
) changed ON d.courier_id = changed.courier_id
SET 
    d.valid_to = NOW(),
    d.current_flag = FALSE;

INSERT INTO dim_courier (
    courier_id, courier_name, vehicle_type, phone, city, state, is_active,
    valid_from, valid_to, current_flag
)
SELECT 
    s.courier_id,
    s.courier_name,
    s.vehicle_type,
    CASE 
        WHEN s.phone = 'Unknown' THEN 'Unknown'
        ELSE CONCAT('***-***-', RIGHT(s.phone, 4))
    END AS phone,
    s.city,
    s.state,
    s.is_active,
    NOW(),
    NULL,
    TRUE
FROM couriers_stg s
LEFT JOIN dim_courier d ON s.courier_id = d.courier_id AND d.current_flag = TRUE
WHERE d.courier_id IS NULL
   OR d.courier_id IN (
        SELECT courier_id FROM (
            SELECT s2.courier_id
            FROM couriers_stg s2
            JOIN dim_courier d2 ON s2.courier_id = d2.courier_id
            WHERE d2.current_flag = TRUE
              AND (
                   s2.courier_name != d2.courier_name OR s2.vehicle_type != d2.vehicle_type OR
                   s2.phone != d2.phone OR s2.city != d2.city OR s2.state != d2.state OR
                   s2.is_active != d2.is_active
                  )
        ) AS changed
    );

-- 14. LOAD FACT TABLES

-- fact_orders
INSERT IGNORE INTO fact_orders (order_id, sk_customer, sk_restaurant, date_id, total_amount, quantity)
SELECT 
    o.order_id,
    dc.sk_customer,
    dr.sk_restaurant,
    DATE(o.order_datetime) AS date_id,
    COALESCE(oi_agg.total_amount, 0) AS total_amount,
    COALESCE(oi_agg.quantity, 0) AS quantity
FROM orders_stg o
INNER JOIN dim_customer dc 
    ON o.customer_id = dc.customer_id AND dc.current_flag = TRUE
INNER JOIN dim_restaurant dr 
    ON o.restaurant_id = dr.restaurant_id AND dr.current_flag = TRUE
LEFT JOIN (
    SELECT 
        order_id,
        SUM(CAST(line_amount AS DECIMAL(10,2))) AS total_amount,
        SUM(CAST(quantity AS UNSIGNED)) AS quantity
    FROM order_items_stg
    GROUP BY order_id
) oi_agg ON o.order_id = oi_agg.order_id
WHERE o.order_datetime IS NOT NULL;

-- fact_deliveries
-- Step 1: Deduplicate deliveries_stg
CREATE TABLE deliveries_stg_dedup AS
SELECT 
    delivery_id, order_id, courier_id, assign_time, drop_time, distance_km
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY delivery_id 
               ORDER BY drop_time DESC, assign_time DESC  -- keep latest/most complete
           ) AS rn
    FROM deliveries_stg
) t
WHERE rn = 1;

-- Step 2: Replace deliveries_stg
DROP TABLE deliveries_stg;
RENAME TABLE deliveries_stg_dedup TO deliveries_stg;

INSERT IGNORE INTO fact_deliveries (
    delivery_id, order_id, sk_courier, delivery_date, distance_km, delivery_time_minutes
)
SELECT 
    d.delivery_id,
    d.order_id,
    dc.sk_courier,
    DATE(d.drop_time) AS delivery_date,
    d.distance_km,
    TIMESTAMPDIFF(MINUTE, d.assign_time, d.drop_time) AS delivery_time_minutes
FROM deliveries_stg d
INNER JOIN fact_orders fo ON d.order_id = fo.order_id
INNER JOIN dim_courier dc 
    ON d.courier_id = dc.courier_id AND dc.current_flag = TRUE
WHERE d.drop_time IS NOT NULL 
  AND d.assign_time IS NOT NULL;

  
-- 15. FINAL AUDIT LOG
INSERT INTO etl_audit_log (step, table_name, records_in, records_out, errors_handled, description)
VALUES
('DW Load', 'dim_customer', (SELECT COUNT(*) FROM customers_stg), (SELECT COUNT(*) FROM dim_customer WHERE current_flag = TRUE), 0, 'SCD2 Full Load'),
('DW Load', 'dim_restaurant', (SELECT COUNT(*) FROM restaurants_stg), (SELECT COUNT(*) FROM dim_restaurant WHERE current_flag = TRUE), 0, 'SCD2 Full Load'),
('DW Load', 'dim_courier', (SELECT COUNT(*) FROM couriers_stg), (SELECT COUNT(*) FROM dim_courier WHERE current_flag = TRUE), 0, 'SCD2 Full Load'),
('DW Load', 'fact_orders', (SELECT COUNT(*) FROM orders_stg), (SELECT COUNT(*) FROM fact_orders), 0, 'Fact loaded'),
('DW Load', 'fact_deliveries', (SELECT COUNT(*) FROM deliveries_stg), (SELECT COUNT(*) FROM fact_deliveries), 0, 'Fact loaded');

-- 16. ANALYTICAL QUERIES (Day 1 Validation)
-- Average Order Value
SELECT AVG(total_amount) AS aov FROM fact_orders;

-- Avg Delivery Time
SELECT AVG(delivery_time_minutes) AS avg_delivery_time FROM fact_deliveries;

-- On-Time Rate (<30 min)
SELECT 
    100.0 * SUM(CASE WHEN delivery_time_minutes <= 30 THEN 1 ELSE 0 END) / COUNT(*) AS on_time_pct
FROM fact_deliveries;

-- Top Cuisines by Revenue
SELECT 
    dr.cuisine,
    SUM(fo.total_amount) AS revenue
FROM fact_orders fo
JOIN dim_restaurant dr ON fo.sk_restaurant = dr.sk_restaurant
GROUP BY dr.cuisine
ORDER BY revenue DESC
LIMIT 10;

-- Courier Performance
SELECT 
    dc.courier_name,
    COUNT(fd.delivery_id) AS deliveries,
    AVG(fd.delivery_time_minutes) AS avg_time
FROM fact_deliveries fd
JOIN dim_courier dc ON fd.sk_courier = dc.sk_courier
GROUP BY dc.sk_courier, dc.courier_name
ORDER BY deliveries DESC
LIMIT 10;

-- Retention Rate
SELECT 
    100.0 * COUNT(DISTINCT CASE WHEN order_cnt > 1 THEN customer_id END) / COUNT(DISTINCT customer_id) AS retention_pct
FROM (
    SELECT dc.customer_id, COUNT(fo.order_id) AS order_cnt
    FROM fact_orders fo
    JOIN dim_customer dc ON fo.sk_customer = dc.sk_customer
    GROUP BY dc.customer_id
) t;
