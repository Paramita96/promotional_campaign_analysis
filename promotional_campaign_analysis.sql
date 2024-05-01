 SELECT dp.product_code, dp.product_name ,fe.base_price, fe.promo_type
FROM retail_events_db.fact_events fe
JOIN retail_events_db.dim_products dp ON fe.product_code = dp.product_code
WHERE fe.base_price > 500
AND fe.promo_type = 'BOGOF'
LIMIT 2;
------------------------------------------------------------------------------------
SELECT city, COUNT(store_id) AS store_count
FROM retail_events_db.dim_stores
GROUP BY city
ORDER BY store_count DESC;
------------------------------------------------------------------------------------
ALTER TABLE retail_events_db.fact_events
ADD COLUMN promo_price INT DEFAULT 0;

SET SQL_SAFE_UPDATES = 0;

UPDATE retail_events_db.fact_events
SET promo_price = 
    CASE 
        WHEN promo_type = '25% off' THEN base_price * 0.75
        WHEN promo_type = '50% off' THEN base_price * 0.5
        WHEN promo_type = '33% off' THEN base_price * 0.67
        WHEN promo_type = '500 cashback' THEN base_price - 500
        ELSE base_price
    END;


USE retail_events_db;

SELECT 
    dc.campaign_name,
    CONCAT(FORMAT(SUM(fe.quantity_sold_before_promo * fe.base_price) / 1000000, 2), 'M') AS total_revenue_before_promo,
    CONCAT(FORMAT(SUM(fe.quantity_sold_after_promo * fe.promo_price) / 1000000, 2), 'M') AS total_revenue_after_promo
FROM 
    dim_campaigns dc
JOIN 
    fact_events fe ON dc.campaign_id = fe.campaign_id
GROUP BY 
    dc.campaign_name;
    ----------------------------------------------------------------------------------------------------------
    WITH Diwali_campaign_sale AS (
    SELECT 
        category,
        ROUND(
            SUM(
                CASE 
                    WHEN promo_type = 'BOGOF' THEN `quantity_sold_after_promo` * 2
                    ELSE `quantity_sold_after_promo`
                END - `quantity_sold_before_promo`
            ) * 100 / SUM(`quantity_sold_before_promo`),
            2
        ) AS `ISU%`
    FROM 
        fact_events 
    JOIN 
        dim_products USING (product_code)
    JOIN 
        dim_campaigns USING (campaign_id)
    WHERE 
        campaign_name = 'Diwali'
    GROUP BY 
        category
)

SELECT 
    Category, 
    `ISU%`, 
    ROW_NUMBER() OVER (ORDER BY `ISU%` DESC) AS rank_order 
FROM 
    Diwali_campaign_sale;
    --------------------------------------------------------------------------------------------------------
    USE retail_events_db;

SELECT 
    dp.product_name,
    dp.category,
    (SUM(fe.quantity_sold_after_promo * fe.promo_price) - SUM(fe.quantity_sold_before_promo * fe.base_price)) / SUM(fe.quantity_sold_before_promo * fe.base_price) * 100 AS IR_percentage
FROM 
    fact_events fe
JOIN 
    dim_products dp ON fe.product_code = dp.product_code
GROUP BY 
    fe.product_code, dp.product_name, dp.category
ORDER BY 
    IR_percentage DESC
LIMIT 5;
-------------------------------------------------------------------------------------------------
USE retail_events_db;

SELECT 
    ds.store_id,
    ds.city,
    (SUM(fe.quantity_sold_after_promo * fe.promo_price) - SUM(fe.quantity_sold_before_promo * fe.base_price)) AS IR
FROM 
    fact_events fe
JOIN 
    dim_stores ds ON fe.store_id = ds.store_id
GROUP BY 
    ds.store_id, ds.city
ORDER BY 
    IR DESC
LIMIT 10;
---------------------------------------------------------------
USE retail_events_db;

SELECT 
    ds.store_id,
    ds.city,
    (SUM(fe.quantity_sold_after_promo) - SUM(fe.quantity_sold_before_promo)) AS ISU
FROM 
    fact_events fe
JOIN 
    dim_stores ds ON fe.store_id = ds.store_id
GROUP BY 
    ds.store_id, ds.city
ORDER BY 
    ISU ASC
LIMIT 10;
-------------------------------------------------------------
USE retail_events_db;

SELECT 
    ds.city,
    AVG(fe.quantity_sold_after_promo * fe.promo_price - fe.quantity_sold_before_promo * fe.base_price) AS avg_IR,
    AVG(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS avg_ISU,
    COUNT(*) AS total_stores
FROM 
    fact_events fe
JOIN 
    dim_stores ds ON fe.store_id = ds.store_id
GROUP BY 
    ds.city
ORDER BY 
    avg_IR DESC;
--------------------------------------------------------------
USE retail_events_db;

SELECT 
    fe.promo_type,
    SUM(fe.quantity_sold_after_promo * fe.promo_price - fe.quantity_sold_before_promo * fe.base_price) AS total_IR
FROM 
    fact_events fe
GROUP BY 
    fe.promo_type
ORDER BY 
    total_IR DESC
LIMIT 2;
-------------------------------------------------------------------
USE retail_events_db;

SELECT 
    fe.promo_type,
    SUM(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS total_ISU
FROM 
    fact_events fe
WHERE 
    fe.promo_type IS NOT NULL
GROUP BY 
    fe.promo_type
ORDER BY 
    total_ISU ASC
LIMIT 2;
-----------------------------------------------------------
USE retail_events_db;

-- Calculate ISU and IR for discount-based promotions
WITH Discount_Promotions AS (
    SELECT 
        fe.promo_type,
        SUM(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS total_ISU,
        SUM((fe.quantity_sold_after_promo * fe.promo_price) - (fe.quantity_sold_before_promo * fe.base_price)) AS total_IR
    FROM 
        fact_events fe
    WHERE 
        fe.promo_type LIKE '%off%' -- Assuming discount-based promotions have 'off' in their names
    GROUP BY 
        fe.promo_type
),

-- Calculate ISU and IR for BOGOF and cashback promotions
NonDiscount_Promotions AS (
    SELECT 
        fe.promo_type,
        SUM(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS total_ISU,
        SUM((fe.quantity_sold_after_promo * fe.promo_price) - (fe.quantity_sold_before_promo * fe.base_price)) AS total_IR
    FROM 
        fact_events fe
    WHERE 
        fe.promo_type NOT LIKE '%off%' -- Exclude discount-based promotions
    GROUP BY 
        fe.promo_type
)

-- Combine the results of discount-based and non-discount promotions
SELECT 
    'Discount-Based' AS promotion_type,
    SUM(total_ISU) AS total_ISU_discount,
    SUM(total_IR) AS total_IR_discount
FROM 
    Discount_Promotions

UNION

SELECT 
    'BOGOF/Cashback' AS promotion_type,
    SUM(total_ISU) AS total_ISU_nondiscount,
    SUM(total_IR) AS total_IR_nondiscount
FROM 
    NonDiscount_Promotions;
    --------------------------------------------------------------------
    USE retail_events_db;

-- Calculate ISU and IR for each promotion type
WITH Promotion_Performance AS (
    SELECT 
        fe.promo_type,
        SUM(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS total_ISU,
        SUM((fe.quantity_sold_after_promo * fe.promo_price) - (fe.quantity_sold_before_promo * fe.base_price)) AS total_IR
    FROM 
        fact_events fe
    GROUP BY 
        fe.promo_type
)

-- Calculate ISU to IR ratio for each promotion type
SELECT 
    promo_type,
    total_ISU,
    total_IR,
    ROUND(total_ISU / NULLIF(total_IR, 0), 2) AS ISU_to_IR_Ratio
FROM 
    Promotion_Performance
ORDER BY 
    ISU_to_IR_Ratio DESC;
---------------------------------------------------------------------
USE retail_events_db;

-- Calculate the lift in sales for each product category
SELECT 
    dp.category,
    SUM(fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) AS lift_in_sales
FROM 
    fact_events fe
JOIN 
    dim_products dp ON fe.product_code = dp.product_code
GROUP BY 
    dp.category
ORDER BY 
    lift_in_sales DESC;
    ------------------------------------------------------------------
    USE retail_events_db;

-- Calculate the percentage change in sales for each product
SELECT 
    dp.product_name,
    (SUM(fe.quantity_sold_after_promo) - SUM(fe.quantity_sold_before_promo)) / SUM(fe.quantity_sold_before_promo) * 100 AS sales_change_percentage
FROM 
    fact_events fe
JOIN 
    dim_products dp ON fe.product_code = dp.product_code
GROUP BY 
    dp.product_name
ORDER BY 
    sales_change_percentage DESC;
    -------------------------------------------------------------------
    USE retail_events_db;

-- Calculate the average ISU% for each combination of product category and promotion type
SELECT 
    dp.category,
    fe.promo_type,
    AVG((fe.quantity_sold_after_promo - fe.quantity_sold_before_promo) / fe.quantity_sold_before_promo * 100) AS avg_ISU_percentage
FROM 
    fact_events fe
JOIN 
    dim_products dp ON fe.product_code = dp.product_code
GROUP BY 
    dp.category, fe.promo_type;
