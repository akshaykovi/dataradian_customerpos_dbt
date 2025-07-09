{{ config(alias='transform_customer_pos') }}
WITH source AS (
  SELECT * FROM BRONZE.POS.CUSTOMER_POS
),

cleaned AS (
  SELECT
    channel,

    -- 1. Standardize multiple date formats (to yyyy-mm-dd)
    COALESCE(
          TRY_TO_DATE(NULLIF(date, ''), 'DD-MM-YYYY'),
          TRY_TO_DATE(NULLIF(date, ''), 'DD-Mon-YY'),
          TRY_TO_DATE(NULLIF(date, ''), 'YYYY-MM-DD')
        ) AS date,

    division,

    -- 2. Extract BRAND (first 3 letters only if valid)
    CASE 
      WHEN UPPER(LEFT(sku, 3)) IN ('ABC', 'DEF', 'GHI') THEN UPPER(LEFT(sku, 3))
      ELSE NULL
    END AS brand,

    -- 3. Extract PRODUCT (digits after brand prefix alphanumeric)
    CASE 
      WHEN POSITION(' ', sku) > 3 THEN SUBSTRING(sku, 4, POSITION(' ', sku) - 4)
      ELSE NULL
    END AS product,

    -- 4. Cast POS_UNITS to integer
    TRY_CAST(pos_units AS INTEGER) AS pos_units,
    
    partner,
    sales_org,
    
    -- 5. Cast SALES_ to float and rename to sales
    COALESCE(TRY_CAST(sales_ AS FLOAT), 0.0) AS sales,
    
    -- 6. Cast UNITS_ON_HAND to integer
    COALESCE(TRY_CAST(units_on_hand AS INTEGER), 0) AS units_on_hand,
    
    store

  FROM source
)

SELECT * FROM cleaned
