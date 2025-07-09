# Dataradian_customer_pos_snowflake_dbt
assesment project using snowflake and DBT to transform given data set.
## 🎯 Objective

Clean and standardize customer POS data by:
- Handling inconsistent date formats
- Extracting product/brand info from SKU
- Converting text fields to appropriate data types
- Managing null and blank values

---
created snowflake DB and schemas and uploaded the csv data into the schema
## 🧪 Source Table

- **Database:** `BRONZE`
- **Schema:** `POS`
- **Table:** `CUSTOMER_POS`

---

## 📦 Target Table

- **Database:** `SILVER`
- **Schema:** `TRANSFORMED_POS`
- **Table:** `TRANSFORM_CUSTOMER_POS` (set using dbt `alias` config)

---

## 🔁 Transformation Logic Summary

| Field             | Transformation                                                                 |
|------------------|----------------------------------------------------------------------------------|
| `date`           | Parsed using `TRY_TO_DATE()` with 3 format fallbacks and handled empty strings   |
| `brand`          | Extracted first 3 chars from `sku` if valid (`ABC`, `DEF`, `GHI`)                |
| `product`        | Extracted substring from `sku` after brand using `SUBSTRING()` logic             |
| `pos_units`      | Cast to integer using `TRY_CAST()`                                               |
| `sales`          | Cast to float and defaulted to `0.0` on null/invalid using `COALESCE()`          |
| `units_on_hand`  | Cast to integer and defaulted to `0` on null/invalid using `COALESCE()`          |

---

## 🧪 Data Validation

- Sample transformed output validated with test CSVs
- Confirmed:
  - No blank `date` values (proper parsing)
  - Correct brand filtering
  - Product extraction logic behaves as expected
  - Missing/blank `sales` and `units_on_hand` default to `0` or `0.0`
