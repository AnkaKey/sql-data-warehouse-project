/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standartization & Consistency (Check the consistancy of values in low cardinality columns)
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;
  
-------------------------------------------------------------------------------

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >  1 OR prd_id IS NULL;

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2; -- to check if there are some not matching categories in table crm.prod_info

-- Check for unwanted spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standartization & Consistency (Check the consistancy of values in low cardinality columns)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * FROM silver.crm_prd_info;

-------------------------------------------------------------------------------

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
  
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt

SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data consistency: Between Sales, Quantity, and Price
-- Expectation: No Results
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.
--->>> RULES: If Sales is negative, zero, or NULL, derive it using Quntity and Price.
--->>> RULES: If Price is zero or NULL, calculate it using Sales and Quantity.
--->>> RULES: If Price is negative, convert it to a positive value.

SELECT DISTINCT 
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;

-------------------------------------------------------------------------------

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standartization & Consistansy
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

-------------------------------------------------------------------------------

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Data Standardization & Consistency
SELECT 
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM silver.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info) 

-- Data Standartization & Consistancy
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT * FROM silver.erp_loc_a101

-------------------------------------------------------------------------------

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2 
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Check Standartization & Consistansy
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2
