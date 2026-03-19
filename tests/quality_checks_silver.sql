/*
==============================================================
Data Quality Checks
==============================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy and standardization
	across the 'silver' schema. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields
	- Data standardization and consistency
	- Invalid date ranges and orders
	- Data consistency between related fields

Usage Notes:
	- Run these cheks after loading data into silver layer.
	- Investigate and resolve any discrepancies found during the checks
==============================================================
*/

SELECT * FROM bronze.crm_cust_info

-- Quality Check
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


SELECT * FROM silver.crm_cust_info

-- Table: bronze.crm_prd_info // Now check for silver (after transformations)
SELECT * FROM silver.crm_prd_info
-- Check PK for NULLs // Duplicates
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
SELECT prd_nm FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)

-- Check NULLs/Negative Numbers
SELECT prd_cost FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL

-- Low Cardinality Column, Check for distinct values
SELECT DISTINCT prd_line FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt
-- END DATE MUST NOT BE EARLIER THAN START DATE

-- Table: bronze.crm_sales_details
SELECT * FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT * FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- CHECK for Invalid Dates
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT 
NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- Order date must always be earlier than shipping date or due date
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Business Rule:
-- Sales = Quantitiy * Price
-- Negative, zeros, Nulls are not allowed!
SELECT DISTINCT
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR TRY_CAST(sls_sales AS INT) != TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS INT))
	 THEN TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS INT))
	 ELSE sls_sales
END  sls_sales,

sls_quantity,


CASE WHEN sls_price < 0 THEN ABS(sls_price)
	 WHEN sls_price = 0 OR sls_price IS NULL THEN TRY_CAST(sls_sales AS INT) / NULLIF(TRY_CAST(sls_quantity AS INT),0)
	 ELSE sls_price
END  sls_price

FROM silver.crm_sales_details
WHERE TRY_CAST(sls_sales AS INT) != TRY_CAST(sls_quantity AS INT) * TRY_CAST(sls_price AS INT)
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
-- SOLUTION 1: Data issues will be fixed direct in source system
-- SOLUTION 2: Data issues has to be fixed in data warehouse
-- RULES: 
-- If Sales is negative, zero or Null, derive it using Quantity and Price
-- If Price is zero or null, calculate it using Sales and Quantity
-- If Price is negative, covert it to positive value

-- Table: silver.erp_cust_az12
SELECT * FROM silver.crm_cust_info

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END cid


FROM bronze.erp_cust_az12 
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
			END NOT IN ( SELECT DISTINCT cst_key FROM silver.crm_cust_info) -- check PK FK keys from tables

-- Identify Out-of-Range Dates
-- Check for birthdays in the future or in the very distant past
SELECT 
CASE WHEN TRY_CAST(bdate AS DATE) > GETDATE() THEN NULL
	 ELSE bdate
	 END bdate
FROM silver.erp_cust_az12
WHERE TRY_CAST(bdate AS DATE) < '1925-01-01' OR TRY_CAST(bdate AS DATE) > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT GEN FROM silver.erp_cust_az12

SELECT DISTINCT
	CASE
		WHEN  UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
		WHEN  UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female' 
		ELSE 'n/a'
	END GEN

FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12

-- Table: silver.erp.loc_a101

SELECT
	cid,
	cntry
FROM silver.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key from silver.crm_cust_info)
SELECT cst_key from silver.crm_cust_info

-- Data Standardization & Consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE
	WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'DE'
	WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'USA'
	WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA','AU') THEN 'AU'
	WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM','UK') THEN 'UK'
	WHEN UPPER(TRIM(cntry)) IN ('CANADA','CN') THEN 'CN'
	WHEN UPPER(TRIM(cntry)) IN ('FRANCE','FR') THEN 'FR'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM 
silver.erp_loc_a101
ORDER BY cntry

-- Table: silver.erp_px_cat_g1v2

SELECT ID FROM bronze.erp_px_cat_g1v2

SELECT cat_id FROM silver.crm_prd_info 

SELECT ID FROM bronze.erp_px_cat_g1v2
WHERE ID NOT IN (
SELECT cat_id FROM silver.crm_prd_info )

-- Check for unwanted Spaces
SELECT
 *

WHERE cat != TRIM(cat)
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2
