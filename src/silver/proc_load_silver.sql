/*
=========================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================================================
Script Purpose:
	This stored procedure performs ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
	It performs the following actions:
		- Truncates the silver tables before loading data.
		- Inserts transformed and cleansed data from Bronze into Silver Tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;

=========================================================
*/

EXEC silver.load_silver


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batch_start DATETIME, @batch_end DATETIME, @start DATETIME, @end DATETIME
	BEGIN TRY
				SET @batch_start = GETDATE()
				PRINT '==============================================';
				PRINT 'Loading SILVER Layer';
				PRINT '==============================================';
				-- Truncate the table // Wipe it clean.
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.crm_cust_info'
				TRUNCATE TABLE silver.crm_cust_info;
				-- Columns to Insert
				PRINT '>> Inserting Data into silver.crm_cust_info'
				INSERT INTO silver.crm_cust_info(
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date
				)
				-- Data transformations using the bronze layer to be inserted into silver layer table
				SELECT
					cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname, -- Remove Unwanted Spaces
					TRIM(cst_lastname) as cst_lastname,
					CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- Data Normalization/Standarization: Mapping values to readable values
						 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						 ELSE 'n/a' -- Handling NULLs/Missing Values
					END cst_marital_status,
					CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						 ELSE 'n/a'
					END cst_gndr,
					cst_create_date
				FROM (
					SELECT
						*,
						ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) [flag_last] -- Removing Duplicates from PK
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
				)t WHERE flag_last = 1 -- Select the most recent record per customer
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'

				-- Table: silver.crm_prd_info
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.crm_prd_info'
				TRUNCATE TABLE silver.crm_prd_info;
				PRINT '>> Inserting Data into silver.crm_prd_info'
				INSERT INTO silver.crm_prd_info(
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
				)

				SELECT
					TRY_CAST(prd_id AS INT) AS [prd_id],
					REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS [cat_id], -- Product Category ID
					SUBSTRING(prd_key,7,LEN(prd_key)) AS [prd_key], -- Dynamic ending length // Extract product key
					prd_nm,
					ISNULL(TRY_CAST(prd_cost AS INT),0) AS prd_cost, 
					CASE UPPER(TRIM(prd_line))
						 WHEN 'M' THEN 'Mountain'
						 WHEN 'R' THEN 'Road'
						 WHEN 'S' THEN 'other Sales'
						 WHEN 'T' THEN 'Touring'
						 ELSE 'n/a' -- handle NULL by using n/a
					END [prd_line], -- Map product line codes to descriptive values
					CAST(prd_start_dt AS DATE) AS [prd_start_dt],
					DATEADD(
					day, -1, LEAD(TRY_CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY TRY_CAST(prd_start_dt AS DATE) ASC)
					) AS prd_end_dt -- calculate end date as one day before the next start date // data enrichment
				FROM bronze.crm_prd_info
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'



				-- Table silver.crm_sales_details
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.crm_sales_details'
				TRUNCATE TABLE silver.crm_sales_details;
				PRINT '>> Inserting Data into silver.crm_sales_details'
				INSERT INTO silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,	
					sls_price
				)

				SELECT
					sls_ord_num,
					sls_prd_key,
					TRY_CAST(sls_cust_id AS INT),
					CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Handling invalid Data
						 ELSE TRY_CAST(sls_order_dt AS DATE) -- Data type Casting
					END sls_order_dt,
					CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
						 ELSE TRY_CAST(sls_ship_dt AS DATE)
					END sls_ship_dt,
					CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
						 ELSE TRY_CAST(sls_due_dt AS DATE)
					END sls_due_dt,
					CASE WHEN sls_sales IS NULL OR TRY_CAST(sls_sales AS INT) <= 0 OR TRY_CAST(sls_sales AS INT) != TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS INT))
						 THEN TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS INT))
						 ELSE TRY_CAST(sls_sales AS INT)
					END  sls_sales, -- Recalculate sales if original value is missing or incorrect
					TRY_CAST(sls_quantity AS INT),
					CASE WHEN TRY_CAST(sls_price AS INT) < 0 THEN ABS(sls_price)
						 WHEN TRY_CAST(sls_price AS INT) = 0 OR sls_price IS NULL THEN TRY_CAST(sls_sales AS INT) / NULLIF(TRY_CAST(sls_quantity AS INT),0)
						 ELSE TRY_CAST(sls_price AS INT) 
					END  sls_price -- Derive price if original value is invalid
				FROM bronze.crm_sales_details
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'


				-- Table: bronze.erp_cust_az12
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.erp_cust_az12'
				TRUNCATE TABLE silver.erp_cust_az12;
				PRINT '>> Inserting Data into silver.erp_cust_az12'
				-- Truncate then Insert
				INSERT INTO silver.erp_cust_az12(
					CID,
					BDATE,
					GEN
				)

				SELECT
					CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
						 ELSE cid
					END cid, -- Remove 'NAS' prefix if present

					CASE WHEN TRY_CAST(bdate AS DATE) > GETDATE() THEN NULL
						 ELSE TRY_CAST(bdate AS DATE)
					END bdate, -- Set future birthdates to NULL 

					CASE
						WHEN  UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
						WHEN  UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female' 
						ELSE 'n/a'
					END gen -- Normalize gender values and handle unknown cases
				FROM bronze.erp_cust_az12
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'


				-- Table: silver.erp_loc_a101
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.erp_loc_a101'
				TRUNCATE TABLE silver.erp_loc_a101;
				-- Insert
				PRINT '>> Inserting Data into silver.erp_loc_a101'
				INSERT INTO silver.erp_loc_a101(
					cid,
					cntry
				)
				SELECT
					Replace(cid,'-','') AS cid, -- Handle invalid values to match keys
					CASE
						WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'DE'
						WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'USA'
						WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA','AU') THEN 'AU'
						WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM','UK') THEN 'UK'
						WHEN UPPER(TRIM(cntry)) IN ('CANADA','CN') THEN 'CN'
						WHEN UPPER(TRIM(cntry)) IN ('FRANCE','FR') THEN 'FR'
						WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
						ELSE TRIM(cntry)
					END cntry -- Normalize and Handle missing or blank country codes
				FROM bronze.erp_loc_a101
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'

				-- Table: silver.erp_px_cat_g1v2
				PRINT '==================================='
				SET @start = GETDATE()
				PRINT '>> Truncating silver.erp_px_cat_g1v2'
				TRUNCATE TABLE silver.erp_px_cat_g1v2;
				-- Insert
				PRINT '>> Inserting Data into silver.erp_px_cat_g1v2'
				INSERT INTO silver.erp_px_cat_g1v2(
					ID,
					CAT,
					SUBCAT,
					MAINTENANCE
				)

				SELECT
					ID,
					TRIM(CAT) AS CAT,
					TRIM(SUBCAT) AS SUBCAT,
					TRIM(MAINTENANCE) AS MAINTENANCE
				FROM bronze.erp_px_cat_g1v2
				PRINT '==================================='
				SET @end = GETDATE()
				PRINT '---------------------------'
				PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start,@end) AS NVARCHAR) + ' seconds';
				PRINT '---------------------------'
				SET @batch_end = GETDATE()
				PRINT '/////////////////////'
				PRINT 'Loading SILVER Layer is Completed';
				PRINT 'Total Load Time: ' + CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR) + 'seconds'
				PRINT '/////////////////////'
	END TRY
	BEGIN CATCH
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>'
	PRINT 'ERROR: Silver Stored Procedure Failed'
	PRINT 'Error Message: ' + ERROR_MESSAGE()
	PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'Erro State: ' + CAST(ERROR_STATE() AS NVARCHAR)
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>'
	END CATCH
END