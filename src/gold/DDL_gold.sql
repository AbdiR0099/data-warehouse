/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

-- Dimension Customers

CREATE OR ALTER VIEW gold.dim_customers AS(
SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate Key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		lc.CNTRY AS country,
		ci.cst_marital_status AS marital_status,
		CASE
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
			ELSE COALESCE(az.gen,'n/a')
		END gender,
		az.BDATE AS birthdate,
		ci.cst_create_date AS create_date
FROM		silver.crm_cust_info ci
LEFT JOIN	silver.erp_cust_az12 az
ON			ci.cst_key = az.CID 
LEFT JOIN	silver.erp_loc_a101 lc
ON			ci.cst_key = lc.CID 
)

-- Dimension Products

CREATE OR ALTER VIEW gold.dim_products AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key, -- (Create a surrogate key for dimension)
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	px.CAT AS category,
	px.SUBCAT AS subcategory,
	px.MAINTENANCE AS maintenance,
	pr.prd_cost AS cost,
	prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.ID
WHERE pr.prd_end_dt IS NULL -- If End Date is NULL then it is the Current Info of the Product // Filter out all historical data
)

-- Fact Sales
-- Order of the Columns: Dimension Keys // Dates // Measures
CREATE OR ALTER VIEW gold.fact_sales AS (
SELECT
	sd.sls_ord_num AS order_number, 
	pr.product_key, -- building fact: use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions
	cs.customer_key, 
	sd.sls_order_dt AS order_date, 
	sd.sls_ship_dt AS shipping_date, 
	sd.sls_due_dt AS due_date, 
	sd.sls_sales AS sales_amount, 
	sd.sls_quantity AS quantity, 
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cs
ON sd.sls_cust_id = cs.customer_id
)
