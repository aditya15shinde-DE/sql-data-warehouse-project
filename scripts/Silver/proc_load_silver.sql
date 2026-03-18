create or alter procedure silver.load_silver AS
BEGIN

	--=====================================================
	-----loading for silver.crm_cust_info
	--=====================================================

	TRUNCATE table silver.crm_cust_info
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select  
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE WHEN cst_marital_status = 'M' THEN 'Married'
		 ELSE 'Single'
	END AS cst_marital_status,
	CASE WHEN cst_gndr = 'F' THEN 'Female'
		 WHEN cst_gndr = 'M' THEN 'Male'
		 ELSE cst_gndr
	END AS cst_gndr,
	cst_create_date
	from(
	select 
	ROW_NUMBER() OVER( PARTITION BY cst_id order by cst_create_date DESC) as rown,
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date

	from bronze.crm_cust_info
	where cst_id IS NOT NULL
	) as x
	where rown=1


	--=====================================================
	--loading for silver.crm_prd_info
	--=====================================================

	TRUNCATE table silver.crm_prd_info
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
	select 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE prd_line
	END AS prd_line,
	prd_start_dt,
	DATEADD(day,-1,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
	) AS prd_end_dt
	from bronze.crm_prd_info



	--=====================================================
	--loading for silver.crm_sales_details
	--=====================================================

	TRUNCATE table silver.crm_sales_details
	insert into silver.crm_sales_details(
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
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8  THEN NULL
		 ELSE CAST(CAST(sls_order_dt as varchar) as DATE)
	END AS sls_order_dt,
	CAST(CAST(sls_ship_dt as varchar) as DATE) as sls_ship_dt,
	CAST(CAST(sls_due_dt as varchar) as date) as sls_due_dt,
	CASE WHEN sls_sales != (sls_quantity* sls_price) OR sls_sales<0 THEN (sls_quantity*ABS(sls_price))
		 ELSE ABS(sls_sales)
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL THEN (sls_sales/sls_quantity)
		 ELSE ABS(sls_price) 
	END as sls_price
	from bronze.crm_sales_details 


	--=====================================================
	--loading for silver.erp_cust_az12
	--=====================================================

	TRUNCATE table silver.erp_cust_az12
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
	CASE WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
		 WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
		 ELSE NULL
	END AS gen
	from bronze.erp_cust_az12 

	--=====================================================
	--loading for silver.erp_loc_a101
	--=====================================================

	TRUNCATE table silver.erp_loc_a101
	insert into silver.erp_loc_a101(
	cid,
	cntry
	)

	select 
	REPLACE(cid,'-','') as cid,
	CASE WHEN cntry IN ('DE','Germany') THEN 'Germany'
		 WHEN cntry IN ('USA','United States','US') THEN 'United States'
		 WHEN cntry = '' THEN NULL
		 ELSE cntry
	END AS cntry
	from bronze.erp_loc_a101 


	--=====================================================
	--loading for silver.erp_px_cat_g1v2
	--=====================================================

	TRUNCATE table silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2(
	id,cat,subcat,maintenance
	)
	select * from bronze.erp_px_cat_g1v2
END