--=======================================================
--Quality Check 'silver.crm_cust_info'
--=======================================================

--Checking for duplicate primary key
select cst_id,COUNT(*) from silver.crm_cust_info
GROUP BY cst_id HAVING COUNT(*) > 1 

--Checking unwanted spaces
select * from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select * from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select * from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

--Checking distinct values for marital Status
select Distinct(cst_marital_status) from silver.crm_cust_info

--Checking distinct values for gender
select Distinct(cst_gndr) from silver.crm_cust_info

--=======================================================
--Quality Check 'silver.crm_prd_info'
--=======================================================

select * from bronze.crm_prd_info

--checking for duplicate primary key
 select prd_id,count(*) from silver.crm_prd_info
 group by prd_id HAving count(*) > 1

 --checking for unwanted spaces in prd_key
 select * from silver.crm_prd_info
 where prd_key != TRIM(prd_key)

 --check all records where start_date is greater than end_date
select * from silver.crm_prd_info
where prd_start_dt>prd_end_dt

--checking if cat_id exists in bronze.erp_px_cat_g1v2
select * from silver.crm_prd_info
where cat_id IN (select id from bronze.erp_px_cat_g1v2)

--checking if prd_key exists in bronze.crm_sales_details
select * from silver.crm_prd_info
where prd_key IN (select sls_prd_key  from bronze.crm_sales_details)

--checking for spaces in prd_nm
select * from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

--checking for NULL values in prd_cost
select * from silver.crm_prd_info 
Where prd_cost IS NULL OR prd_cost < 0

--checking DISTINCT VALUES for prd_line
select DISTINCT(prd_line) from silver.crm_prd_info

--checking for records where start_date is greater than end date
select * from silver.crm_prd_info
where prd_start_dt > prd_end_dt


--=======================================================
--Quality Check 'silver.crm_sales_details'
--=======================================================

select * from bronze.crm_sales_details
where sls_ord_num = 'SO55367'

--checking for unwanted spaces in sls_order_num
select * from silver.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num) or sls_ord_num IS NULL

--checking for unwanted spaces in sls_prd_key
select * from silver.crm_sales_details
where sls_prd_key != TRIM(sls_prd_key) or sls_prd_key IS NULL

--checking if we are able to join silver.crm_prd_info
select * from silver.crm_sales_details
where sls_prd_key IN (select prd_key from silver.crm_prd_info)

--checking if we are able to join silver.crm_cust_info
select * from silver.crm_sales_details
where sls_cust_id IN (select cst_id  from silver.crm_cust_info)

--checking for NULL and negatives in sls_cust_id
select * from silver.crm_sales_details
where sls_cust_id IS NULL or sls_cust_id < 0

--checking for date quality sls_order_dt

select * from silver.crm_sales_details
where sls_order_dt IS NULL

--checking for date quality sls_ship_dt

select * from silver.crm_sales_details
where  sls_ship_dt IS NULL

--checking for date quality sls_due_dt

select * from silver.crm_sales_details
where sls_due_dt IS NULL

--checking for sls_price quality
select * from silver.crm_sales_details
Where sls_price < 0 OR sls_price IS NULL

--checking for total sales by sls_quantity * sls_price

select * from silver.crm_sales_details
where sls_sales != (sls_quantity* sls_price)

--checking for sls_quantity
select * from silver.crm_sales_details
where sls_quantity<0 or sls_quantity IS NULL

--checking where sls_order_dt is greater than sls_ship_dt

select * from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt

--checking where sls_ship_dt is greater than sls_due_dt

select * from silver.crm_sales_details
where sls_ship_dt>sls_due_dt

--=======================================================
--Quality Check 'silver.erp_cust_az12'
--=======================================================

--checking if we are able to join silver.crm_prd_info using cid

select * from silver.erp_cust_az12
where cid NOT IN (select cst_key from silver.crm_cust_info)

--checking for distinct values in gender

select distinct(gen) from silver.erp_cust_az12

--=======================================================
--Quality Check 'silver.erp_loc_a101'
--=======================================================

select * from bronze.erp_loc_a101
select * from silver.crm_cust_info

--Checking if we are able to join silver.erp_loc_a101 with silver.crm_cust_info
select * from silver.erp_loc_a101
where cid IN (select cst_key from silver.crm_cust_info)

--checking for any unwanted spaces in cid
select * from silver.erp_loc_a101
where cid != TRIM(cid)

--checking distinct values for cntry
select DISTINCT(cntry) from silver.erp_loc_a101
select * from silver.erp_loc_a101
where cntry = ''
 
--=======================================================
--Quality Check 'silver.erp_px_cat_g1v2'
--=======================================================

--checking for any unwanted spaces in id

select * from silver.erp_px_cat_g1v2
where id != TRIM(id)

--checking if we are able to join silver.erp_px_cat_g1v2 with silver.crm_prd_info 

select * from silver.erp_px_cat_g1v2
where id IN (select cat_id from silver.crm_prd_info)

--checking for distinct values in cat

select DISTINCT(cat) from silver.erp_px_cat_g1v2

--checking for any unwanted spaces in cat

select * from silver.erp_px_cat_g1v2
where cat != Trim(cat)

--checking for distinct values in subcat

select DISTINCT(subcat) from silver.erp_px_cat_g1v2

--checking for any unwanted spaces in subcat

select * from silver.erp_px_cat_g1v2
where subcat != Trim(subcat)

--checking for distinct values in maintenance

select DISTINCT(maintenance) from silver.erp_px_cat_g1v2


