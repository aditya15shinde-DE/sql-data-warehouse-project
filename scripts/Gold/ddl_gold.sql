create view gold.dim_customers AS
select
ROW_NUMBER() OVER (order by cst_id) as customer_key,
cst_id as customer_id,
cst_key as customer_number,
cst_firstname firstname,
cst_lastname lastname,
d3.cntry country,
cst_marital_status marital_status,
CASE WHEN cst_gndr IS NULL THEN gen
     ELSE cst_gndr
END AS gender,
bdate birthdate,
cst_create_date create_date
from silver.crm_cust_info as d1
LEFT JOIN silver.erp_cust_az12 as d2
ON d1.cst_key=d2.cid
LEFT JOIN silver.erp_loc_a101 as d3
ON d1.cst_key = d3.cid
GO


create view gold.dim_products AS
select
ROW_NUMBER() OVER (Order by prd_start_dt,prd_key) as product_key,
prd_id as product_id,
prd_key as product_number,
prd_nm as product_name,
cat_id as category_id,
cat category,
subcat subcategory,
maintenance ,
prd_cost cost,
prd_line product_line,
prd_start_dt start_date
from silver.crm_prd_info as d1
LEFT JOIN silver.erp_px_cat_g1v2 as d2
ON d1.cat_id=d2.id
where prd_end_dt IS NULL
GO

create view gold.fact_sales AS
select 
sls_ord_num as order_number,
d2.product_key,
d1.customer_key,
sls_order_dt order_date,
sls_ship_dt shipping_date,
sls_due_dt due_date,
sls_sales sales_amount,
sls_quantity quantity,
sls_price price
from silver.crm_sales_details as f
LEFT JOIN gold.dim_customers as d1
ON f.sls_cust_id=d1.customer_id
LEFT JOIN gold.dim_products as d2
ON f.sls_prd_key = d2.product_number
GO

