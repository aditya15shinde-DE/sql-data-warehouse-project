Create Or ALTER Procedure bronze.load_bronze AS
Begin
    Declare @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DateTIME

	Begin Try
	        SET @batch_start_time = GETDATE()
			Print '======================================'
			Print 'Loading Bronze Layer'
			Print '======================================'

			Print '--------------------------------------'
			Print 'Loading CRM Tables'
			Print '--------------------------------------'
			
			SET @start_time = GETDATE() 
			Print '>>Truncating Table bronze.crm_cust_info'
			Truncate Table bronze.crm_cust_info
			Print 'Inserting Data Into:bronze.crm_cust_info'
			BULK INSERT bronze.crm_cust_info
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
			with (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK 
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'


			SET @start_time = GETDATE()
			Print '>>Truncating Table bronze.crm_prd_info'
			Truncate table bronze.crm_prd_info
			Print 'Inserting Data Into:bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
			with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  TABLOCK
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'

			
			SET @start_time = GETDATE()
			Print '>>Truncating Table bronze.crm_sales_details'
			Truncate table bronze.crm_sales_details
			Print 'Inserting Data Into:bronze.crm_sales_details'
			BULK INSERT bronze.crm_sales_details
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
			with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  TABLOCK
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'



			Print '--------------------------------------'
			Print 'Loading ERP Tables'
			Print '--------------------------------------'


			SET @start_time = GETDATE()
			Print '>>Truncating Table bronze.erp_cust_az12'
			Truncate table bronze.erp_cust_az12
			Print 'Inserting Data Into:bronze.erp_cust_az12'
			Bulk insert bronze.erp_cust_az12
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
			with(
			 firstrow = 2,
			 fieldterminator=',',
			 tablock
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'


			SET @start_time = GETDATE()
			Print '>>Truncating Table bronze.erp_loc_a101'
			Truncate table bronze.erp_loc_a101
			Print 'Inserting Data Into:bronze.erp_loc_a101'
			bulk insert bronze.erp_loc_a101
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
			with(
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'


			SET @start_time = GETDATE()
			Print '>>Truncating Table bronze.erp_px_cat_g1v2'
			truncate table bronze.erp_px_cat_g1v2
			Print 'Inserting Data Into:bronze.erp_px_cat_g1v2'
			bulk insert bronze.erp_px_cat_g1v2
			from 'G:\Data Engineering\SQL Data Warehouse Project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
			with(
			   firstrow = 2,
			   fieldterminator = ',',
			   tablock
			)
			SET @end_time = GETDATE()
			Print '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			Print '----------'

			SET @batch_end_time = GETDATE()
			Print '>> Batch Load Duration: ' + CAST(Datediff(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
	End Try
	Begin Catch
	        Print '====================================='
			Print 'Error Occured During Loading Bronze Layer'
			Print 'Error Message' + Error_message();
			Print 'Error Message' + CAST (Error_Number() AS Nvarchar)
			Print 'Error Message' + CAST (Error_State() AS Nvarchar)
			Print '====================================='
	END Catch
END
