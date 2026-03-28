/*
Note: This script creates a stored procedure named "load_bronze" in the "bronze" schema. 
The procedure is designed to load data into the bronze layer of a data warehouse. 
It performs the following steps:
	1. Initializes variables to track the start and end times of the loading process, as well as the number of rows loaded.
	2. Uses a TRY-CATCH block to handle any errors that may occur during the loading process.
	3. Truncates the target tables in the bronze schema before loading new data.
	4. Uses the BULK INSERT command to load data from CSV files into the respective tables in the bronze schema.
	5. Prints messages to the console to indicate the progress of the loading process, including the time taken and the number of rows loaded for each table.
	6. If any errors occur during the loading process, the CATCH block will print the error message, line number, and error number to the console.
*/

-- create or alter the stored procedure to load data into the bronze layer
USE DataWarehouse;
GO


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@batch_start_time DATETIME, 
		@batch_end_time DATETIME,
		@rows_loaded INT

	SET NOCOUNT ON;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Load data into bronze.crm_cust_info
		SET @start_time = GETDATE();

		PRINT '>> Truncating bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.crm_cust_info);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.crm_cust_info in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';

		-- Load data into bronze.crm_prd_info
		SET @start_time = GETDATE();

		PRINT '>> Truncating bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		
		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.crm_prd_info);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.crm_prd_info in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';

		
		
		-- Load data into bronze.crm_saled_details
		SET @start_time = GETDATE();

		PRINT '>> Truncating bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.crm_sales_details);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.crm_sales_details in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';

		
		PRINT '------------------------------------------------';
		PRINT 'CRM Tables loaded Succefully';
		PRINT '------------------------------------------------';

		
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP tables....';
		PRINT '------------------------------------------------';

		-- load data into bronze.erp_CUST_AZ12
		SET @start_time = GETDATE();
		
		PRINT '>> Truncating bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		
		PRINT '>> Inserting Data into: bronze.erp_CUST_AZ12';

		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n', 
			TABLOCK
		);

		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.erp_CUST_AZ12);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.erp_CUST_AZ12 in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';


		-- load data into bronze.erp_LOC_A101
		SET @start_time = GETDATE();
		
		PRINT '>> Truncating bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		
		PRINT '>> Inserting Data into: bronze.erp_LOC_A101';

		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n', 
			TABLOCK
		);

		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.erp_LOC_A101);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.erp_LOC_A101 in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';


		-- load data into bronze.PX_CAT_G1V2
		SET @start_time = GETDATE();
		
		PRINT '>> Truncating bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		
		PRINT '>> Inserting Data into: bronze.PX_CAT_G1V2';

		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\user\OneDrive - Savill Analytics\Savill Doc\Savill Teaching Docs\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n', 
			TABLOCK
		);

		SET @rows_loaded = (SELECT COUNT(*) FROM bronze.erp_PX_CAT_G1V2);

		SET @end_time = GETDATE();

		PRINT CONCAT ('Loaded bronze.erp_PX_CAT_G1V2 in ', 
			CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR), ' seconds.' ,'| Rows loaded: ', @rows_loaded
		 );
		PRINT '>> --------------------------------------------';
		
		
		PRINT '------------------------------------------------';
		PRINT 'ERP Tables loaded Succefully';
		PRINT '------------------------------------------------';
		
		SET @batch_end_time = GETDATE();

		PRINT '=================================================';
		PRINT 'Bronze layer loaded Succefully';
		PRINT '=================================================';

		PRINT CONCAT ('Loaded bronze Layer in ', 
			CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR), ' seconds.' );
		PRINT '>> --------------------------------------------';
		

	END TRY

	BEGIN CATCH

		PRINT '================================================';
		PRINT 'Error occurred while loading Bronze layer: ' 
		PRINT 'ErrorMessage: ' + ERROR_MESSAGE();
		PRINT 'Errorline: '	   + CAST(ERROR_LINE() AS VARCHAR);
		PRINT 'ErrorNumber: '  + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT '=================================================';
		
	END CATCH
END