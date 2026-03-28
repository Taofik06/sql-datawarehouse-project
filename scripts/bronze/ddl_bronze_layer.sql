/*
====================================================================
CREATE TABLES IN BRONZE SCHEMA
====================================================================
Note: The following SQL script creates tables in the 'bronze' schema. 
It first checks if the tables already exist and drops them if they do, then creates new tables with the specified columns and data types.

*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
	cst_id              INT,
	cst_key             NVARCHAR(50),
	cst_firstname       NVARCHAR(50),
	cst_lastname        NVARCHAR(50),
	cst_marital_status  NVARCHAR(50),
	cst_gndr            NVARCHAR(50),
	cst_create_date     DATE
);

GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

GO

CREATE TABLE bronze.crm_prd_info (
	prd_id              INT,
	prd_key             NVARCHAR(50),
	prd_nm	            NVARCHAR(50),
	prd_cost	        INT,
	prd_line			NVARCHAR(50),
	prd_start_dt		DATE,
	prd_end_dt			DATE
);

GO


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt		INT,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT
);
GO



IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
GO

CREATE TABLE bronze.erp_CUST_AZ12 (
	cid              NVARCHAR(50),
	bdate            DATE,
	gen		         NVARCHAR(50)
);
GO



IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
GO


CREATE TABLE bronze.erp_LOC_A101 (
	cid              NVARCHAR(50),
	cntry            NVARCHAR(50)
);
GO


IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
GO

CREATE TABLE bronze.erp_PX_CAT_G1V2 (
	id				 NVARCHAR(50),
	cat		         NVARCHAR(50),
	subcat			 NVARCHAR(50),
	maintenance		 NVARCHAR(50)
);
GO



IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC ('CREATE SCHEMA audit');
GO

IF OBJECT_ID('audit.bronze_load_audit', 'U') IS NOT NULL
    DROP TABLE audit.bronze_load_audit;
GO

CREATE TABLE audit.bronze_load_audit
(
    audit_id            INT IDENTITY(1,1) PRIMARY KEY,
    batch_id            UNIQUEIDENTIFIER NOT NULL,
    table_name          VARCHAR(200) NOT NULL,
    source_file         VARCHAR(500) NOT NULL,
    load_start_time     DATETIME NOT NULL,
    load_end_time       DATETIME NULL,
    duration_seconds    INT NULL,
    rows_loaded         INT NULL,
    load_status         VARCHAR(20) NOT NULL,
    error_message       NVARCHAR(4000) NULL,
    created_at          DATETIME NOT NULL DEFAULT GETDATE()
);
GO