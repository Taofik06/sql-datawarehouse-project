/*
=========================================
Create Database and Schema
=========================================
Note: This script will create a database named "DataWarehouse" and three schemas named "bronze", "silver", and "gold". 
If the database already exists, it will be dropped and recreated. Make sure to back up any important data before running this script, 
as it will result in data loss if the database already exists.
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SavillDW')
BEGIN
	ALTER DATABASE SavillDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE SavillDW;
END;
GO

-- Create the database datawarehouse
CREATE DATABASE SavillDW;
GO

-- Use the database datawarehouse
USE SavillDW;
GO


-- create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
