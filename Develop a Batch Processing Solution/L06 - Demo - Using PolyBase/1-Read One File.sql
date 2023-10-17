-- Create master key
CREATE MASTER KEY;
GO

-- Create credential to access Data Lake
CREATE DATABASE SCOPED CREDENTIAL PSDataLakeCredential
WITH 
	IDENTITY = 'user', 
	Secret = '***Access Key***';
GO

-- Create external data source, pointing to Data Lake
CREATE EXTERNAL DATA SOURCE PSDataLake
with (  
      TYPE = HADOOP,
      LOCATION ='abfss://***ContainerName***@***DataLakeName***.dfs.core.windows.net',  
      CREDENTIAL = PSDataLakeCredential  
);  
GO

-- Create external file format
CREATE EXTERNAL FILE FORMAT CSVFileFormat 
WITH 
(   FORMAT_TYPE = DELIMITEDTEXT
,   FORMAT_OPTIONS  
	(   
		FIELD_TERMINATOR   = ','
		, STRING_DELIMITER = '"'
        , DATE_FORMAT      = 'yyyy-MM-dd HH:mm:ss'
        , USE_TYPE_DEFAULT = FALSE
        , FIRST_ROW  = 2
    )
);
GO

-- Create schema for external resources
CREATE SCHEMA ext
GO

-- Create external table for Taxi Zones
CREATE EXTERNAL TABLE ext.TaxiZones
(
	LocationId INT,
	Borough NVARCHAR(100),
	Zone NVARCHAR(100),
    ServiceZone NVARCHAR(100)
)
WITH
(
    DATA_SOURCE = PSDataLake
  , FILE_FORMAT = CSVFileFormat  
  , LOCATION='/Raw/TaxiZones/TaxiZones1.csv'  
)
GO

SELECT * FROM ext.TaxiZones
GO

















