-- Drop external table
DROP EXTERNAL TABLE ext.TaxiZones

-- Recreate external table
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
  , LOCATION='/Raw/TaxiZones/'
  , REJECT_TYPE = VALUE
  , REJECT_VALUE = 10
  , REJECTED_ROW_LOCATION='/Errors/TaxiZones'
)
GO

SELECT * FROM ext.TaxiZones



















