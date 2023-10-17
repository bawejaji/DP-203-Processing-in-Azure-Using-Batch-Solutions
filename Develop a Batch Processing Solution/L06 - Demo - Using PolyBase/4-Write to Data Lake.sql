-- Use 'CETAS' to write data to Data Lake (using PolyBase)

CREATE EXTERNAL TABLE ext.ProcessedTaxiRides
WITH 
(  
    DATA_SOURCE = PSDataLake,
    FILE_FORMAT = SynapseParquetFormat,
    LOCATION='/Facts/TaxiRides.parquet'    
) 
AS
SELECT 
      *
FROM main.TaxiRides y
    INNER JOIN ext.TaxiZones z ON y.PickupLocationId = z.LocationID
WHERE PickupTime >= '2023-01-01' 
    AND PickupTime < '2023-02-01'
    AND PassengerCount > 0
	AND z.ServiceZone = 'Yellow Zone';