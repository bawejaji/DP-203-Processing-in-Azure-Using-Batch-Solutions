/*********************************************************************************
    Load file TaxiRides.csv to TaxiRides table
        - Create external table (ExternalTaxiRides) on CSV file
        - Use CTAS to load data from external table to TaxiRides table
*********************************************************************************/

-- Create external data source, pointing to Data Lake
CREATE EXTERNAL DATA SOURCE PSDataLake
with (  
      TYPE = HADOOP,
      LOCATION ='abfss://***Container Name***@***Data Lake Name***.dfs.core.windows.net'
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

-- Create external table for Taxi Rides
CREATE EXTERNAL TABLE ExternalTaxiRides
(
	RideId                  INT,
	VendorId                INT,
	PickupTime              VARCHAR(100),
    DropTime                VARCHAR(100),
    PickupLocationId        INT,
    DropLocationId          INT,
    CabNumber               VARCHAR(100),
    DriverLicenseNumber     VARCHAR(100),
    PassengerCount          INT,
    TripDistance            FLOAT,
    RateCodeId              INT,
    PaymentType             INT,
    TotalAmount             FLOAT,
    FareAmount              FLOAT,
    Extra                   FLOAT,
    MtaTax                  FLOAT,
    TipAmount               FLOAT,
    TollsAmount             FLOAT,
    ImprovementSurcharge    FLOAT
)
WITH
(
    DATA_SOURCE = PSDataLake
  , FILE_FORMAT = CSVFileFormat  
  , LOCATION='/Raw/TaxiRides.csv'  
)
GO

-- Query external table
SELECT *
FROM ExternalTaxiRides
GO

-- Create main table and load data from external table
CREATE TABLE TaxiRides
WITH 
(
	DISTRIBUTION = HASH(PickupLocationId),
	CLUSTERED COLUMNSTORE INDEX
) 
AS 
SELECT * 
FROM ExternalTaxiRides


-- Select from Dedicated SQL Pool table
SELECT COUNT(*) FROM TaxiRides


/*********************************************************************************
    Load file TaxiRides_delta.csv to TaxiRidesDelta table
        - Create external table (ExternalTaxiRidesDelta) on CSV file
        - Use CTAS to load data from external table to TaxiRidesDelta table
*********************************************************************************/

-- Create external table for Taxi Rides Delta
CREATE EXTERNAL TABLE ExternalTaxiRidesDelta
(
	RideId                  INT,
	VendorId                INT,
	PickupTime              VARCHAR(100),
    DropTime                VARCHAR(100),
    PickupLocationId        INT,
    DropLocationId          INT,
    CabNumber               VARCHAR(100),
    DriverLicenseNumber     VARCHAR(100),
    PassengerCount          INT,
    TripDistance            FLOAT,
    RateCodeId              INT,
    PaymentType             INT,
    TotalAmount             FLOAT,
    FareAmount              FLOAT,
    Extra                   FLOAT,
    MtaTax                  FLOAT,
    TipAmount               FLOAT,
    TollsAmount             FLOAT,
    ImprovementSurcharge    FLOAT
)
WITH
(
    DATA_SOURCE = PSDataLake
  , FILE_FORMAT = CSVFileFormat  
  , LOCATION='/Raw/TaxiRides_delta.csv'  
)
GO

-- Query external table
SELECT * FROM ExternalTaxiRidesDelta
GO

-- Create main table and load data from external table
CREATE TABLE TaxiRidesDelta
WITH 
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
) 
AS 
SELECT * 
FROM ExternalTaxiRidesDelta


-- Select from Dedicated SQL Pool table
SELECT * 
FROM TaxiRidesDelta



/*********************************************************************************
    Merge data from TaxiRidesDelta to TaxiRides table
*********************************************************************************/

-- Check passenger count before Merge
SELECT RideId, PassengerCount
FROM TaxiRides
WHERE RideId = 1
GO

/**** Merge statement ****/

-- Table to be modified
MERGE INTO TaxiRides AS tgt

    -- Source table
	USING TaxiRidesDelta AS src 

        -- Join condition
		ON src.RideId = tgt.RideId

-- When source record already exist in target, update the record
WHEN MATCHED
    THEN
		UPDATE SET tgt.PassengerCount = src.PassengerCount

-- When source record does not exist in target, insert the record
WHEN NOT MATCHED 
    THEN
		INSERT (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber,
                PassengerCount, TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, 
                TipAmount, TollsAmount, ImprovementSurcharge)

		VALUES (src.RideId, src.VendorId, src.PickupTime, src.DropTime, src.PickupLocationId, src.DropLocationId, src.CabNumber, src.DriverLicenseNumber,
                src.PassengerCount, src.TripDistance, src.RateCodeId, src.PaymentType, src.TotalAmount, src.FareAmount, src.Extra, src.MtaTax,
                src.TipAmount, src.TollsAmount, src.ImprovementSurcharge);

-- When target record does not exist in source, delete the record
-- WHEN NOT MATCHED BY SOURCE 
    --THEN DELETE;
GO

-- Check passenger count after Merge
SELECT RideId, PassengerCount
FROM TaxiRides
WHERE RideId = 1
GO

SELECT COUNT(*)
FROM TaxiRides
GO





