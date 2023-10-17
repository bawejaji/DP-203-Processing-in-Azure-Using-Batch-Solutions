CREATE TABLE TaxiRides
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
	DISTRIBUTION = HASH(PickupLocationId),
	CLUSTERED COLUMNSTORE INDEX
) 
GO