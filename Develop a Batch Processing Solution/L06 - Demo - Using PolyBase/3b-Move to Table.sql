-- Create schema 'main' for Dedicated SQL Pool tables
CREATE SCHEMA main
GO

-- Create table main.TaxiRides, using CTAS (Polybase)
CREATE TABLE main.TaxiRides
WITH 
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
) 
AS 
SELECT * 
FROM ext.TaxiRides


-- Select from Dedicated SQL Pool table
SELECT * FROM main.TaxiRides


















