CREATE CREDENTIAL [pltaxiscosmosdb]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '***Cosmos DB Access Key***'
GO

SELECT TOP 100 
      RideId
    , PickupLocationId
    , Rating
    , Feedback
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=***Cosmos DB Account Name***;Database=***Database Name***',
                OBJECT = '***Container Name***',
                SERVER_CREDENTIAL = 'pltaxiscosmosdb'
) AS [RidesFeedback]