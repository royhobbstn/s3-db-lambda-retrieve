# s3-db-lambda-retrieve

Compute expressions from Census data fields stored as JSON in AWS S3

## A Concise Explanation of What This Does

This is essentially the backend service for the Census Map.  The client sends some information about the current extent and map state, (along with a list of cluster data it has already seen) and the service figures out what data the map needs and sends it back.

**If You're Still Reading...**

 - There's a Redis Cache.  All the data for a theme is loaded from S3 at once and cached, but only the clusters needed are sent back.
 - Map extent is compared to a spatially indexed GeoJSON file.
 - Takes advantage of the AWS Lambda 'warm' function capability, whereby GeoJSON files can be potentially cached within Lambda itself.

**Most Noteworthy**

 - The Lambda function 'optimistically' sends data.  If you initiate a zoom, this Lambda is called immediately, and all data is sent at each zoom level relative to the zoom 'pole' coordinates.  Thereby, by the time your map tiles have loaded, the data is already present to join.