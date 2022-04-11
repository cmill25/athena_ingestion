# Programmatically Ingesting Data via Amazon Athena
Our Data Engineering team was recently requested to ingest data stored within an Amazon S3 bucket. However, we were not granted access to the actual container: our only access point was querying with Amazon Athena. This presented a unique situation, as we needed to programmatically ingest and fully load this data overnight to retrieve updated records.

We made an attempt to ingest the data via an Azure Function utilizing a Python job: this worked for daily ingestion, but the full historical data was too large for the function app to handle. We were able write a Spark job for the process which was able to process the massive amount of data in around five minutes.

If you find yourself with a data source where you only have Athena query access, the process detailed here may help you to programmatically ingest your data.
