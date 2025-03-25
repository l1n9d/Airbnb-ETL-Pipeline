# Airbnb-ETL-Pipeline
We’ll design a Google Cloud Dataflow ETL pipeline to extract raw Airbnb data from a cloud storage bucket. Then we’ll load it into BigQuery tables and views for analytical purposes after performing various data cleaning and transformation tasks.

Technically, the pipeline will be written using Apache Beam’s Python software development kit (SDK) using the following steps:

Ingest data from a Google Cloud Storage bucket.
Validate the ingested data with various integrity checks for values that are missing, null, or outside the prescribed range.
Clean the data to remove unnecessary columns, special characters and symbols, and duplications.
Transform and enrich the data using various techniques like filtering, grouping, and aggregation.
Load the transformed data to BigQuery tables and views.
Appropriate access control and security mechanisms will be implemented to run the ETL pipeline using GCP identity and access management (IAM) authorization and authentication techniques. Following a real-time approach, we’ll create and run the pipeline through a service account, not a personal user account.

During pipeline execution, we’ll monitor it using Dataflow monitoring tools. Once the pipeline finishes, certain validation and testing steps will be carried out to validate the pipeline’s output in BiQuery. This project focuses on the fundamental ETL process of extracting data from one source and loading it into another, demonstrating how to ETL unaltered data for analysis in Google Cloud.
