# Import Statements
import apache_beam as beam
from typing import NamedTuple
import csv
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
import os
from datetime import datetime

print("Execution Started")
#Set GOOGLE_APPLICATION_CREDENTIALS environment variable to use Service account key for pipeline execution
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usercode/etl-pipeline-key.json"

class AirbnbData(NamedTuple):
    id: str
    name: str
    host_id: str
    host_name: str
    neighbourhood_group: str
    neighbourhood: str
    latitude: str
    longitude: str
    room_type: str
    price: str
    minimum_nights: str
    number_of_reviews: str
    last_review: str
    reviews_per_month: str
    calculated_host_listings_count: str
    availability_365: str
    number_of_reviews_ltm: str
    license: str
    price_type: str = ''
    neighborhood_avg_price: float = 0
    price_usd: float = 0
    lr_year: str = ''
    lr_month: str = ''
    lr_day: str = ''

#Create dataset function
def create_dataset():
    # Create BigQuery Client    
    client = bigquery.Client()

    #Dataset-id variable
    dataset_id = "demi001.airbnb_ds_new"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    #Create dataset
    dataset = client.create_dataset(dataset, exists_ok=True)

#Set the pipeline options        
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='demi001',
    temp_location='gs://demistore01/tmp/',
    staging_location='gs://demistore01/staging',
    job_name='etl-pipeline-new',
    machine_type='e2-standard-2',
    flags=[],
    num_workers=1,
    max_num_workers=1,
    region='us-central1',
    save_main_session=True
)  

# Define function to impute missing, empty values
def impute_values(row):
    return row._replace(
        neighbourhood='Not Available' if row.neighbourhood in ('', None) else row.neighbourhood,
        availability_365 ='0' if row.availability_365  in ('', None) else row.availability_365 ,
        reviews_per_month='0' if row.reviews_per_month in ('', None) else row.reviews_per_month,
        number_of_reviews_ltm='0' if row.number_of_reviews_ltm in ('', None) else row.number_of_reviews_ltm
    )

class ImputeValues(beam.DoFn):
    def process(self, element):
        return [impute_values(element)]  

#Define gcs_path variable
gcs_bucket = 'demistore01'
csv_file_path = 'airbnb_data.csv'
gcs_path = f'gs://{gcs_bucket}/{csv_file_path}'

# Define variables for Data write to BigQuery
output_table_spec = 'demi001:airbnb_ds_new.airbnb_tb_new'
gcs_temp_location = 'gs://demistore01/bigquery_temp/'

# Create dataset
try:
    create_dataset()
    print("Dataset created")

except:
    print("Failed to create dataset")

print("Executing the pipeline. This may take few minutes")

#Create pipeline object
with beam.Pipeline(options=pipeline_options) as pipeline:

    #Read the data from gcs bucket
    csv_data = ( 
        pipeline 
        | 'Read CSV' >> beam.io.ReadFromText(gcs_path, skip_header_lines=1)
    )

    parsed_data = (
        csv_data
        | 'Parse CSV' >> beam.Map(lambda row: AirbnbData(*next(csv.reader([row])), price_type='', neighborhood_avg_price=0, price_usd=0, lr_year='', lr_month='', lr_day=''))
    )

        
    # Validate the incoming data 
    filtered_data = (
        parsed_data
        | 'Filter Positive Prices' >> beam.Filter(lambda row: int(row.price) >= 0)
        | 'Filter Abnormal Coordinates' >> beam.Filter(
            lambda row: (
                -90.0 <= float(row.latitude) <= 90.0 and
                -180.0 <= float(row.longitude) <= 180.0
            ))
        | 'Filter Non-Null Host IDs' >> beam.Filter(lambda row: row.host_id.strip() != '' )
    )

    #Remove duplicate records from data
    deduplicated_data = (
        filtered_data
        | 'Remove Duplicates' >> beam.Distinct()
    )

    #Replace special character comma with underscore    
    special_char_replaced  = (
        deduplicated_data
        | 'Replace Comma' >> beam.Map(lambda row: row._replace(**{'name': row.name.replace(',', '_')}))
    )

    # Impute missing, empty values
    cleaned_data = (
        special_char_replaced
        | 'Impute Values' >> beam.ParDo(ImputeValues())
    )

    # Create new informational columns    
    enriched_data = (
        cleaned_data
        | 'Classify PriceType' >> beam.Map(lambda row: row._replace(price_type='economic' if int(row.price) < 50 else ('mid-range' if 50 <= int(row.price) < 150 else 'luxury')))
        | 'Convert to USD' >> beam.Map(lambda row: row._replace(
            price_usd=float(row.price) * 0.66 ))
            | 'Extract Date Components' >> beam.Map(
            lambda row: row._replace(
                lr_year='',
                lr_month='',
                lr_day=''
            ) if row.last_review in ('', None) else row._replace(
                lr_year=int(datetime.strptime(row.last_review, '%Y-%m-%d').year),
                lr_month=int(datetime.strptime(row.last_review, '%Y-%m-%d').month),
                lr_day=int(datetime.strptime(row.last_review, '%Y-%m-%d').day)
            ))
    )
        
    # Calculate average price per neighborhood
    avg_price_per_neighborhood = (
        enriched_data
        | 'KeyBy Neighborhood' >> beam.Map(lambda row: (row.neighbourhood, float(row.price)))
        | 'GroupBy Neighborhood' >> beam.GroupByKey()
        | 'Compute Avg Price' >> beam.Map(lambda kv: (kv[0], sum(kv[1]) / len(kv[1])))
    )

    # Create final pCollection to load into BigQuery
    enriched_data_with_avg_price = (
        enriched_data
        | 'Add NeighborhoodAvgPrice' >> beam.Map(lambda row, avg_price_dict: row._replace(neighborhood_avg_price=round(avg_price_dict.get(row.neighbourhood, 0), 2)), avg_price_dict=beam.pvalue.AsDict(avg_price_per_neighborhood))
    )    

    # Upload Data to BigQuery while creating table
    (enriched_data_with_avg_price
        | 'Convert to Dict' >> beam.Map(lambda row: row._asdict())
        | 'Write to BigQuery' >> WriteToBigQuery(
            output_table_spec,
            schema='SCHEMA_AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location=gcs_temp_location)
    )
# Create BigQuery client 
client = bigquery.Client()

# Specify the ID for view 
view_id = "demi001.airbnb_ds_new.airbnb_analysis_vw_new"

# Create a TableReference object for the view.
view_ref = bigquery.Table(view_id)

# Define and set the SQL query for view.
view_ref.view_query = """
SELECT
  neighbourhood,
  price_type,
  ROUND(AVG(price), 2) AS avg_price,
  ROUND(MIN(price), 2) AS min_price,
  ROUND(MAX(price), 2) AS max_price,
  MIN(neighborhood_avg_price) AS min_neighbourhood_avg_price,
  MAX(neighborhood_avg_price) AS max_neighbourhood_avg_price
FROM airbnb_ds_new.airbnb_tb_new
GROUP BY neighbourhood, price_type 
ORDER BY neighbourhood, price_type;
"""

# Make an API request to create the view.
view = client.create_table(view_ref , exists_ok=True)
print("View created")
print("Code completed")