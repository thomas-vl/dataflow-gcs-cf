import os
from googleapiclient.discovery import build

from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud import storage
import csv

def load_tsv(data, context):
    client = bigquery.Client()
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(data['bucket'])
    blob = bucket.get_blob(data['name'])
    tsv_file = blob.download_to_file('test.tsv')

    csv.DictReader(tsv_file, dialect='Excel-Tab')

    rows = [{'date':'2019-03-01', 'page':'url'}]

    table_name = 'table_name'
    dataset = 'sample_data'
    project = 'project-id'

    schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("page", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("query", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("impressions", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ctr", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("position", "FLOAT", mode="REQUIRED"),
    ]
    table_ref = bigquery.Table(f"{project}.{dataset}.{table_name}",schema=schema)

    try:
        table = client.get_table(table_ref)
    except exceptions.NotFound:
        table = client.create_table(table_ref)

    client.insert_rows_json(table, rows, ignore_unknown_values=True)

#gcloud functions deploy load_tsv --runtime python37 --trigger-resource experiment-center-df-files --trigger-event google.storage.object.finalize --project experiment-center



def start_dataflow(data, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(data['bucket']))
    print('File: {}'.format(data['name']))
    print('Metageneration: {}'.format(data['metageneration']))
    print('Created: {}'.format(data['timeCreated']))
    print('Updated: {}'.format(data['updated']))

    project = os.environ['GCP_PROJECT']
    service = build('dataflow', 'v1b3', cache_discovery=False)
    service.projects().templates().launch(
        projectId=project,
        gcsPath=f"gs://{project}-df-template/templates/df-bq",
        body={ 'parameters':{'input':f"gs://{data['bucket']}/{data['name']}" } }
        ).execute()

#gcloud functions deploy start_dataflow --runtime python37 --trigger-resource experiment-center-df-files --trigger-event google.storage.object.finalize --project experiment-center
