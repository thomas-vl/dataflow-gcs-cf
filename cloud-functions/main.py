import os
from googleapiclient.discovery import build

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
