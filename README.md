# dataflow-gcs-cf
Install gcloud commandline tool

##Set variables
EXPORT PROJECT="your-gcp-project-id"
EXPORT LOCATION="gcp-location" #for example us-east1

##Create 2 buckets one for the files and one for the template
gsutil mb -p $PROJECT -c regional -l $LOCATION -b on gs://$PROJECT-df-template/
gsutil mb -p $PROJECT -c regional -l $LOCATION -b on gs://$PROJECT-df-files/


##Deploy dataflow template
python -m main --output 'experiment-center:test.test' --runner DataflowRunner --project experiment-center --staging_location gs://experiment-center-df-template/staging --temp_location gs://experiment-center-df-template/temp --template_location gs://experiment-center-df-template/templates/df-bq

##Deploy the function (from cloud-functions folder)
gcloud functions deploy start_dataflow --runtime python37 --trigger-resource experiment-center-df-files --trigger-event google.storage.object.finalize --project experiment-center
