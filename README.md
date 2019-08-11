Pre requisites:
1) Have a gcp project with a linked billing account
2) Open up cloud shell

## Set variables
```shell
export project="your-gcp-project-id" #change this to your project
export region="your-gcp-region" #for example us-central1
export bq_dataset="dataflow_example"
```

## Create 2 buckets one for the files and one for the template
```shell
gsutil mb -p $project -c regional -l $region -b on gs://$project-df-template/
gsutil mb -p $project -c regional -l $region -b on gs://$project-df-files/
```

## Create bigquery dataset
```shell
bq mk --location=us --dataset $project:$bq_dataset
```

## Clone the repostiory files
```shell
git clone https://github.com/thomas-vl/dataflow-gcs-cf.git
```

## Deploy dataflow template
```shell
sudo pip3 install apache-beam[gcp]
cd ~/dataflow-gcs-cf/dataflow
python3 -m main --output $project:$bq_dataset.example --runner DataflowRunner --project $project \
 --staging_location gs://$project-df-template/staging --temp_location gs://$project-df-template/temp \
 --template_location gs://$project-df-template/templates/df-bq
```
validate if the template file exists:
```shell
gsutil ls gs://$project-df-template/templates/
```

## Deploy the function (from cloud-functions folder)
```shell
cd ~/dataflow-gcs-cf/cloud-functions
gcloud functions deploy start_dataflow --runtime python37 --trigger-resource $project-df-files --trigger-event google.storage.object.finalize --project $project --region $region
```
