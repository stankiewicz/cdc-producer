# CDC Producer

## Sources

Repository is created based on [quickstart](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java), but most of the files are removed.


## Architecture


## Run

Altough pom.xml supports multiple profiles, this was tested locally and dataflow only.

### Locally

```
mvn compile exec:java \
-Dexec.mainClass=com.google.dataflow.ingestion.pipeline.CDCPipeline \
-Dexec.args="--output=counts"
```
### Dataflow

```

mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=com.google.dataflow.feature.pipeline.TaxiNRTPipeline \
-Dexec.args="--project=PROJECT_ID \
--gcpTempLocation=gs://BUCKET_NAME/temp/ \
--output=gs://BUCKET_NAME/output \
--runner=DataflowRunner \
--region=REGION"
```

example:

```
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=com.google.dataflow.feature.pipeline.TaxiNRTPipeline \
-Dexec.args="--project=radoslaws-playground-pso \
--runner=DataflowRunner \
--projectId=radoslaws-playground-pso \
--datasetName=features \
--tableName=taxi_30s \
--region=us-central1 \
--gcpTempLocation=gs://radoslaws-playground-pso/temp/  \
--enableStreamingEngine=true  --maxNumWorkers=1 \
--usePublicIps=false \
--subnetwork=regions/us-central1/subnetworks/default \
--serviceAccount=96401984427-compute@developer.gserviceaccount.com"
```

manual sync:
```agsl
curl -X POST \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json; charset=utf-8" \
     -d "" \
     "https://us-central1-aiplatform.googleapis.com/v1/projects/96401984427/locations/us-central1/featureOnlineStores/online/featureViews/nrt:sync"
```
request.json
```json
{
  data_key: { "key":"5f69f507-25ca-4236-9941-fc93b23193d1"},
  data_format: "KEY_VALUE"
}

```
pull 
```agsl
curl -X POST \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json; charset=utf-8" \
     -d @request.json \
 "https://us-central1-aiplatform.googleapis.com/v1/projects/96401984427/locations/us-central1/featureOnlineStores/online/featureViews/nrt:fetchFeatureValues"
```


```agsl
curl -X GET \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "https://us-central1-aiplatform.googleapis.com/v1/projects/96401984427/locations/us-central1/featureOnlineStores/optimized"

```
