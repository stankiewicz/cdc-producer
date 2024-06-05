# CDC Producer

## Sources

Repository is created based on [quickstart](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java), but most of the files are removed.


## Architecture

```

                   ┌──────────────┐
                   │  PubSubIO    │ Topic: cdc_person
                   │ (Read/Source)│
                   └──────┬───────┘
                          │  PCollection<String>
                          v
                   ┌────────────────┐
                   │ParseCCTransform│
                   └──────┬────┬────┘
                          │    │  PCollection<Row>
                          v    │
        ┌───────────────────┐  │
        │PersistCDCTransform│  │
        └─────────┬─────────┘  │
    row,Mutations │            └────────┐  
                  v                     v
           ┌──────────────┐      ┌───────────────────┐
           │Cloud BigTable│      │ActionableTransform│ filter interesting events
           │   (cdc)      │      └──────┬────────────┘
           └──────────────┘             │  PCollection<Row>
                  ^                     v
                  │               ┌───────────┐
                  │               │ Reshuffle │ to checkpoint and 
                  │               └──────┬────┘ make sure changes are flushed to BigTable
                  │                      │
                  │ lookup               v
                  │            ┌─────────────┐
                  └────────────┤ BuildEvents │
                               └──────┬──────┘
                                      │  PCollection<PubSubMessage> with topics
                                      v
                            ┌──────────────────────┐
                            │  PubSubIO            │ 
                            │(writeMessagesDynamic)│
                            └──────────────────────┘



```

## Run

Altough pom.xml supports multiple profiles, this was tested locally and dataflow only.

### Run tests

run all tests
```
mvn test
```

### Locally

```
mvn compile exec:java \
-Dexec.mainClass=com.google.dataflow.ingestion.pipeline.CDCPipeline \
-Dexec.args="--output=counts"
```
### Dataflow

```
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=com.google.dataflow.ingestion.pipeline.CDCPipeline \
-Dexec.args="--project=PROJECT_ID \
--gcpTempLocation=gs://BUCKET_NAME/temp/ \
--output=gs://BUCKET_NAME/output \
--runner=DataflowRunner \
--region=REGION"
```
