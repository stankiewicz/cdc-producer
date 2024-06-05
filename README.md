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


## Data Model

### Person 
Table cdc_person is mapped from Person entity in cdc, with that change that:
- person_id promoted to the rowkey, cast to string
- only after values are persisted
- only one column family is used (p)
- op_type is kept and is used as tombstone in case data is deleted

| rowkey | cf=p<br/>cn=op_type | cf=p<br/>cn=city | cf=p<br/>cn=firstName | cf=p<br/>cn=lastName |
|--------|---------------------|------------------|-----------------------|----------------------|
| 1234   | i                   | Warsaw           | John                  | Doe                  |
| 4567   | u                   | Krakow           | Jane                  | Doe                  |
| 890    | d                   |                  |                       |                      |


### Orders
Table cdc_orders is mapped from Order entity in cdc, with that change that:
- order_id and person_id is promoted to the rowkey, built as composite key to string
- only after values are persisted
- only one column family is used (p)
- op_type is kept and is used as tombstone in case data is deleted

| rowkey | cf=p<br/>cn=op_type | cf=p<br/>cn=status | cf=p<br/>cn=items | cf=p<br/>cn=address |
|--------|---------------------|--------------------|-------------------|---------------------|
| 1234_1 | i                   | New                | SKU1,SKU2         | Warsaw              |
| 1234_2 | d                   |                    |                   |                     |
| 5678_1 | u                   | Delivered          | SKU10             | Krakow              |

When pulling the data, usual use case is pulling all orders for specific person_id.
Bigtable provides ability to do prefix scan.

### Versions

TBD how many versions of cells to keep.

### Tombstones and solving out of order changes

CDC changes covers removal of data. If row is removed,
after* properties are empty and before.PK property has to be used to identify row to be removed.

To remove row, all column names - city, firstName etc are removed but op_type is kept with timestamp of removal.

Having tombstones allow to detect and resolve a situation when entity is modified and removed and for some reason CDC change is done in different order. 
If delete is processed first, values are set to null, op_type is set to d with timestamp=LATER.
Then if update is processed, values are set and op_type is set to u with timestamp=EARLIER.

When data is pulled, consumer will notice op_type = d and will ignore values. 

Out of order applies can happen if CDC changes are distributed to random partitions or if there is batch that refreshes or repeats data from CDC.


