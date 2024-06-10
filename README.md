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

Altough pom.xml supports multiple profiles, this is tested locally and dataflow only.

### Locally (TODO add args)

```
mvn compile exec:java \
-Dexec.mainClass=com.google.dataflow.ingestion.pipeline.CDCPipeline \
-Dexec.args="--output=counts"
```
### Dataflow (TODO add args)

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

| rowkey | cf=o<br/>cn=op_type | cf=o<br/>cn=status | cf=o<br/>cn=items | cf=o<br/>cn=address |
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

# Pipeline correctness

Above pipeline is correct data, assuming that input data is valid and there is no data loss (e.g. pipeline is cancelled, not drained).
Pipeline changes to datastore are idempotent, assuming they are executed in same order - mutations are always upserting the data with specific row key and timestamp. In case of out of order, additonal logic is added on read to resolve it. 
In case of data loss, data can be replayed, either by starting from offset or running batch. 
Batch job could be created that takes snapshot of data and timestamp and produces mutations. If there are any gaps, it will fill it. 


# Design considerations

## Reading many low volume topics

With this pattern you may need to read from many low volume topics at the same time. 
Doing this naively with a Dataflow job per topic can be very cost-inefficient since each job will require at least one full worker provisioned. 
In cases like this, consider one of the following options:

### Combine topics before they are ingested into Dataflow

When possible, ingesting a few high volume topics is much more efficient than ingesting many low volume topics. 
This allows each topic to be handled by a single Dataflow job which can fully utilize its workers.

### Pack multiple topics into a single Dataflow job
If you can't bundle topics before ingesting into Dataflow, 
it can be beneficial to bundle multiple topics into a single pipeline. 
This allows Dataflow to handle multiple topics on the same worker and can increase efficiency.

There are two options for packing multiple topics into a pipeline: 
- use a single read step or use multiple read steps. You should use a single read step to do this if - All of your topics are collocated in the same cluster, or it is ok for your topics to share fate. When using a single read, a single sink or transform having issues can cause all of your topics to accumulate backlog.

- multiple read IO steps

## KafkaIO read tuning

Default settings in your Kafka cluster and client can heavily impact performance. 
In particular the following settings may all be too low or too high. 
You should experiment with these values for your workload. 
We have provided recommended starting places:

- unboundedReaderMaxElements - defaults to 10,000. A higher number like 100,000 can increase the size of your bundles, which can have significant benefits if doing aggregations in your pipeline, though it can also increase latency in some cases. Can be set with setUnboundedReaderMaxElements. Only relevant when using runner v1.
- unboundedReaderMaxReadTimeSec/unboundedReaderMaxReadTimeMs - defaults to 10s/1000ms. A higher number like 20s can increase the size of your bundles, but a lower size like 500s can lower your latency. Can be set with setUnboundedReaderMaxReadTimeMs.  Only relevant when using runner v1.
- max.poll.records - defaults to 500. A higher number may perform better by retrieving more incoming records together. Can be set using withConsumerConfigUpdates. This matters more when using runner v2.
- fetch.max.bytes - default to 1mb. Increasing this can improve throughput by reducing the number of requests; too high and it could increase latency (but downstream processing is likely to be the main bottleneck). A recommended starting place is 100mb. Can be set using withConsumerConfigUpdates. This matters more when using runner v2.
- max.partition.fetch.bytes - the maximum amount of data per partition the server will return. Defaults to 1mb. Increasing this can improve throughput by reducing the number of requests; too high and it could increase latency (but downstream processing is likely to be the main bottleneck). A recommended starting place is 100mb. Can be set using withConsumerConfigUpdates. This matters more when using runner v2.
- consumerPollingTimeout - defaults to 2 seconds. A higher number may be necessary if your consumer client is timing out before it is able to read any records. This is most often relevant when doing cross-region reads or reads with a slow network. Can be set using withConsumerPollingTimeout.

For low latency pipelines we would recommend starting with v1 runner and changing unboundedReaderMaxReadTimeMs to 500ms.
