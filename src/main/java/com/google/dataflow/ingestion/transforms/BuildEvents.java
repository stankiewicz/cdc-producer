package com.google.dataflow.ingestion.transforms;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.LocationChange;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class BuildEvents extends DoFn<Row, KV<String,Event>> {

  private final String instanceId;
  private final String projectId;
  private final String appProfileId;
  String tableId;

  BigtableDataClient dataClient;

  BigtableDataSettings settings = null;

  boolean test = false;
  int port;

  @VisibleForTesting
  public BuildEvents(int port, String projectId, String instanceId, String tableId){
    test=  true;
    this.tableId = tableId;
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.port = port;
    this.appProfileId = null;
  }

  public BuildEvents(String projectId, String instanceId, String tableId, String appProfileId){

    this.tableId = tableId;
    this.instanceId = instanceId;
    this.projectId = projectId;
    this.appProfileId = appProfileId;
  }

  @Setup
  public void setup() throws IOException {
    if(test) {
      settings = BigtableDataSettings.newBuilderForEmulator(port).setProjectId(projectId)
          .setInstanceId(instanceId).build();
    }else{
      settings = BigtableDataSettings.newBuilder()
          .setProjectId(projectId)
          .setAppProfileId(appProfileId)
          .setInstanceId(instanceId).build();
    }
    dataClient = BigtableDataClient.create(settings);
  }

  @Teardown
  public void teardown(){
    dataClient.close();
  }

  @ProcessElement
  public void processElements(@Element Row row, OutputReceiver<KV<String,Event>> outputReceiver){
      if(row.getBoolean("locationChange")!=null){
        // TODO read once vs read per event?
        final com.google.cloud.bigtable.data.v2.models.Row latestRow = dataClient.readRow(
            TableId.of(tableId), row.getString("key"));
        LocationChange lc = LocationChange.create(
            row.getString("key"),
            latestRow.getCells("p","city").get(0).getValue().toStringUtf8(),
            latestRow.getCells("p","firstName").get(0).getValue().toStringUtf8(),
            latestRow.getCells("p","lastName").get(0).getValue().toStringUtf8());
        outputReceiver.output(KV.of("location_change",Event.of(lc)));
      }
  }

}
