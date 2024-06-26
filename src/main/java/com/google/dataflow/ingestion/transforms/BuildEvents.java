/*
 *
 *  Copyright (c) 2024  Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package com.google.dataflow.ingestion.transforms;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.LocationChange;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class BuildEvents extends DoFn<Row, KV<String, Event>> {

    private final String instanceId;
    private final String projectId;
    private final String appProfileId;
    private final String locationChangeTopic;
    String tableId;

    BigtableDataClient dataClient;

    BigtableDataSettings settings = null;

    boolean test = false;
    int port;

    @VisibleForTesting
    BuildEvents(int port, String projectId, String instanceId, String tableId, String locationChangeTopic) {
        this.locationChangeTopic = locationChangeTopic;
        test = true;
        this.tableId = tableId;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.port = port;
        this.appProfileId = null;
    }

    public BuildEvents(String projectId, String instanceId, String tableId, String appProfileId, String locationChangeTopic) {
        this.locationChangeTopic = locationChangeTopic;
        this.tableId = tableId;
        this.instanceId = instanceId;
        this.projectId = projectId;
        this.appProfileId = appProfileId;
    }

    @Setup
    public void setup() throws IOException {
        if (test) {
            settings =
                    BigtableDataSettings.newBuilderForEmulator(port)
                            .setProjectId(projectId)
                            .setInstanceId(instanceId)
                            .build();
        } else {
            settings =
                    BigtableDataSettings.newBuilder()
                            .setProjectId(projectId)
                            .setAppProfileId(appProfileId)
                            .setInstanceId(instanceId)
                            .build();
        }
        dataClient = BigtableDataClient.create(settings);
    }

    @Teardown
    public void teardown() {
        dataClient.close();
    }

    @ProcessElement
    public void processElements(
            @Element Row row, OutputReceiver<KV<String, Event>> outputReceiver) {
        if (row.getBoolean("locationChange") != null) {
            // TODO read once vs read per event?
            final com.google.cloud.bigtable.data.v2.models.Row latestRow =
                    dataClient.readRow(TableId.of(tableId), row.getString("key"));

            Map<String, String> orderStatus = new HashMap<>();

            ServerStream<com.google.cloud.bigtable.data.v2.models.Row> rows =
                    dataClient.readRows(
                            Query.create(TableId.of("cdc_order")).prefix(row.getString("key")));
            for (com.google.cloud.bigtable.data.v2.models.Row order : rows) {

                orderStatus.put(
                        order.getKey().toStringUtf8().split("_")[1],
                        order.getCells("o", "status").get(0).getValue().toStringUtf8());
            }

            LocationChange lc =
                    LocationChange.create(
                            row.getString("key"),
                            latestRow.getCells("p", "city").get(0).getValue().toStringUtf8(),
                            latestRow.getCells("p", "firstName").get(0).getValue().toStringUtf8(),
                            latestRow.getCells("p", "lastName").get(0).getValue().toStringUtf8(),
                            orderStatus);
            outputReceiver.output(KV.of(this.locationChangeTopic, Event.of(lc)));
        }
    }
}
