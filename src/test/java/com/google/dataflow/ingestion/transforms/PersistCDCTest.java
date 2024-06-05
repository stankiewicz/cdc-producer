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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.dataflow.ingestion.model.CDC.Order;
import com.google.dataflow.ingestion.model.CDC.Person;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Perform local unit testing of pipeline code which writes data to BigTable.
 *
 * Beam SDK for Java provides a number of utilities to make this easier
 * https://beam.apache.org/documentation/pipelines/test-your-pipeline/
 * For example, org.apache.beam.sdk.testing.TestPipeline
 *
 * Bigtable provides an emulator to use for testing
 * https://cloud.google.com/bigtable/docs/emulator
 * There is a Java wrapper for the emulator
 * https://cloud.google.com/bigtable/docs/emulator#java_wrapper_for_the_emulator
 *
 */
@RunWith(JUnit4.class)
public class PersistCDCTest {

    @Rule public TestPipeline p = TestPipeline.create();

    @Rule public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    // Clients that will be connected to the emulator
    private BigtableTableAdminClient tableAdminClient;
    private BigtableDataClient dataClient;

    @Before
    public void setUp() throws IOException, ExecutionException, InterruptedException {
        // Initialize the clients to connect to the emulator
        BigtableTableAdminSettings.Builder tableAdminSettings =
                BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        ;
        tableAdminClient =
                BigtableTableAdminClient.create(
                        tableAdminSettings
                                .setProjectId("fake-project")
                                .setInstanceId("fake-instance")
                                .build());

        BigtableDataSettings.Builder dataSettings =
                BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        dataClient =
                BigtableDataClient.create(
                        dataSettings
                                .setProjectId("fake-project")
                                .setInstanceId("fake-instance")
                                .build());

        // Create a test table that can be used in tests
        // column family "p" - person
        // column family "o" - order
        String tableId = "cdc";
        tableAdminClient.createTable(CreateTableRequest.of(tableId).addFamily("p"));
        tableAdminClient.modifyFamilies(ModifyColumnFamiliesRequest.of(tableId).addFamily("o"));
    }

    @Test
    public void testPerson() throws Exception {
        final SerializableFunction<Person, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Person.class);
        Person input =
                Person.newBuilder()
                        .setOpType("i")
                        .setAfterCity("city2")
                        .setBeforeCity("city1")
                        .setAfterPersonId(1234L)
                        .setBeforePersonId(1234L)
                        .setAfterFirstName("john")
                        .setBeforeFirstName("john")
                        .setBeforeLastName("doe")
                        .setAfterLastName("doe")
                        .build();
        Row inputRow = toRowFunction.apply(input);
        p.apply(
                        Create.of(inputRow)
                                .withCoder(
                                        RowCoder.of(p.getSchemaRegistry().getSchema(Person.class))))
                .apply(
                        new PersistCDCTransform(
                                bigtableEmulator.getPort(),
                                "fake-project",
                                "fake-instance",
                                "cdc"));
        p.run();

        com.google.cloud.bigtable.data.v2.models.Row freshRow =
                dataClient.readRow(TableId.of("cdc"), "1234");
        Assert.assertEquals(
                freshRow.getCells("p", "firstName").get(0).getValue().toStringUtf8(), "john");
        Assert.assertEquals(
                freshRow.getCells("p", "lastName").get(0).getValue().toStringUtf8(), "doe");
        Assert.assertEquals(
                freshRow.getCells("p", "city").get(0).getValue().toStringUtf8(), "city2");
    }

    @Test
    public void testOrder() throws Exception {
        "".isEmpty();
    }
}
