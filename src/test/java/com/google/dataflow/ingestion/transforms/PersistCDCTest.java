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
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.common.collect.ImmutableList;
import com.google.dataflow.ingestion.model.CDC;
import com.google.dataflow.ingestion.model.CDC.Order;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.DB;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.TimestampedValues;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
        tableAdminClient.createTable(CreateTableRequest.of("cdc").addFamily("p"));
        tableAdminClient.createTable(CreateTableRequest.of("cdc_order").addFamily("o"));
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
        final TimestampedValues<Row> timestamped =
                Create.timestamped(
                                ImmutableList.of(inputRow),
                                ImmutableList.of(Instant.now().getMillis()))
                        .withCoder(RowCoder.of(p.getSchemaRegistry().getSchema(Person.class)));
        p.apply(timestamped)
                .apply(
                        new PersistCDCTransform<>(
                                bigtableEmulator.getPort(),
                                "fake-project",
                                "fake-instance",
                                "cdc",
                                CDC.Person.class,
                                DB.Person.class,
                                DB.Person::createFrom,
                                DB.Person.FIELD_CF_MAPPING));
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
    public void testPersonRemoval() throws Exception {
        dataClient.mutateRow(
                RowMutation.create(TableId.of("cdc"), "1234").setCell("p", "op_type", "i"));
        final SerializableFunction<Person, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Person.class);
        Person input =
                Person.newBuilder()
                        .setOpType("d")
                        .setAfterCity(null)
                        .setBeforeCity("city1")
                        .setAfterPersonId(null)
                        .setBeforePersonId(1234L)
                        .setAfterFirstName(null)
                        .setBeforeFirstName("john")
                        .setBeforeLastName("doe")
                        .setAfterLastName(null)
                        .build();
        Row inputRow = toRowFunction.apply(input);
        final TimestampedValues<Row> timestamped =
                Create.timestamped(
                                ImmutableList.of(inputRow),
                                ImmutableList.of(Instant.now().getMillis()))
                        .withCoder(RowCoder.of(p.getSchemaRegistry().getSchema(Person.class)));
        p.apply(timestamped)
                .apply(
                        new PersistCDCTransform<>(
                                bigtableEmulator.getPort(),
                                "fake-project",
                                "fake-instance",
                                "cdc",
                                CDC.Person.class,
                                DB.Person.class,
                                DB.Person::createFrom,
                                DB.Person.FIELD_CF_MAPPING));
        p.run();

        com.google.cloud.bigtable.data.v2.models.Row freshRow =
                dataClient.readRow(TableId.of("cdc"), "1234");
        Assert.assertEquals(
                freshRow.getCells("p", "op_type").get(0).getValue().toStringUtf8(), "d");
        Assert.assertEquals(freshRow.getCells("p", "lastName").size(), 0);
    }

    @Test
    public void testOrder() throws Exception {
        final SerializableFunction<Order, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Order.class);
        Order input =
                Order.newBuilder()
                        .setOpType("i")
                        .setAfterAddress("city1")
                        .setBeforeAddress("city1")
                        .setAfterPersonId(1234L)
                        .setBeforePersonId(1234L)
                        .setAfterItems("SKU1")
                        .setBeforeItems("SKU1")
                        .setAfterStatus("delivered")
                        .setBeforeStatus("shipping")
                        .setBeforeOrderId(1L)
                        .setAfterOrderId(1L)
                        .build();
        Row inputRow = toRowFunction.apply(input);
        final TimestampedValues<Row> timestamped =
                Create.timestamped(
                                ImmutableList.of(inputRow),
                                ImmutableList.of(Instant.now().getMillis()))
                        .withCoder(RowCoder.of(p.getSchemaRegistry().getSchema(Order.class)));
        p.apply(timestamped)
                .apply(
                        new PersistCDCTransform<>(
                                bigtableEmulator.getPort(),
                                "fake-project",
                                "fake-instance",
                                "cdc_order",
                                CDC.Order.class,
                                DB.Order.class,
                                DB.Order::createFrom,
                                DB.Order.FIELD_CF_MAPPING));
        p.run();

        com.google.cloud.bigtable.data.v2.models.Row freshRow =
                dataClient.readRow(TableId.of("cdc_order"), "1234_1");
        Assert.assertEquals(
                freshRow.getCells("o", "status").get(0).getValue().toStringUtf8(), "delivered");
        Assert.assertEquals(
                freshRow.getCells("o", "items").get(0).getValue().toStringUtf8(), "SKU1");
        Assert.assertEquals(
                freshRow.getCells("o", "address").get(0).getValue().toStringUtf8(), "city1");
    }

    @Test
    public void testOrderRemoval() throws Exception {
        dataClient.mutateRow(
                RowMutation.create(TableId.of("cdc_order"), "1234_1").setCell("o", "op_type", "i"));
        final SerializableFunction<Order, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Order.class);
        Order input =
                Order.newBuilder()
                        .setOpType("d")
                        .setAfterAddress(null)
                        .setBeforeAddress("city1")
                        .setAfterPersonId(null)
                        .setBeforePersonId(1234L)
                        .setAfterItems(null)
                        .setBeforeItems("SKU1")
                        .setAfterStatus(null)
                        .setBeforeStatus("shipping")
                        .setBeforeOrderId(1L)
                        .setAfterOrderId(null)
                        .build();

        Row inputRow = toRowFunction.apply(input);
        final TimestampedValues<Row> timestamped =
                Create.timestamped(
                                ImmutableList.of(inputRow),
                                ImmutableList.of(Instant.now().getMillis()))
                        .withCoder(RowCoder.of(p.getSchemaRegistry().getSchema(Order.class)));
        p.apply(timestamped)
                .apply(
                        new PersistCDCTransform<>(
                                bigtableEmulator.getPort(),
                                "fake-project",
                                "fake-instance",
                                "cdc_order",
                                CDC.Order.class,
                                DB.Order.class,
                                DB.Order::createFrom,
                                DB.Order.FIELD_CF_MAPPING));
        p.run();

        com.google.cloud.bigtable.data.v2.models.Row freshRow =
                dataClient.readRow(TableId.of("cdc_order"), "1234_1");
        Assert.assertEquals(
                freshRow.getCells("o", "op_type").get(0).getValue().toStringUtf8(), "d");
        Assert.assertEquals(freshRow.getCells("o", "status").size(), 0);
    }
}
