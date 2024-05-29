package com.google.dataflow.ingestion.transforms;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.EventCoder;
import com.google.dataflow.ingestion.model.LocationChange;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildEventsTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Rule
  public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

  // Clients that will be connected to the emulator
  private BigtableTableAdminClient tableAdminClient;
  private BigtableDataClient dataClient;

  @Before
  public void setUp() throws IOException, ExecutionException, InterruptedException {
    // Initialize the clients to connect to the emulator
    BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort());
    ;
    tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.setProjectId("fake-project").setInstanceId("fake-instance").build());

    BigtableDataSettings.Builder dataSettings = BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort());
    dataClient = BigtableDataClient.create(dataSettings.setProjectId("fake-project").setInstanceId("fake-instance").build());

    // Create a test table that can be used in tests
    tableAdminClient.createTable(
        CreateTableRequest.of("cdc")
            .addFamily("p")
    );

    ApiFuture<Void> mutateFuture = dataClient.mutateRowAsync(
        RowMutation.create("cdc", "1234")
            .setCell("p", "city", "Warsaw")
            .setCell("p", "firstName", "John")
            .setCell("p", "lastName", "Doe")
    );

    mutateFuture.get();
  }
  @Test
  public void testLocationChangeEvent() throws Exception {
    p.getSchemaRegistry().registerSchemaProvider(
        LocationChange.class, new AutoValueSchema());

    p.getCoderRegistry();
    p.getSchemaRegistry();
    p.getCoderRegistry().registerCoderForClass(LocationChange.class, p.getSchemaRegistry().getSchemaCoder(
        LocationChange.class));

    p.getCoderRegistry().registerCoderForClass(Event.class, EventCoder.of(p.getCoderRegistry()));
    Schema schema = Schema.of(
        Field.of("locationChange", FieldType.BOOLEAN),
        Field.of("surnameChange", FieldType.BOOLEAN),
        Field.of("key", FieldType.STRING));
    final Row locationChange = Row.withSchema(
            // order must match SQL
            schema)
        .withFieldValue("key", "1234")
        .withFieldValue("locationChange", true)
        .withFieldValue("surnameChange", false).build();

    PCollection<KV<String, LocationChange>> output =
        p.apply(Create.of(locationChange).withCoder(RowCoder.of(schema)))
            .apply(ParDo.of(new BuildEvents(bigtableEmulator.getPort(),"fake-project","fake-instance","cdc")))
            .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptor.of(
                LocationChange.class)))
                .via(kv -> KV.of(kv.getKey(),kv.getValue().getLocationChange())))
                    ;

    PAssert.that(output).containsInAnyOrder(KV.of("location_change",
        LocationChange.create("1234","Warsaw","John","Doe")));
    p.run().waitUntilFinish();
  }

}