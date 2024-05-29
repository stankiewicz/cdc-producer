package com.google.dataflow.ingestion.transforms;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.LocationChange;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PersistCDCTest {

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


  }

  @Test
  public void testPerson() throws Exception {
    final SerializableFunction<Person, Row> toRowFunction = p.getSchemaRegistry()
        .getToRowFunction(Person.class);
    Person input = Person.newBuilder().setOpType("i")
        .setAfterCity("city2").setBeforeCity("city1")
        .setAfterPersonId(1234L).setBeforePersonId(1234L)
        .setAfterFirstName("john").setBeforeFirstName("john")
        .setBeforeLastName("doe").setAfterLastName("doe").build();
    Row inputRow = toRowFunction.apply(input);
    p.apply(Create.of(inputRow)
            .withCoder(
                RowCoder.of(p.getSchemaRegistry().getSchema(Person.class))))
            .apply(new PersistCDCTransform(bigtableEmulator.getPort(),"fake-project","fake-instance","cdc")
                );
    p.run();

    com.google.cloud.bigtable.data.v2.models.Row freshRow = dataClient.readRow(TableId.of("cdc"),"1234");
   Assert.assertEquals(freshRow.getCells("p","firstName").get(0).getValue().toStringUtf8(),"john");
    Assert.assertEquals(freshRow.getCells("p","lastName").get(0).getValue().toStringUtf8(),"doe");
    Assert.assertEquals(freshRow.getCells("p","city").get(0).getValue().toStringUtf8(),"city2");

  }

}