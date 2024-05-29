package com.google.dataflow.ingestion.transforms;

import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.EventCoder;
import com.google.dataflow.ingestion.model.LocationChange;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildRecordTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testLocationChange() throws Exception {
    p.getCoderRegistry().registerCoderForClass(LocationChange.class, p.getSchemaRegistry().getSchemaCoder(
        LocationChange.class));
    p.getCoderRegistry().registerCoderForClass(Event.class, EventCoder.of(p.getCoderRegistry()));

    PCollection<String> output =
        p.apply(Create.of(KV.of("location_change",
                Event.of(LocationChange.create("1234","Warsaw","John","Doe")))).withCoder(KvCoder.of(StringUtf8Coder.of(),EventCoder.of(p.getCoderRegistry()))))

            .apply(
                ParDo.of(new BuildRecord())).apply(MapElements.into(TypeDescriptors.strings()).via(pm->new String(pm.getPayload())))
        ;
    String expected = "{\"city\":\"Warsaw\",\"first_name\":\"John\",\"last_name\":\"Doe\",\"person_id\":\"1234\"}";
    PAssert.that(output).containsInAnyOrder(expected);
    p.run().waitUntilFinish();
  }

}