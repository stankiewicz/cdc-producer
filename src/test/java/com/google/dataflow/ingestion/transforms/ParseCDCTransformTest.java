package com.google.dataflow.ingestion.transforms;

import com.google.dataflow.ingestion.model.CDC.Person;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParseCDCTransformTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testPerson() throws Exception {

    p.getSchemaRegistry().registerSchemaProvider(
        Person.class, new AutoValueSchema());


    List<String> jsons = Arrays.asList("{\"op_type\":\"i\",\"before.FIRST_NAME\":\"john\",\"after.FIRST_NAME\":\"john\",\"before.LAST_NAME\":\"doe\",\"after.LAST_NAME\":\"doe\",\"before.CITY\":\"city1\",\"after.CITY\":\"city2\",\"before.PERSON_ID\":1234,\"after.PERSON_ID\":1234 }");
    PCollection<Row> output =
        p.apply(Create.of(jsons).withCoder(StringUtf8Coder.of()))
            .apply(new ParseCDCTransform(Person.class));

    final SerializableFunction<Person, Row> toRowFunction = p.getSchemaRegistry()
        .getToRowFunction(Person.class);
    Person expected = Person.newBuilder().setOpType("i")
        .setAfterCity("city2").setBeforeCity("city1")
        .setAfterPersonId(1234L).setBeforePersonId(1234L)
        .setAfterFirstName("john").setBeforeFirstName("john")
        .setBeforeLastName("doe").setAfterLastName("doe").build();
    PAssert.that(output).containsInAnyOrder(toRowFunction.apply(expected));
    p.run().waitUntilFinish();
  }

}