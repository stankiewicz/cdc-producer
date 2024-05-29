package com.google.dataflow.ingestion.transforms;

import com.google.common.collect.ImmutableMap;
import com.google.dataflow.ingestion.model.CDC.Person;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActionableTransformTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testLocationChange() throws Exception {

    p.getSchemaRegistry().registerSchemaProvider(
        Person.class, new AutoValueSchema());
    p.getCoderRegistry().registerCoderForClass(Person.class,AvroCoder.of(Person.class));

    // this map keeps order
    Map filters = ImmutableMap.of("locationChange","`before.CITY` <> `after.CITY`"
        ,"surnameChange","`before.LAST_NAME` <> `after.LAST_NAME`");

    Person input = Person.newBuilder().setOpType("i")
        .setAfterCity("city2").setBeforeCity("city1")
        .setAfterPersonId(1234L).setBeforePersonId(1234L)
        .setAfterFirstName("john").setBeforeFirstName("john")
        .setBeforeLastName("doe").setAfterLastName("doe").build();
    final SerializableFunction<Person, Row> toRowFunction = p.getSchemaRegistry()
        .getToRowFunction(Person.class);
    PCollection<Row> output =
        p.apply(Create.of(toRowFunction.apply(input)).withCoder(RowCoder.of(p.getSchemaRegistry().getSchema(Person.class))))
            .apply(new ActionableTransform("before.PERSON_ID", filters));


    PAssert.that(output).containsInAnyOrder(
        Row.withSchema(
            // order must match SQL
            Schema.of(
                Field.of("locationChange", FieldType.BOOLEAN),
                Field.of("surnameChange", FieldType.BOOLEAN),
                Field.of("key", FieldType.STRING)))
                .withFieldValue("key", "1234")
                .withFieldValue("locationChange",true)

            .withFieldValue("surnameChange", false).build());
    p.run().waitUntilFinish();
  }

}