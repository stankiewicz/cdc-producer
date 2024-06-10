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

import com.google.common.collect.ImmutableList;
import com.google.dataflow.ingestion.model.CDC.Order;
import com.google.dataflow.ingestion.model.CDC.Person;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.TimestampedValues;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParseCDCTransformTest {

    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    public void testPerson() throws Exception {

        p.getSchemaRegistry().registerSchemaProvider(Person.class, new AutoValueSchema());

        List<String> jsons =
                Arrays.asList(
                        "{\"op_type\":\"i\",\"before.FIRST_NAME\":\"john\",\"after.FIRST_NAME\":\"john\",\"before.LAST_NAME\":\"doe\",\"after.LAST_NAME\":\"doe\",\"before.CITY\":\"city1\",\"after.CITY\":\"city2\",\"before.PERSON_ID\":1234,\"after.PERSON_ID\":1234"
                            + " }");
        final TimestampedValues<String> timestamped =
                Create.timestamped(jsons, ImmutableList.of(Instant.now().getMillis()));
        PCollection<Row> output =
                p.apply(timestamped)
                        // .withCoder(TimestampedValueCoder.of(StringUtf8Coder.of())
                        .apply(new ParseCDCTransform<Person>(Person.class));

        final SerializableFunction<Person, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Person.class);
        Person expected =
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
        PAssert.that(output).containsInAnyOrder(toRowFunction.apply(expected));
        p.run().waitUntilFinish();
    }

    @Test
    public void testOrder() throws Exception {

        p.getSchemaRegistry().registerSchemaProvider(Person.class, new AutoValueSchema());

        List<String> jsons =
                Arrays.asList(
                        "{\"op_type\":\"i\",\"before.STATUS\":\"shipping\",\"after.STATUS\":\"delivered\",\"before.ITEMS\":\"SKU1\",\"after.ITEMS\":\"SKU1\",\"before.ADDRESS\":\"city1\",\"after.ADDRESS\":\"city1\",\"before.PERSON_ID\":1234,\"after.PERSON_ID\":1234,\"before.ORDER_ID\":1,\"after.ORDER_ID\":1"
                            + " }");
        final TimestampedValues<String> timestamped =
                Create.timestamped(jsons, ImmutableList.of(Instant.now().getMillis()));
        PCollection<Row> output =
                p.apply(timestamped)
                        // .withCoder(TimestampedValueCoder.of(StringUtf8Coder.of())
                        .apply(new ParseCDCTransform<Order>(Order.class));

        final SerializableFunction<Order, Row> toRowFunction =
                p.getSchemaRegistry().getToRowFunction(Order.class);
        Order expected =
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
        PAssert.that(output).containsInAnyOrder(toRowFunction.apply(expected));
        p.run().waitUntilFinish();
    }
}
