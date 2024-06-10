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

import com.google.common.collect.ImmutableMap;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.EventCoder;
import com.google.dataflow.ingestion.model.LocationChange;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildRecordTest {

    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    public void testLocationChange() throws Exception {
        p.getCoderRegistry()
                .registerCoderForClass(
                        LocationChange.class,
                        p.getSchemaRegistry().getSchemaCoder(LocationChange.class));
        p.getCoderRegistry()
                .registerCoderForClass(Event.class, EventCoder.of(p.getCoderRegistry()));

        PCollection<String> output =
                p.apply(
                                Create.of(
                                                KV.of(
                                                        "location_change",
                                                        Event.of(
                                                                LocationChange.create(
                                                                        "1234",
                                                                        "Warsaw",
                                                                        "John",
                                                                        "Doe",
                                                                        ImmutableMap.of(
                                                                                "1",
                                                                                "delivered")))))
                                        .withCoder(
                                                KvCoder.of(
                                                        StringUtf8Coder.of(),
                                                        EventCoder.of(p.getCoderRegistry()))))
                        .apply(ParDo.of(new BuildRecord()))
                        .apply(
                                MapElements.into(TypeDescriptors.strings())
                                        .via(pm -> new String(pm.getPayload())));
        String expected =
                "{\"city\":\"Warsaw\",\"first_name\":\"John\",\"last_name\":\"Doe\",\"person_id\":\"1234\"}";
        PAssert.that(output).containsInAnyOrder(expected);
        p.run().waitUntilFinish();
    }
}
