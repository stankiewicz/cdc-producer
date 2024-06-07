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

package com.google.dataflow.feature;

import com.google.dataflow.feature.model.ClickstreamEvent;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FeatureCalculationTest {

    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    public void testFeatureCalculation() throws Exception {

        p.getSchemaRegistry().registerSchemaProvider(ClickstreamEvent.class, new AutoValueSchema());
        p.getCoderRegistry()
                .registerCoderForClass(
                        ClickstreamEvent.class,
                        SchemaCoder.of(p.getSchemaRegistry().getSchema(ClickstreamEvent.class)));

        final TestStream<String> clickstreamEvents =
                TestStream.create(StringUtf8Coder.of())
                        .addElements(
                                TimestampedValue.of(
                                        "{\"personId\":\"1234\",\"eventKind\":\"click\",\"sessionStarted\":\"1970-01-01T00:00:00\""
                                            + " }",
                                        Instant.ofEpochMilli(1)))
                        // .addElements(TimestampedValue.of(ClickstreamEvent.newBuilder().setPersonId("1234").setEventKind("click").setSessionStarted(Instant.ofEpochSecond(1)).build(),Instant.ofEpochSecond(1)))
                        .advanceWatermarkTo(Instant.ofEpochSecond(20))
                        .advanceWatermarkTo(Instant.ofEpochSecond(40))
                        .advanceWatermarkTo(Instant.ofEpochSecond(61))
                        .advanceWatermarkTo(Instant.ofEpochSecond(70))
                        .advanceWatermarkTo(Instant.ofEpochSecond(91))
                        .advanceWatermarkTo(Instant.ofEpochSecond(121))
                        .advanceWatermarkTo(Instant.ofEpochSecond(151))
                        // .addElements(TimestampedValue.of(ClickstreamEvent.newBuilder().setPersonId("1234").setEventKind("click").setSessionStarted(Instant.ofEpochSecond(62)).build(),Instant.ofEpochMilli(62)))
                        .advanceWatermarkToInfinity();

        final PCollection<Row> input =
                p.apply(clickstreamEvents)
                        .setCoder(StringUtf8Coder.of())
                        .apply(ParseJsons.of(ClickstreamEvent.class))
                        .setCoder(p.getCoderRegistry().getCoder(ClickstreamEvent.class))
                        .setSchema(
                                p.getSchemaRegistry().getSchema(ClickstreamEvent.class),
                                TypeDescriptor.of(ClickstreamEvent.class),
                                p.getSchemaRegistry().getToRowFunction(ClickstreamEvent.class),
                                p.getSchemaRegistry().getFromRowFunction(ClickstreamEvent.class))
                        .apply("toRow", Convert.toRows());

        final PCollection<KV<String, Long>> f1Rewindow =
                input.apply(
                        "f1",
                        new NRTFeature<>(
                                TypeDescriptors.longs(),
                                "personId",
                                "count(*)",
                                Duration.standardSeconds(90),
                                Duration.standardSeconds(30),
                                0L));

        final PCollection<KV<String, Long>> f2Rewindow =
                input.apply(
                        "f2",
                        new NRTFeature<>(
                                TypeDescriptors.longs(),
                                "personId",
                                "count(*)",
                                Duration.standardSeconds(60),
                                Duration.standardSeconds(30),
                                0L));
        final TupleTag<Long> t1 = MergeFeatures.T1;
        final TupleTag<Long> t2 = MergeFeatures.T2;
        PCollection<KV<String, CoGbkResult>> result =
                KeyedPCollectionTuple.of(t1, f1Rewindow)
                        .and(t2, f2Rewindow)
                        .apply(CoGroupByKey.create());

        final PCollection<String> formatted = result.apply(ParDo.of(new MergeFeatures()));

        List<String> expected = new ArrayList<>();
        expected.add("1234,1,1,1970-01-01T00:00:29.999Z");
        expected.add("1234,1,1,1970-01-01T00:00:59.999Z");
        expected.add("1234,1,0,1970-01-01T00:01:29.999Z");
        expected.add("1234,0,null,1970-01-01T00:01:59.999Z");
        // no 1234,null,null
        PAssert.that(formatted).containsInAnyOrder(expected);

        p.run().waitUntilFinish();
    }
}
