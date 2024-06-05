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

package com.google.dataflow.ingestion.pipeline;

import com.google.common.collect.ImmutableMap;
import com.google.dataflow.ingestion.model.CDC;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.DB;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.transforms.ActionableTransform;
import com.google.dataflow.ingestion.transforms.BuildEvents;
import com.google.dataflow.ingestion.transforms.BuildRecord;
import com.google.dataflow.ingestion.transforms.ParseCDCTransform;
import com.google.dataflow.ingestion.transforms.PersistCDCTransform;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class CDCPipeline {

    public interface Options extends PipelineOptions {

        @Description("Input Topic")
        String getTopic();

        @Description("Project ID for Bigtable")
        String getBigTableProjectId();

        @Description("Instance ID for Bigtable")
        String getBigTableInstanceId();

        @Description("Table ID for Bigtable")
        String getBigTableTableId();

        @Description("AppProfile ID for Bigtable")
        String getBigTableAppProfileId();
    }

    public static void main(String[] args) {

        CDCPipeline.Options options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(CDCPipeline.Options.class);
        Pipeline pipeline = Pipeline.create(options);

        String projectId = options.getBigTableProjectId();
        String instanceId = options.getBigTableInstanceId();
        String tableId = options.getBigTableTableId();
        String appProfileId = options.getBigTableAppProfileId();

        // read person topic
        PCollection<String> cdc =
                pipeline.apply(PubsubIO.readStrings().fromTopic(options.getTopic()));
        // parse to row
        final PCollection<Row> rows =
                cdc.apply("Extract Rows", new ParseCDCTransform<Person>(Person.class));

        rows.apply(
                "Persist",
                new PersistCDCTransform<>(
                        projectId,
                        instanceId,
                        tableId,
                        appProfileId,
                        CDC.Person.class,
                        DB.Person.class,
                        DB.Person::createFrom,
                        DB.Person.FIELD_CF_MAPPING));

        // TODO should this be run before CBT lookup or after?

        // ORDER MATTERS - feed immutable map in the same order
        Map<String, String> filters =
                ImmutableMap.of(
                        "locationChange",
                        "`before_CITY` <> `after_CITY`",
                        "surnameChange",
                        "`before_LAST_NAME` <> `after_LAST_NAME`");
        PCollection<Row> actionable =
                rows.apply(
                        "Complex rule engine",
                        new ActionableTransform("before_PERSON_ID", filters));

        final PCollection<Row> checked = actionable.apply("Checkpoint", Reshuffle.viaRandomKey());

        final PCollection<KV<String, Event>> events =
                checked.apply(
                        "BuildEvents",
                        ParDo.of(new BuildEvents(projectId, instanceId, tableId, appProfileId)));

        final PCollection<PubsubMessage> recordsWithTopics =
                events.apply("Build Record", ParDo.of(new BuildRecord()));
        recordsWithTopics.apply("Send to consumers", PubsubIO.writeMessagesDynamic());

        pipeline.run();
    }
}
