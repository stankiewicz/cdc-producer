package com.google.dataflow.ingestion.pipeline;

import com.google.common.collect.ImmutableMap;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.dataflow.ingestion.model.LocationChange;
import com.google.dataflow.ingestion.transforms.ActionableTransform;
import com.google.dataflow.ingestion.transforms.BuildEvents;
import com.google.dataflow.ingestion.transforms.BuildRecord;
import com.google.dataflow.ingestion.transforms.ParseCDCTransform;
import com.google.dataflow.ingestion.transforms.PersistCDCTransform;
import com.google.dataflow.ingestion.model.Event;
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

  public interface Options
      extends  PipelineOptions {

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

    CDCPipeline.Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(CDCPipeline.Options.class);
    Pipeline pipeline = Pipeline.create(options);

    String projectId = options.getBigTableProjectId();
    String instanceId = options.getBigTableInstanceId();
    String tableId = options.getBigTableTableId();
    String appProfileId = options.getBigTableAppProfileId();

    // read person topic
    PCollection<String> cdc = pipeline.apply(PubsubIO.readStrings().fromTopic("person"));
    // parse to row
    final PCollection<Row> rows = cdc.apply("Extract Rows", new ParseCDCTransform(Person.class));


    rows.apply("Persist", new PersistCDCTransform(
        projectId, instanceId, tableId, appProfileId
    ));

    // TODO should this be run before CBT lookup or after?

    // ORDER MATTERS - feed immutable map in the same order
    Map<String, String> filters = ImmutableMap.of("locationChange","`before.CITY` <> `after.CITY`"
        ,"surnameChange","`before.LAST_NAME` <> `after.LAST_NAME`");
    PCollection<Row> actionable = rows.apply("Complex rule engine",
        new ActionableTransform("before.PERSON_ID", filters));

    final PCollection<Row> checked = actionable.apply("Checkpoint", Reshuffle.viaRandomKey());

    final PCollection<KV<String, Event>> events = checked.apply("BuildEvents",
        ParDo.of(new BuildEvents(projectId,instanceId,tableId, appProfileId)));

    final PCollection<PubsubMessage> recordsWithTopics = events.apply("Build Record", ParDo.of(new BuildRecord()));
    recordsWithTopics.apply("Send to consumers", PubsubIO.writeMessagesDynamic());

    pipeline.run();
  }
}
