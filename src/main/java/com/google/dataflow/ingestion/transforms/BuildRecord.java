package com.google.dataflow.ingestion.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.LocationChange;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class BuildRecord extends DoFn<KV<String, Event>, PubsubMessage> {

  @ProcessElement
  public void processElements(@Element KV<String,Event> topicWithEvent, OutputReceiver<PubsubMessage> outputReceiver)
      throws JsonProcessingException {
    Event event = topicWithEvent.getValue();
    String topic = topicWithEvent.getKey();
    ObjectMapper om = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    String json = null;
    switch (event.getType()){
      case LocationChange:
        json = om.writeValueAsString(event.getLocationChange());
    }
    PubsubMessage pm = new PubsubMessage(json.getBytes(), null).withTopic(topic);
    outputReceiver.output(pm);
  }

}
