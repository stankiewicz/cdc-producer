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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.dataflow.ingestion.model.Event;
import com.google.dataflow.ingestion.model.LocationChange;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class BuildRecord extends DoFn<KV<String, Event>, PubsubMessage> {

    @ProcessElement
    public void processElements(
            @Element KV<String, Event> topicWithEvent, OutputReceiver<PubsubMessage> outputReceiver)
            throws JsonProcessingException {
        Event event = topicWithEvent.getValue();
        String topic = topicWithEvent.getKey();
        ObjectMapper om =
                new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        String json = null;
        switch (event.getType()) {
            case LocationChange:
                json = om.writeValueAsString(event.getLocationChange());
                break;
        }
        PubsubMessage pm = new PubsubMessage(json.getBytes(), null).withTopic(topic);
        outputReceiver.output(pm);
    }
}
