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

package com.google.dataflow.ingestion.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
@JsonSerialize(as = LocationChange.class)
public abstract class LocationChange {
    @SchemaCreate
    public static LocationChange create(
            String personId,
            String city,
            String firstName,
            String lastName,
            Map<String, String> ordersWithStatus) {
        return new AutoValue_LocationChange(personId, city, firstName, lastName, ordersWithStatus);
    }

    @JsonProperty("person_id")
    abstract String getPersonId();

    @JsonProperty("city")
    abstract String getCity();

    @JsonProperty("first_name")
    abstract String getFirstName();

    @JsonProperty("last_name")
    abstract String getLastName();

    @JsonProperty("ordersWithStatus")
    abstract Map<String, String> getOrdersWithStatus();
}
