package com.google.dataflow.ingestion.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;


@AutoValue
@DefaultSchema(AutoValueSchema.class)
@JsonSerialize(as = LocationChange.class)
public abstract class LocationChange {
  @SchemaCreate
  public static LocationChange create(String personId,
      String city, String firstName, String lastName){
    return new AutoValue_LocationChange(personId, city, firstName, lastName);
  }

  @JsonProperty("person_id")
  abstract String getPersonId();
  @JsonProperty("city")
  abstract String getCity();
  @JsonProperty("first_name")
  abstract String getFirstName();
  @JsonProperty("last_name")
  abstract String getLastName();
}
