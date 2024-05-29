package com.google.dataflow.ingestion.model;

import com.google.auto.value.AutoOneOf;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoOneOf(Event.Type.class)
@DefaultCoder(CustomCoder.class)
public abstract class Event {
  public enum Type {LocationChange, LastNameChange}
  public abstract Type getType();

  public abstract  LocationChange getLocationChange();

  public abstract  String getLastNameChange();

  @SchemaCreate
  public static Event of(LocationChange locationChange) {
    return AutoOneOf_Event.locationChange(locationChange);
  }

  @SchemaCreate
  public static Event of(String s) {
    return AutoOneOf_Event.lastNameChange(s);
  }


}
