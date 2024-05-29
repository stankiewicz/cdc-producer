package com.google.dataflow.ingestion.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class EventCoder extends CustomCoder<Event> {

  public static EventCoder of(CoderRegistry cr) throws CannotProvideCoderException {
    return new EventCoder(cr);
  }
  Coder<LocationChange> locationChangeSchemaCoder;
  final StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();
  public EventCoder(CoderRegistry cr) throws CannotProvideCoderException {
    locationChangeSchemaCoder = cr.getCoder(LocationChange.class);
  }

  @Override
  public void encode(Event value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
      throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
      String type = value.getType().toString();

      stringUtf8Coder.encode(type,outStream);
    switch (value.getType()) {
      case LocationChange:
        locationChangeSchemaCoder.encode(value.getLocationChange(),outStream);
        break;
      case LastNameChange:
        break;
    }
  }

  @Override
  public @UnknownKeyFor @Nullable @Initialized Event decode(
      @UnknownKeyFor @NonNull @Initialized InputStream inStream)
      throws CoderException, IOException {
    String type = stringUtf8Coder.decode(inStream);
    switch (Event.Type.valueOf(type)) {
      case LocationChange:
        return Event.of(locationChangeSchemaCoder.decode(inStream));
      case LastNameChange:
        break;
    }
    return null;
  }
}
