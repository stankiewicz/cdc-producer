package com.google.dataflow.ingestion.model;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.checkerframework.checker.nullness.qual.Nullable;


public class CDC {

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  @DefaultCoder(AvroCoder.class)
  public abstract static class Person {

    @SchemaFieldName("op_type")
    public @Nullable
    abstract String getOpType();

    @SchemaFieldName("before.FIRST_NAME")
    public @Nullable
    abstract String getBeforeFirstName();

    @SchemaFieldName("after.FIRST_NAME")
    public @Nullable
    abstract String getAfterFirstName();

    @SchemaFieldName("before.LAST_NAME")
    public @Nullable
    abstract String getBeforeLastName();

    @SchemaFieldName("after.LAST_NAME")
    public @Nullable
    abstract String getAfterLastName();

    @SchemaFieldName("before.CITY")
    public @Nullable
    abstract String getBeforeCity();

    @SchemaFieldName("after.CITY")
    public @Nullable
    abstract String getAfterCity();

    @SchemaFieldName("before.PERSON_ID")
    public @Nullable
    abstract Long getBeforePersonId();

    @SchemaFieldName("after.PERSON_ID")
    @RowKey
    public @Nullable
    abstract Long getAfterPersonId();

    public static Builder newBuilder() {
      return new AutoValue_CDC_Person.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setOpType(@Nullable String opType);

      public abstract Builder setBeforeFirstName(@Nullable String beforeFirstName);

      public abstract Builder setAfterFirstName(@Nullable String afterFirstName);

      public abstract Builder setBeforeCity(@Nullable String beforeCity);

      public abstract Builder setAfterCity(@Nullable String afterCity);

      public abstract Builder setBeforeLastName(@Nullable String beforeLastName);

      public abstract Builder setAfterLastName(@Nullable String afterLastName);

      public abstract Builder setBeforePersonId(@Nullable Long beforePersonId);

      public abstract Builder setAfterPersonId(@Nullable Long afterPersonId);

      public abstract Person build();
    }
  }

}

