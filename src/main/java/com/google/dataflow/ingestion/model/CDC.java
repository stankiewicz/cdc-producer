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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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
    @JsonSerialize(as = Person.class)
    @JsonDeserialize(builder = Person.Builder.class)
    public abstract static class Person {

        @SchemaFieldName("op_type")
        @JsonProperty("op_type")
        public @Nullable abstract String getOpType();

        @SchemaFieldName("before_FIRST_NAME")
        @JsonProperty("before.FIRST_NAME")
        public @Nullable abstract String getBeforeFirstName();

        @SchemaFieldName("after_FIRST_NAME")
        @JsonProperty("after.FIRST_NAME")
        public @Nullable abstract String getAfterFirstName();

        @SchemaFieldName("before_LAST_NAME")
        @JsonProperty("before.LAST_NAME")
        public @Nullable abstract String getBeforeLastName();

        @SchemaFieldName("after_LAST_NAME")
        @JsonProperty("after.LAST_NAME")
        public @Nullable abstract String getAfterLastName();

        @SchemaFieldName("before_CITY")
        @JsonProperty("before.CITY")
        public @Nullable abstract String getBeforeCity();

        @SchemaFieldName("after_CITY")
        @JsonProperty("after.CITY")
        public @Nullable abstract String getAfterCity();

        @SchemaFieldName("before_PERSON_ID")
        @JsonProperty("before.PERSON_ID")
        public @Nullable abstract Long getBeforePersonId();

        @SchemaFieldName("after_PERSON_ID")
        @JsonProperty("after.PERSON_ID")
        @RowKey
        public @Nullable abstract Long getAfterPersonId();

        public static Builder newBuilder() {
            return new AutoValue_CDC_Person.Builder();
        }

        @AutoValue.Builder
        public abstract static class Builder {

            @JsonCreator
            public static Builder builder() {
                return new AutoValue_CDC_Person.Builder();
            }

            @JsonProperty("op_type")
            public abstract Builder setOpType(@Nullable String opType);

            @JsonProperty("before.FIRST_NAME")
            public abstract Builder setBeforeFirstName(@Nullable String beforeFirstName);

            @JsonProperty("after.FIRST_NAME")
            public abstract Builder setAfterFirstName(@Nullable String afterFirstName);

            @JsonProperty("before.CITY")
            public abstract Builder setBeforeCity(@Nullable String beforeCity);

            @JsonProperty("after.CITY")
            public abstract Builder setAfterCity(@Nullable String afterCity);

            @JsonProperty("before.LAST_NAME")
            public abstract Builder setBeforeLastName(@Nullable String beforeLastName);

            @JsonProperty("after.LAST_NAME")
            public abstract Builder setAfterLastName(@Nullable String afterLastName);

            @JsonProperty("before.PERSON_ID")
            public abstract Builder setBeforePersonId(@Nullable Long beforePersonId);

            @JsonProperty("after.PERSON_ID")
            public abstract Builder setAfterPersonId(@Nullable Long afterPersonId);

            public abstract Person build();
        }
    }

    /**
     * An Order requested by a Person
     * The "AutoValue" annotation will generate source code for a AutoValue_CDC_Order class
     * The "DefaultSchema" annotation
     */
    @AutoValue
    @DefaultSchema(AutoValueSchema.class)
    @DefaultCoder(AvroCoder.class)
    public abstract static class Order {

        @SchemaFieldName("op_type")
        public @Nullable abstract String getOpType();

        public static Builder newBuilder() {
            return new AutoValue_CDC_Order.Builder();
        }

        @AutoValue.Builder
        public abstract static class Builder {

            public abstract Builder setOpType(@Nullable String opType);

            public abstract Order build();
        }
    }
}
