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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CDC {

    @AutoValue
    @DefaultSchema(AutoValueSchema.class)
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

    @AutoValue
    @DefaultSchema(AutoValueSchema.class)
    @JsonSerialize(as = Order.class)
    @JsonDeserialize(builder = Order.Builder.class)
    public abstract static class Order {

        @SchemaFieldName("op_type")
        @JsonProperty("op_type")
        public @Nullable abstract String getOpType();

        @SchemaFieldName("before_STATUS")
        @JsonProperty("before.STATUS")
        public @Nullable abstract String getBeforeStatus();

        @SchemaFieldName("after_STATUS")
        @JsonProperty("after.STATUS")
        public @Nullable abstract String getAfterStatus();

        @SchemaFieldName("before_ITEMS")
        @JsonProperty("before.ITEMS")
        public @Nullable abstract String getBeforeItems();

        @SchemaFieldName("after_ITEMS")
        @JsonProperty("after.ITEMS")
        public @Nullable abstract String getAfterItems();

        @SchemaFieldName("before_ADDRESS")
        @JsonProperty("before.ADDRESS")
        public @Nullable abstract String getBeforeAddress();

        @SchemaFieldName("after_ADDRESS")
        @JsonProperty("after.ADDRESS")
        public @Nullable abstract String getAfterAddress();

        @SchemaFieldName("before_PERSON_ID")
        @JsonProperty("before.PERSON_ID")
        public @Nullable abstract Long getBeforePersonId();

        @SchemaFieldName("after_PERSON_ID")
        @JsonProperty("after.PERSON_ID")
        public @Nullable abstract Long getAfterPersonId();

        @SchemaFieldName("before_ORDER_ID")
        @JsonProperty("before.ORDER_ID")
        public @Nullable abstract Long getBeforeOrderId();

        @SchemaFieldName("after_ORDER_ID")
        @JsonProperty("after.ORDER_ID")
        public @Nullable abstract Long getAfterOrderId();

        public static Builder newBuilder() {
            return AutoValue_CDC_Order.newBuilder();
        }

        @AutoValue.Builder
        public abstract static class Builder {

            @JsonCreator
            public static Builder builder() {
                return AutoValue_CDC_Order.newBuilder();
            }

            @JsonProperty("op_type")
            public abstract Builder setOpType(@Nullable String opType);

            @JsonProperty("before.STATUS")
            public abstract Builder setBeforeStatus(@Nullable String beforeStatus);

            @JsonProperty("after.STATUS")
            public abstract Builder setAfterStatus(@Nullable String afterStatus);

            @JsonProperty("before.ADDRESS")
            public abstract Builder setBeforeAddress(@Nullable String beforeAddress);

            @JsonProperty("after.ADDRESS")
            public abstract Builder setAfterAddress(@Nullable String afterAddress);

            @JsonProperty("before.ITEMS")
            public abstract Builder setBeforeItems(@Nullable String beforeItems);

            @JsonProperty("after.ITEMS")
            public abstract Builder setAfterItems(@Nullable String afterItems);

            @JsonProperty("before.PERSON_ID")
            public abstract Builder setBeforePersonId(@Nullable Long beforePersonId);

            @JsonProperty("after.PERSON_ID")
            public abstract Builder setAfterPersonId(@Nullable Long afterPersonId);

            @JsonProperty("before.PERSON_ID")
            public abstract Builder setBeforeOrderId(@Nullable Long beforeOrderId);

            @JsonProperty("after.PERSON_ID")
            public abstract Builder setAfterOrderId(@Nullable Long afterOrderId);

            public abstract Order build();
        }
    }
}
