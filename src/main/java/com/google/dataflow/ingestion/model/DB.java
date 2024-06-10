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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DB {

    @AutoValue
    @DefaultSchema(AutoValueSchema.class)
    @DefaultCoder(AvroCoder.class)
    public abstract static class Person {

        public static final Map<String, Set<String>> FIELD_CF_MAPPING =
                ImmutableMap.of("p", ImmutableSet.of("op_type", "firstName", "lastName", "city"));

        @SchemaFieldName("op_type")
        public @Nullable abstract String getOpType();

        @SchemaFieldName("firstName")
        public @Nullable abstract String getFirstName();

        @SchemaFieldName("lastName")
        public @Nullable abstract String getLastName();

        @SchemaFieldName("city")
        public @Nullable abstract String getCity();

        @SchemaFieldName("key")
        @RowKey
        public @Nullable abstract String getPersonId();

        @SchemaCreate
        public static Person create(
                String opType,
                @Nullable String firstName,
                @Nullable String lastName,
                @Nullable String city,
                @Nullable String personId) {
            return new AutoValue_DB_Person(opType, firstName, lastName, city, personId);
        }

        public static Person createFrom(CDC.Person person) {
            return create(
                    person.getOpType(),
                    person.getAfterFirstName(),
                    person.getAfterLastName(),
                    person.getAfterCity(),
                    ""
                            + (person.getOpType().equals("d")
                                    ? person.getBeforePersonId()
                                    : person.getAfterPersonId()));
        }
    }

    @AutoValue
    @DefaultSchema(AutoValueSchema.class)
    @DefaultCoder(AvroCoder.class)
    public abstract static class Order {

        public static final Map<String, Set<String>> FIELD_CF_MAPPING =
            ImmutableMap.of("o", ImmutableSet.of("op_type", "items", "status", "address"));

        @SchemaFieldName("op_type")
        public @Nullable abstract String getOpType();

        @SchemaFieldName("status")
        public @Nullable abstract String getStatus();

        @SchemaFieldName("items")
        public @Nullable abstract String getItems();

        @SchemaFieldName("address")
        public @Nullable abstract String getAddress();

        @SchemaFieldName("key")
        public @Nullable abstract String getKey();

        @SchemaCreate
        public static Order create(
            String opType,
            @Nullable String status,
            @Nullable String items,
            @Nullable String address,
            @Nullable String key) {
            return new AutoValue_DB_Order(opType, status, items, address, key);
        }

        public static Order createFrom(CDC.Order order) {
            return create(
                order.getOpType(),
                order.getAfterStatus(),
                order.getAfterItems(),
                order.getAfterAddress(),
                ""
                    + (order.getOpType().equals("d")
                    ? order.getBeforePersonId()+"_" + order.getBeforeOrderId()
                    : order.getAfterPersonId()+"_" + order.getAfterOrderId()));
        }
    }
}
