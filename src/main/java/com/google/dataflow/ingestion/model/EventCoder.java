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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
            throws @UnknownKeyFor @NonNull @Initialized CoderException,
                    @UnknownKeyFor @NonNull @Initialized IOException {
        String type = value.getType().toString();

        stringUtf8Coder.encode(type, outStream);
        switch (value.getType()) {
            case LocationChange:
                locationChangeSchemaCoder.encode(value.getLocationChange(), outStream);
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
