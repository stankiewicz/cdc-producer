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

import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ParseCDCTransform<T> extends PTransform<PCollection<String>, PCollection<Row>> {

    final Class<T> aClass;

    public ParseCDCTransform(Class aClass) {
        this.aClass = aClass;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
        Schema expectedSchema = null;
        try {
            expectedSchema = input.getPipeline().getSchemaRegistry().getSchema(aClass);

            return input.apply("Parse JSON to Beam Rows", ParseJsons.of(aClass))
                    .setCoder(AvroCoder.of(aClass))
                    .setSchema(
                            expectedSchema,
                            TypeDescriptor.of(aClass),
                            input.getPipeline().getSchemaRegistry().getToRowFunction(aClass),
                            input.getPipeline().getSchemaRegistry().getFromRowFunction(aClass))
                    .apply("toRow", Convert.toRows());
        } catch (NoSuchSchemaException e) {
            throw new RuntimeException(e);
        }
    }
}
