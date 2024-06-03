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

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ParseCDCTransform extends PTransform<PCollection<String>, PCollection<Row>> {

    final Class aClass;

    public ParseCDCTransform(Class aClass) {
        this.aClass = aClass;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
        Schema expectedSchema = null;
        try {
            final Class<? extends Class> aClass1 = aClass.getClass();
            expectedSchema = input.getPipeline().getSchemaRegistry().getSchema(aClass);
        } catch (NoSuchSchemaException e) {
            throw new RuntimeException(e);
        }

        return input.apply("Parse JSON to Beam Rows", JsonToRow.withSchema(expectedSchema));
    }
}
