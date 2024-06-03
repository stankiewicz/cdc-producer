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

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ActionableTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final Map<String, String> filters;
    private final String primaryKey;

    public ActionableTransform(String primaryKey, Map<String, String> filters) {
        this.filters = filters;
        this.primaryKey = primaryKey;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        StringBuilder sb = new StringBuilder();
        StringBuilder where = new StringBuilder();

        sb.append("select ");
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            sb.append("(" + entry.getValue() + ") as ");
            sb.append(entry.getKey());
            sb.append(",");
            where.append("(");
            where.append(entry.getValue());
            where.append(") or ");
        }
        where.append("FALSE");
        sb.append("CAST(`" + primaryKey + "` as varchar(255)) as key"); // cast??
        sb.append(" from PCOLLECTION WHERE ");
        sb.append(where.toString());
        return input.apply("Filter actionable", SqlTransform.query(sb.toString()));
    }
}
