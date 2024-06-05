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

package com.google.dataflow.ingestion.bigtable;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.DeleteFromColumn;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/**
 * Bigtable reference: <a
 * href=https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2></a>.
 *
 * <p>Requires a flat schema and a mapping which column family corresponds to which column.
 *
 * <p>{@link Row} represents Bigtable {@link Mutation} in the following way:
 *
 * <p>Mapped Beam {@link Row}:
 *
 * <p>BEAM_ROW: ROW<key STRING, [columnQualifier VALUE]+>
 *
 * <p>VALUE: Beam {@link Schema} type except for ARRAY, DECIMAL, ITERABLE, MAP, ROW
 *
 * <p>Mapped {@link Mutation}:
 *
 * <p>mutation: key, setCell[]
 *
 * <p>setCell: familyName, columnQualifier, ROW[columnQualifier] - familyName comes from the column:
 * family mapping
 */
public class BeamRowToBigtableMutation
        extends PTransform<PCollection<Row>, PCollection<KV<ByteString, Iterable<Mutation>>>>
        implements Serializable {

    private final Map<String, String> columnFamilyMapping;

    public BeamRowToBigtableMutation(Map<String, Set<String>> familyColumnMapping) {
        columnFamilyMapping =
                familyColumnMapping.entrySet().stream()
                        .flatMap(
                                kv ->
                                        kv.getValue().stream()
                                                .map(column -> KV.of(column, kv.getKey())))
                        .collect(toMap(KV::getKey, KV::getValue));
    }

    @Override
    public PCollection<KV<ByteString, Iterable<Mutation>>> expand(PCollection<Row> input) {
        return input.apply(ParDo.of(new ToBigtableRowFn(columnFamilyMapping)));
    }

    public static class ToBigtableRowFn extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

        private final Map<String, String> columnFamilyMapping;
        private final CellValueParser cellValueParser = new CellValueParser();

        public ToBigtableRowFn(Map<String, String> columnFamilyMapping) {
            this.columnFamilyMapping = columnFamilyMapping;
        }

        @ProcessElement
        public void processElement(
                @Element Row row,
                @Timestamp Instant ts,
                OutputReceiver<KV<ByteString, Iterable<Mutation>>> or) {
            List<Mutation> mutations =
                    columnFamilyMapping.entrySet().stream()
                            .map(
                                    columnFamily ->
                                            mutation(
                                                    columnFamily.getValue(),
                                                    columnFamily.getKey(),
                                                    row,
                                                    ts))
                            .collect(toList());
            String key = row.getString(KEY);
            if (key != null) {
                or.output(KV.of(ByteString.copyFromUtf8(key), mutations));
            } else {
                throw new NullPointerException("Null key");
            }
        }

        private Mutation mutation(String family, String column, Row row, Instant t) {
            if (row.getValue(column) == null) {
                return Mutation.newBuilder()
                        .setDeleteFromColumn(
                                DeleteFromColumn.newBuilder()
                                        .setFamilyName(family)
                                        .setColumnQualifier(ByteString.copyFromUtf8(column))
                                        .build())
                        .build();
            } else {
                return Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setValue(convertValueToByteString(row, column))
                                        .setColumnQualifier(ByteString.copyFromUtf8(column))
                                        .setFamilyName(family)
                                        .setTimestampMicros(t.getMillis() * 1000L)
                                        .build())
                        .build();
            }
        }

        private ByteString convertValueToByteString(Row row, String column) {
            Schema.Field field = row.getSchema().getField(column);
            Object value = row.getValue(column);
            if (value == null) {
                throw new NullPointerException("Null value at column " + column);
            } else {
                return cellValueParser.valueToByteString(value, field.getType());
            }
        }
    }
}
