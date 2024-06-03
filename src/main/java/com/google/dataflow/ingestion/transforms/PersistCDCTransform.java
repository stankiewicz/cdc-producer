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

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.DeleteFromFamily;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.dataflow.ingestion.model.CDC.Person;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class PersistCDCTransform extends PTransform<PCollection<Row>, PDone> {

    private final String projectId;
    private final String instanceId;
    private final String tableId;

    private final String appProfileId;

    int port;

    boolean test = false;

    @VisibleForTesting
    PersistCDCTransform(int port, String projectId, String instanceId, String tableId) {
        test = true;
        this.tableId = tableId;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.port = port;
        this.appProfileId = null;
    }

    public PersistCDCTransform(
            String projectId, String instanceId, String tableId, String appProfileId) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.appProfileId = appProfileId;
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        PCollection<KV<ByteString, Iterable<Mutation>>> mutations =
                input.apply("Convert to DTO", Convert.fromRows(Person.class))
                        .apply("Convert to Mutation", ParDo.of(new ConvertPersonToMutation()));
        if (test) {
            return mutations.apply(
                    "Write mutations",
                    BigtableIO.write()
                            .withEmulator("localhost:" + port)
                            .withTableId(tableId)
                            .withInstanceId(instanceId)
                            .withProjectId(projectId));
        } else {
            return mutations.apply(
                    "Write mutations",
                    BigtableIO.write()
                            .withTableId(tableId)
                            .withAppProfileId(appProfileId)
                            .withInstanceId(instanceId)
                            .withProjectId(projectId));
        }
    }

    public static class ConvertPersonToMutation
            extends DoFn<Person, KV<ByteString, Iterable<Mutation>>> {

        @ProcessElement
        public void processElement(
                @Element Person inputElement,
                @Timestamp Instant ts,
                ProcessContext c,
                OutputReceiver<KV<ByteString, Iterable<Mutation>>> outputReceiver) {
            long timestamp = System.currentTimeMillis();
            List<Mutation> mutations = new ArrayList<>();
            if (inputElement.getOpType().equals("u") || inputElement.getOpType().equals("i")) {
                // setCell mutations can be reduced based on before/after change

                mutations.add(
                        Mutation.newBuilder()
                                .setSetCell(
                                        SetCell.newBuilder()
                                                .setColumnQualifier(
                                                        ByteString.copyFromUtf8("firstName"))
                                                .setValue(
                                                        ByteString.copyFromUtf8(
                                                                inputElement.getAfterFirstName()))
                                                .setFamilyName("p")
                                                .setTimestampMicros(timestamp * 1000)
                                                .build())
                                .build());
                mutations.add(
                        Mutation.newBuilder()
                                .setSetCell(
                                        SetCell.newBuilder()
                                                .setColumnQualifier(
                                                        ByteString.copyFromUtf8("lastName"))
                                                .setValue(
                                                        ByteString.copyFromUtf8(
                                                                inputElement.getAfterLastName()))
                                                .setFamilyName("p")
                                                .setTimestampMicros(timestamp * 1000)
                                                .build())
                                .build());
                mutations.add(
                        Mutation.newBuilder()
                                .setSetCell(
                                        SetCell.newBuilder()
                                                .setColumnQualifier(ByteString.copyFromUtf8("city"))
                                                .setValue(
                                                        ByteString.copyFromUtf8(
                                                                inputElement.getAfterCity()))
                                                .setFamilyName("p")
                                                .setTimestampMicros(timestamp * 1000)
                                                .build())
                                .build());
                outputReceiver.output(
                        KV.of(
                                ByteString.copyFromUtf8("" + inputElement.getAfterPersonId()),
                                ImmutableList.copyOf(mutations)));
            } else if (inputElement.getOpType().equals("d")) {
                final KV<ByteString, Iterable<Mutation>> kv =
                        KV.of(
                                ByteString.copyFromUtf8("" + inputElement.getAfterPersonId()),
                                ImmutableList.of(
                                        Mutation.newBuilder()
                                                .setDeleteFromFamily(
                                                        DeleteFromFamily.newBuilder()
                                                                .setFamilyName("p")
                                                                .build())
                                                .build()));
                outputReceiver.output(kv);
            }
        }
    }
}
