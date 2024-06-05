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
import com.google.common.annotations.VisibleForTesting;
import com.google.dataflow.ingestion.bigtable.BeamRowToBigtableMutation;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class PersistCDCTransform<C, B> extends PTransform<PCollection<Row>, PDone> {

    final Class<C> cdcClass;
    final Class<B> dbClass;

    private final String projectId;
    private final String instanceId;
    private final String tableId;

    private final String appProfileId;
    private final SerializableFunction<C, B> cdcToDBConverter;

    private Map<String, Set<String>> mapping;

    int port;

    boolean test = false;

    @VisibleForTesting
    PersistCDCTransform(
            int port,
            String projectId,
            String instanceId,
            String tableId,
            Class cdcClass,
            Class dbClass,
            SerializableFunction<C, B> cdcToDBConverter,
            Map<String, Set<String>> mapping) {
        this.mapping = mapping;
        this.cdcToDBConverter = cdcToDBConverter;
        this.cdcClass = cdcClass;
        this.dbClass = dbClass;
        test = true;
        this.tableId = tableId;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.port = port;
        this.appProfileId = null;
    }

    public PersistCDCTransform(
            String projectId,
            String instanceId,
            String tableId,
            String appProfileId,
            Class cdcClass,
            Class dbClass,
            SerializableFunction<C, B> cdcToDBConverter,
            Map<String, Set<String>> mapping) {
        this.mapping = mapping;
        this.cdcToDBConverter = cdcToDBConverter;
        this.cdcClass = cdcClass;
        this.dbClass = dbClass;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.appProfileId = appProfileId;
    }

    @Override
    public PDone expand(PCollection<Row> input) {

        final PCollection<B> dbDTO =
                input.apply("Convert to CDC DTO", Convert.fromRows(cdcClass))
                        .apply(
                                "Convert to DB DTO",
                                MapElements.into(TypeDescriptor.of(dbClass))
                                        .via(x -> cdcToDBConverter.apply(x)));

        PCollection<KV<ByteString, Iterable<Mutation>>> mutations =
                dbDTO.apply("Convert back to Row", Convert.toRows())
                        .apply("Convert to Mutation", new BeamRowToBigtableMutation(mapping));
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
}
