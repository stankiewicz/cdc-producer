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

package com.google.dataflow.feature;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

public class Debug<T> extends DoFn<T, T> {
    String name;

    public Debug(String name) {
        this.name = name;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element T in, @Timestamp Instant ts) {
        System.out.println(name + ":" + ts.toString() + ": " + in + ", " + c.pane());
        c.output(in);
    }
}
