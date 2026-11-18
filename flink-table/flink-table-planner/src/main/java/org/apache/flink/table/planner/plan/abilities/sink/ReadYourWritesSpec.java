/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.abilities.sink;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsReadYourWrites;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A sub-class of {@link SinkAbilitySpec} that makes the rows a sink receives visible to the lookups
 * of its task, see {@link SupportsReadYourWrites}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("ReadYourWrites")
public final class ReadYourWritesSpec implements SinkAbilitySpec {

    @JsonCreator
    public ReadYourWritesSpec() {}

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (!(tableSink instanceof SupportsReadYourWrites)
                || !((SupportsReadYourWrites) tableSink).applyReadYourWrites()) {
            throw new TableException(
                    String.format(
                            "%s can no longer make its rows visible to the lookups of its task.",
                            tableSink.getClass().getName()));
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ReadYourWritesSpec;
    }

    @Override
    public int hashCode() {
        return ReadYourWritesSpec.class.hashCode();
    }
}
