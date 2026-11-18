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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;

/**
 * Enables a {@link DynamicTableSink} to make the rows it receives visible to lookups of the same
 * table that the same task issues afterwards through its {@link LookupTableSource}, including the
 * pages of {@link SupportsPagedLookup}.
 *
 * <p>A sink can keep this promise by flushing before such a lookup, or by adding the rows it wrote
 * that are not acknowledged yet to the rows the lookup reads. It covers only the writer of the same
 * task, so the sink must not redistribute its input.
 */
@PublicEvolving
public interface SupportsReadYourWrites {

    /**
     * Makes the rows this sink receives visible to the lookups of the same task issued afterwards.
     *
     * @return whether the sink can keep that promise, for example not if its table changes rows on
     *     write, by merging them with the rows it holds
     */
    boolean applyReadYourWrites();
}
