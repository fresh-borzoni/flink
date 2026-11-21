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

package org.apache.flink.table.connector.source.lookup;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.abilities.SupportsEpochs;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A lookup function that can also read the rows of a key as of an epoch of the scan of the same
 * table, see {@link SupportsEpochs}. An {@link AsyncLookupFunction} implements it in addition to
 * its lookups of the latest rows.
 *
 * <p>The rows as of epoch {@code e} are the rows the scan started from, with exactly the changes
 * the scan emitted in epochs up to {@code e} applied: none of a later epoch. The scan starts from
 * an empty table if it emits a snapshot as inserts; epoch {@link SupportsEpochs#BEFORE_FIRST} reads
 * the rows it started from. The result of a read never changes, so callers may share and cache it.
 *
 * <p>A read of an epoch the scan has not completed yet waits until it has. A read of an epoch whose
 * history the table no longer keeps fails.
 */
@PublicEvolving
public interface SupportsVersionedLookup {

    /**
     * Reads the rows that match the key row as of the given epoch. The key row holds the lookup
     * keys in the order of {@link LookupTableSource.LookupContext#getKeys()}, like the key row of
     * {@link AsyncLookupFunction#asyncLookup}.
     */
    CompletableFuture<Collection<RowData>> asyncLookupAsOf(RowData keyRow, long epoch);
}
