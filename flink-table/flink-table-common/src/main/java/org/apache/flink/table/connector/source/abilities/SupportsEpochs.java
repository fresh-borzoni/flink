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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.lookup.SupportsVersionedLookup;

/**
 * Enables a {@link ScanTableSource} to cut its changes into epochs, so that lookups of the same
 * table can read it as of an epoch, see {@link SupportsVersionedLookup}.
 *
 * <p>An epoch is a number. Every source of the job that applied the same epoch domain counts the
 * same epochs, and each one ends an epoch at its own position in each part of its table, e.g. at an
 * offset per bucket. A source that applied epochs:
 *
 * <ul>
 *   <li>emits every change with its epoch as the timestamp of its record;
 *   <li>emits a watermark {@code e} once it has emitted every change of the epochs up to {@code e},
 *       so a watermark means that the epochs up to it are complete;
 *   <li>emits a watermark {@code e - 1} before the first change of epoch {@code e}: {@link
 *       #BEFORE_FIRST} before its first change, and after a restore the last epoch it ended before
 *       its first change, so that a consumer that receives its changes in order knows the epoch of
 *       every change also where an operator dropped the timestamps: one after the last watermark;
 *   <li>after a restore, ends the epochs it had ended before at the same positions, so that a
 *       replay reads the same changes in each epoch.
 * </ul>
 *
 * <p>A {@link org.apache.flink.table.connector.source.LookupTableSource} that applied a domain
 * returns lookup functions that implement {@link SupportsVersionedLookup}, which read as of the
 * epochs of the scans of the same table that applied the same domain.
 */
@PublicEvolving
public interface SupportsEpochs {

    /** The epoch before the first one: the rows a scan started from. */
    long BEFORE_FIRST = -1L;

    /**
     * An epoch after every epoch a scan ends: a read as of it sees every change the scan emitted. A
     * watermark of {@link Long#MAX_VALUE}, the end of the input, completes it.
     */
    long AFTER_LAST = Long.MAX_VALUE - 3;

    /**
     * A scan cuts its changes into the epochs of the given domain; a lookup source reads as of
     * them. Returns false if the source cannot.
     *
     * @param domain identifies the epochs of one job; sources and lookups that apply the same
     *     domain count the same epochs
     */
    boolean applyEpochs(String domain);
}
