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

package org.apache.flink.table.runtime.operators.join.deltajoin.refresh;

/** How a group refresh knows which rows a group emitted before, to emit only the difference. */
public enum GroupRefreshOutputMode {

    /**
     * Every group has at most one row. The operator emits an upsert of that row, or a delete that
     * only carries the group key when the group is empty. It only remembers whether it emitted a
     * row for a group; changes may be repeated.
     */
    UPSERT,

    /**
     * The operator keeps the rows it emitted for every group in keyed state and emits the
     * difference. Consumers must be rolled back with the operator after a failure, as the state is.
     */
    STATE,

    /**
     * The operator writes the rows of an output table and reads them back from it instead of
     * keeping them in state: it remembers recently written groups in memory and reads other groups
     * back by their group key.
     */
    READ_BACK
}
