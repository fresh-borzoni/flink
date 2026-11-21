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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Everything a {@link GroupRefreshOperatorBase} needs to know beyond how it reads its groups: how
 * the lines of a group become emitted rows, how the rows are told apart, and how the operator knows
 * what it emitted before.
 *
 * <p>A line is one row per table of the join, null for tables padded with NULLs. It becomes a
 * joined row through a field map, and the joined row becomes zero or one emitted rows through an
 * optional projection.
 */
public final class GroupRefreshSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    final RowType[] tableTypes;
    final int[][] tableKeys;
    final int[] joinedTables;
    final int[] joinedFields;
    final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputCalc;
    final RowType outputType;

    /** The type of the group key, for messages; null if unknown. */
    final @Nullable RowType groupKeyType;

    final GroupRefreshOutputMode outputMode;
    final @Nullable RowDataKeySelector outputKey;
    final @Nullable GroupLookup outputLookup;
    final @Nullable GroupLookup outputRowLookup;
    final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputKeyCalc;
    final boolean readYourWrites;
    final @Nullable int[] outputGroupKey;
    final int[][] declaredKeys;
    final int capacity;
    final int maxWaitingKeys;
    final long maxCachedRows;
    final long maxPinnedRows;
    final long lookupTimeoutMs;
    final boolean emitUpdateBefore;
    final int maxGroupSize;
    final int pageSize;

    private GroupRefreshSpec(Builder builder) {
        Preconditions.checkArgument(
                builder.tableTypes.length == builder.tableKeys.length,
                "Every table needs a unique key, empty if it has none.");
        Preconditions.checkArgument(
                builder.joinedTables.length == builder.joinedFields.length,
                "Every joined field needs its table.");
        Preconditions.checkArgument(
                builder.outputCalc != null
                        || builder.outputType.getFieldCount() == builder.joinedFields.length,
                "Without a projection, the joined rows are emitted.");
        Preconditions.checkArgument(
                (builder.outputMode == GroupRefreshOutputMode.READ_BACK)
                        == (builder.outputLookup != null),
                "Reading back, and only reading back, needs a lookup of the output.");
        Preconditions.checkArgument(
                builder.outputLookup == null || builder.outputKey != null,
                "Reading back needs the key of the output.");
        Preconditions.checkArgument(
                (builder.outputMode == GroupRefreshOutputMode.UPSERT)
                        == (builder.outputGroupKey != null),
                "Upserts, and only upserts, need the output fields of the group key.");
        Preconditions.checkArgument(builder.capacity > 0, "The capacity must be positive.");
        Preconditions.checkArgument(
                builder.maxGroupSize > 0, "The largest group size must be positive.");
        Preconditions.checkArgument(builder.pageSize >= 0, "The page size must not be negative.");
        this.tableTypes = builder.tableTypes;
        this.tableKeys = builder.tableKeys;
        this.joinedTables = builder.joinedTables;
        this.joinedFields = builder.joinedFields;
        this.outputCalc = builder.outputCalc;
        this.outputType = builder.outputType;
        this.groupKeyType = builder.groupKeyType;
        this.outputMode = builder.outputMode;
        this.outputKey = builder.outputKey;
        this.outputLookup = builder.outputLookup;
        this.outputRowLookup = builder.outputRowLookup;
        this.outputKeyCalc = builder.outputKeyCalc;
        this.readYourWrites = builder.readYourWrites;
        this.outputGroupKey = builder.outputGroupKey;
        this.declaredKeys = builder.declaredKeys;
        this.capacity = builder.capacity;
        this.maxWaitingKeys = builder.maxWaitingKeys;
        this.maxCachedRows = builder.maxCachedRows;
        this.maxPinnedRows = builder.maxPinnedRows;
        this.lookupTimeoutMs = builder.lookupTimeoutMs;
        this.emitUpdateBefore = builder.emitUpdateBefore;
        this.maxGroupSize = builder.maxGroupSize;
        this.pageSize = builder.pageSize;
    }

    /**
     * @param tableTypes the rows of every table of the join
     * @param tableKeys a unique key of every table, empty if a table has none
     * @param joinedTables the table of every field of the joined row
     * @param joinedFields the field of its table of every field of the joined row
     * @param outputType the emitted rows: the joined rows, or the rows of the projection
     * @param outputMode how the rows a group emitted before are known
     */
    public static Builder builder(
            RowType[] tableTypes,
            int[][] tableKeys,
            int[] joinedTables,
            int[] joinedFields,
            RowType outputType,
            GroupRefreshOutputMode outputMode) {
        return new Builder(
                tableTypes, tableKeys, joinedTables, joinedFields, outputType, outputMode);
    }

    public GroupRefreshOutputMode getOutputMode() {
        return outputMode;
    }

    public RowType getTableType(int table) {
        return tableTypes[table];
    }

    /** A unique key of the table, empty if it has none. */
    public int[] getTableKey(int table) {
        return tableKeys[table];
    }

    public int getCapacity() {
        return capacity;
    }

    public long getLookupTimeoutMs() {
        return lookupTimeoutMs;
    }

    /** Builds a {@link GroupRefreshSpec}. */
    public static final class Builder {
        private final RowType[] tableTypes;
        private final int[][] tableKeys;
        private final int[] joinedTables;
        private final int[] joinedFields;
        private final RowType outputType;
        private @Nullable RowType groupKeyType;
        private final GroupRefreshOutputMode outputMode;
        private @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputCalc;
        private @Nullable RowDataKeySelector outputKey;
        private @Nullable GroupLookup outputLookup;
        private @Nullable GroupLookup outputRowLookup;
        private @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputKeyCalc;
        private boolean readYourWrites;
        private @Nullable int[] outputGroupKey;
        private int[][] declaredKeys = new int[0][];
        private int capacity = 100;
        private int maxWaitingKeys = 1000;
        private long maxCachedRows = 100_000;
        private long maxPinnedRows = 200_000;
        private long lookupTimeoutMs;
        private boolean emitUpdateBefore = true;
        private int maxGroupSize = Integer.MAX_VALUE;
        private int pageSize;

        private Builder(
                RowType[] tableTypes,
                int[][] tableKeys,
                int[] joinedTables,
                int[] joinedFields,
                RowType outputType,
                GroupRefreshOutputMode outputMode) {
            this.tableTypes = tableTypes;
            this.tableKeys = tableKeys;
            this.joinedTables = joinedTables;
            this.joinedFields = joinedFields;
            this.outputType = outputType;
            this.outputMode = outputMode;
        }

        /** A filter and projection from the joined rows to the emitted rows. */
        public Builder projection(
                @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputCalc) {
            this.outputCalc = outputCalc;
            return this;
        }

        /**
         * For {@link GroupRefreshOutputMode#READ_BACK}: the primary key of the output table on the
         * emitted rows, and a lookup of the rows of a group key in the output table.
         */
        /**
         * The primary key of the output table on the emitted rows, by which a group keeps its rows
         * apart, without reading them back; for {@link GroupRefreshOutputMode#VERSIONED}.
         */
        public Builder outputKey(RowDataKeySelector outputKey) {
            this.outputKey = outputKey;
            return this;
        }

        public Builder readBack(RowDataKeySelector outputKey, GroupLookup outputLookup) {
            this.outputKey = outputKey;
            this.outputLookup = outputLookup;
            return this;
        }

        /**
         * For {@link GroupRefreshOutputMode#READ_BACK}: whether a lookup of the output table sees
         * every row emitted before it, so that groups need not stay in memory until a checkpoint
         * completes.
         */
        public Builder readYourWrites(boolean readYourWrites) {
            this.readYourWrites = readYourWrites;
            return this;
        }

        /**
         * For {@link GroupRefreshOutputMode#READ_BACK} with read-your-writes: a lookup of the
         * output table by its primary key, so that single rows of a group can be read back, and
         * optionally the primary key computed from a joined row. That key must depend only on the
         * unique keys and join keys of the tables: it is computed from rows that have nothing else.
         * Without it, row keys are only known for complete lines.
         */
        public Builder readBackRows(
                GroupLookup outputRowLookup,
                @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> outputKeyCalc) {
            this.outputRowLookup = outputRowLookup;
            this.outputKeyCalc = outputKeyCalc;
            return this;
        }

        /**
         * For {@link GroupRefreshOutputMode#UPSERT}: the emitted field of every group key field; a
         * delete of an empty group only sets these fields.
         */
        public Builder groupKeyType(RowType groupKeyType) {
            this.groupKeyType = groupKeyType;
            return this;
        }

        public Builder upsertByGroupKey(int[] outputGroupKey) {
            this.outputGroupKey = outputGroupKey;
            return this;
        }

        /** Keys of the emitted rows that consumers rely on; every group is kept unique on them. */
        public Builder declaredKeys(int[][] declaredKeys) {
            this.declaredKeys = declaredKeys;
            return this;
        }

        /**
         * The number of refreshes that run at the same time, and the number of keys that may wait
         * for one before the input blocks.
         */
        public Builder capacity(int capacity, int maxWaitingKeys) {
            this.capacity = capacity;
            this.maxWaitingKeys = maxWaitingKeys;
            return this;
        }

        /** The number of emitted rows kept in memory once they could be read back. */
        public Builder maxCachedRows(long maxCachedRows) {
            this.maxCachedRows = maxCachedRows;
            return this;
        }

        /**
         * The number of emitted rows that may wait in memory for a checkpoint before the operator
         * stops starting refreshes until a checkpoint completes. Read-your-writes keeps none.
         */
        public Builder maxPinnedRows(long maxPinnedRows) {
            this.maxPinnedRows = maxPinnedRows;
            return this;
        }

        /** How long a lookup may take before the job fails; 0 for no limit. */
        public Builder lookupTimeoutMs(long lookupTimeoutMs) {
            this.lookupTimeoutMs = lookupTimeoutMs;
            return this;
        }

        /**
         * Whether an update emits the old row before the new one, or only the new one for consumers
         * that upsert by key.
         */
        public Builder emitUpdateBefore(boolean emitUpdateBefore) {
            this.emitUpdateBefore = emitUpdateBefore;
            return this;
        }

        /**
         * Groups with more rows than this are read in pages where the lookups support it, and
         * refreshed holding only the row keys of the group; 0 reads every group at once.
         */
        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * The largest number of joined rows, and of rows emitted before, of one group read at once.
         */
        public Builder maxGroupSize(int maxGroupSize) {
            this.maxGroupSize = maxGroupSize;
            return this;
        }

        public GroupRefreshSpec build() {
            return new GroupRefreshSpec(this);
        }
    }
}
