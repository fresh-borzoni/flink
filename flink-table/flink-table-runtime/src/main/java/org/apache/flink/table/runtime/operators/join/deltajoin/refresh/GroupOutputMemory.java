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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Knows which rows the group of a key emitted last, so that a refresh can emit the difference.
 *
 * <p>Rows are identified by an output key that is unique within a group. Rows are normalized to
 * {@link BinaryRowData} with {@link RowKind#INSERT}, so that equal rows compare equal.
 */
interface GroupOutputMemory {

    /**
     * Reads the rows the group of the given key emitted last. Called in the task thread with the
     * key as the current key. The callback may be called later, in the task thread.
     */
    void read(
            RowData key, Consumer<Map<RowData, BinaryRowData>> onRows, Consumer<Throwable> onError)
            throws Exception;

    /** The group of the given key emitted these rows. Called with the key as the current key. */
    void written(RowData key, Map<RowData, BinaryRowData> rows) throws Exception;

    /**
     * Reads the rows the group emitted last, like {@link #read}, unless there are more than {@code
     * limit} of them: then calls {@code onTooLarge} instead.
     */
    default void read(
            RowData key,
            int limit,
            Consumer<Map<RowData, BinaryRowData>> onRows,
            Runnable onTooLarge,
            Consumer<Throwable> onError)
            throws Exception {
        read(
                key,
                rows -> {
                    if (rows.size() > limit) {
                        onTooLarge.run();
                    } else {
                        onRows.accept(rows);
                    }
                },
                onError);
    }

    /**
     * Reads every row the group emitted last in pages of about {@code pageSize} rows, and tells
     * with each page whether it is the last. Rows the group emitted while the pages are read may or
     * may not be among them. Called in the task thread with the key as the current key; the
     * callbacks are called in the task thread, possibly later.
     */
    default void readPages(
            RowData key,
            int pageSize,
            BiConsumer<Map<RowData, BinaryRowData>, Boolean> onPage,
            Consumer<Throwable> onError)
            throws Exception {
        read(key, rows -> onPage.accept(rows, true), onError);
    }

    /** Forgets what is held of the group in memory; it is read again when needed. */
    default void forget(RowData key) {}

    /**
     * Whether the memory holds as many rows as it may hold before checkpoints release some: no
     * further group should be refreshed until then.
     */
    default boolean full() {
        return false;
    }

    /** The rows held until a checkpoint completes. */
    default long pinnedRows() {
        return 0;
    }

    /**
     * A checkpoint barrier passed the operator: every row emitted so far precedes it. If the
     * barrier is aligned, it overtakes no row on the way to the sink.
     */
    default void snapshotState(long checkpointId, boolean barrierAligned) {}

    /**
     * The checkpoint completed. If its barrier was aligned, the sink received every row emitted
     * before the barrier and made it visible.
     */
    default void notifyCheckpointComplete(long checkpointId) {}

    default void close() throws Exception {}

    /** Whether single rows of a group can be read and replaced, see {@link #readRows}. */
    default boolean supportsRows() {
        return false;
    }

    /**
     * Reads the rows with the given row keys that the group of the given key emitted last; row keys
     * without a row are left out. Called in the task thread with the key as the current key.
     */
    default void readRows(
            RowData key,
            Collection<RowData> rowKeys,
            Consumer<Map<RowData, BinaryRowData>> onRows,
            Consumer<Throwable> onError)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * The group of the given key replaced the rows with the given row keys by the given rows; its
     * other rows are unchanged. Called with the key as the current key.
     */
    default void writtenRows(
            RowData key, Collection<RowData> rowKeys, Map<RowData, BinaryRowData> rows)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Normalizes rows and indexes them by the output key. The rows of a group must have distinct
     * output keys, otherwise they would overwrite each other in the output.
     */
    static Map<RowData, BinaryRowData> index(
            Collection<RowData> rows, RowDataSerializer serializer, RowDataKeySelector outputKey)
            throws Exception {
        Map<RowData, BinaryRowData> indexed = new LinkedHashMap<>();
        for (RowData row : rows) {
            BinaryRowData normalized = serializer.toBinaryRow(row).copy();
            normalized.setRowKind(RowKind.INSERT);
            RowData key = outputKey.getKey(normalized);
            if (indexed.put(key, normalized) != null) {
                throw new IllegalStateException(
                        String.format(
                                "Two rows of one group have the same output key %s. A constant "
                                        + "that replaces NULL in a key column of the output must "
                                        + "not be a value of that column.",
                                GroupRows.format(key, outputKey.getProducedType().toRowType())));
            }
        }
        return indexed;
    }

    /**
     * Keeps the emitted rows in keyed state. Correct after a failure only if the consumers of the
     * operator are rolled back with it, as the state is.
     */
    final class KeyedStateMemory implements GroupOutputMemory {

        private final MapState<RowData, RowData> state;
        private final RowDataSerializer serializer;

        KeyedStateMemory(MapState<RowData, RowData> state, RowDataSerializer serializer) {
            this.state = state;
            this.serializer = serializer;
        }

        @Override
        public void read(
                RowData key,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Consumer<Throwable> onError)
                throws Exception {
            Map<RowData, BinaryRowData> rows = new HashMap<>();
            for (Map.Entry<RowData, RowData> entry : state.entries()) {
                rows.put(entry.getKey(), serializer.toBinaryRow(entry.getValue()).copy());
            }
            onRows.accept(rows);
        }

        @Override
        public void read(
                RowData key,
                int limit,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Runnable onTooLarge,
                Consumer<Throwable> onError)
                throws Exception {
            Map<RowData, BinaryRowData> rows = new HashMap<>();
            for (Map.Entry<RowData, RowData> entry : state.entries()) {
                if (rows.size() == limit) {
                    onTooLarge.run();
                    return;
                }
                rows.put(entry.getKey(), serializer.toBinaryRow(entry.getValue()).copy());
            }
            onRows.accept(rows);
        }

        @Override
        public void readPages(
                RowData key,
                int pageSize,
                BiConsumer<Map<RowData, BinaryRowData>, Boolean> onPage,
                Consumer<Throwable> onError)
                throws Exception {
            Iterator<Map.Entry<RowData, RowData>> it = state.iterator();
            Map<RowData, BinaryRowData> page = new HashMap<>();
            while (it.hasNext()) {
                Map.Entry<RowData, RowData> entry = it.next();
                page.put(entry.getKey(), serializer.toBinaryRow(entry.getValue()).copy());
                if (page.size() == pageSize && it.hasNext()) {
                    onPage.accept(page, false);
                    page = new HashMap<>();
                }
            }
            // the iteration is over: the last page may change the state
            onPage.accept(page, true);
        }

        @Override
        public boolean supportsRows() {
            return true;
        }

        @Override
        public void readRows(
                RowData key,
                Collection<RowData> rowKeys,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Consumer<Throwable> onError)
                throws Exception {
            Map<RowData, BinaryRowData> rows = new HashMap<>();
            for (RowData rowKey : rowKeys) {
                RowData row = state.get(rowKey);
                if (row != null) {
                    rows.put(rowKey, serializer.toBinaryRow(row).copy());
                }
            }
            onRows.accept(rows);
        }

        @Override
        public void writtenRows(
                RowData key, Collection<RowData> rowKeys, Map<RowData, BinaryRowData> rows)
                throws Exception {
            for (RowData rowKey : rowKeys) {
                BinaryRowData row = rows.get(rowKey);
                if (row == null) {
                    state.remove(rowKey);
                } else {
                    state.put(rowKey, row);
                }
            }
        }

        @Override
        public void written(RowData key, Map<RowData, BinaryRowData> rows) throws Exception {
            Set<RowData> unchanged = new HashSet<>();
            Iterator<Map.Entry<RowData, RowData>> it = state.iterator();
            while (it.hasNext()) {
                Map.Entry<RowData, RowData> entry = it.next();
                BinaryRowData row = rows.get(entry.getKey());
                if (row == null) {
                    it.remove();
                } else if (row.equals(serializer.toBinaryRow(entry.getValue()))) {
                    unchanged.add(entry.getKey());
                }
            }
            for (Map.Entry<RowData, BinaryRowData> entry : rows.entrySet()) {
                if (!unchanged.contains(entry.getKey())) {
                    state.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Keeps the emitted rows of recently written groups in memory and reads other groups back from
     * the output table, without Flink state. Without read-your-writes, a group stays pinned in
     * memory until an aligned checkpoint completes whose barrier passed after its last write, since
     * the sink makes the rows visible by then.
     *
     * <p>Both limits count rows, not groups. Groups that may be read back are kept up to {@code
     * maxCachedRows}, least recently used first out, and a group larger than that is not kept at
     * all. Pinned groups cannot be dropped: once they hold more than {@code maxPinnedRows}, the
     * memory is {@link #full()} and the operator starts no refresh until a checkpoint releases
     * them.
     *
     * <p>After a restore every group is read back. A write of the failed attempt that lands later
     * is corrected only when its group changes again.
     */
    final class PinnedCacheMemory implements GroupOutputMemory {

        private final GroupLookup outputLookup;
        private final @Nullable GroupLookup rowLookup;
        private final RowDataSerializer serializer;
        private final RowDataKeySelector outputKey;
        private final long maxCachedRows;
        private final long maxPinnedRows;
        private final boolean readYourWrites;

        /** Access order, eldest first. */
        private final LinkedHashMap<RowData, Entry> cache = new LinkedHashMap<>(16, 0.75f, true);

        /** Barriers whose checkpoints did not complete yet. */
        private final TreeSet<Long> alignedBarriers = new TreeSet<>();

        private long lastBarrier;
        private long pinnedRows;
        private long unpinnedRows;
        private long readBacks;

        PinnedCacheMemory(
                GroupLookup outputLookup,
                RowDataSerializer serializer,
                RowDataKeySelector outputKey,
                long maxCachedRows,
                long maxPinnedRows,
                long restoredCheckpointId,
                boolean readYourWrites,
                @Nullable GroupLookup rowLookup) {
            this.outputLookup = outputLookup;
            this.rowLookup = rowLookup;
            this.serializer = serializer;
            this.outputKey = outputKey;
            this.maxCachedRows = maxCachedRows;
            this.maxPinnedRows = maxPinnedRows;
            this.lastBarrier = restoredCheckpointId;
            this.readYourWrites = readYourWrites;
        }

        @Override
        public void read(
                RowData key,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Consumer<Throwable> onError)
                throws Exception {
            Entry entry = cache.get(key);
            if (entry != null) {
                onRows.accept(new HashMap<>(entry.rows));
                return;
            }
            readBacks++;
            outputLookup.read(
                    key,
                    rows -> {
                        try {
                            onRows.accept(index(rows, serializer, outputKey));
                        } catch (Throwable t) {
                            onError.accept(t);
                        }
                    },
                    onError);
        }

        @Override
        public void read(
                RowData key,
                int limit,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Runnable onTooLarge,
                Consumer<Throwable> onError)
                throws Exception {
            Entry entry = cache.get(key);
            if (entry != null || !outputLookup.supportsPages()) {
                GroupOutputMemory.super.read(key, limit, onRows, onTooLarge, onError);
                return;
            }
            readBacks++;
            GroupLookup.Pages pages = outputLookup.openPages(key, limit);
            pages.next(
                    (rows, last) -> {
                        if (!last) {
                            pages.close();
                            onTooLarge.run();
                            return;
                        }
                        try {
                            onRows.accept(index(rows, serializer, outputKey));
                        } catch (Throwable t) {
                            onError.accept(t);
                        }
                    },
                    onError);
        }

        @Override
        public void readPages(
                RowData key,
                int pageSize,
                BiConsumer<Map<RowData, BinaryRowData>, Boolean> onPage,
                Consumer<Throwable> onError)
                throws Exception {
            if (cache.containsKey(key) || !outputLookup.supportsPages()) {
                GroupOutputMemory.super.readPages(key, pageSize, onPage, onError);
                return;
            }
            readBacks++;
            readPage(outputLookup.openPages(key, pageSize), onPage, onError);
        }

        private void readPage(
                GroupLookup.Pages pages,
                BiConsumer<Map<RowData, BinaryRowData>, Boolean> onPage,
                Consumer<Throwable> onError) {
            pages.next(
                    (rows, last) -> {
                        Map<RowData, BinaryRowData> page;
                        try {
                            page = index(rows, serializer, outputKey);
                        } catch (Throwable t) {
                            pages.close();
                            onError.accept(t);
                            return;
                        }
                        onPage.accept(page, last);
                        if (!last) {
                            readPage(pages, onPage, onError);
                        }
                    },
                    onError);
        }

        @Override
        public void forget(RowData key) {
            release(cache.remove(key));
        }

        @Override
        public boolean full() {
            return pinnedRows > maxPinnedRows;
        }

        @Override
        public long pinnedRows() {
            return pinnedRows;
        }

        /**
         * Single rows are read back by the primary key of the output table, which needs every
         * written row to be readable at once.
         */
        @Override
        public boolean supportsRows() {
            return readYourWrites && rowLookup != null;
        }

        @Override
        public void readRows(
                RowData key,
                Collection<RowData> rowKeys,
                Consumer<Map<RowData, BinaryRowData>> onRows,
                Consumer<Throwable> onError)
                throws Exception {
            Entry entry = cache.get(key);
            Map<RowData, BinaryRowData> found = new HashMap<>();
            if (entry != null || rowKeys.isEmpty()) {
                for (RowData rowKey : rowKeys) {
                    BinaryRowData row = entry == null ? null : entry.rows.get(rowKey);
                    if (row != null) {
                        found.put(rowKey, row);
                    }
                }
                onRows.accept(found);
                return;
            }
            int[] missing = {rowKeys.size()};
            boolean[] failed = {false};
            for (RowData rowKey : rowKeys) {
                readBacks++;
                rowLookup.read(
                        rowKey,
                        rows -> {
                            if (failed[0]) {
                                return;
                            }
                            try {
                                found.putAll(index(rows, serializer, outputKey));
                            } catch (Throwable t) {
                                failed[0] = true;
                                onError.accept(t);
                                return;
                            }
                            if (--missing[0] == 0) {
                                onRows.accept(found);
                            }
                        },
                        error -> {
                            if (!failed[0]) {
                                failed[0] = true;
                                onError.accept(error);
                            }
                        });
            }
        }

        @Override
        public void writtenRows(
                RowData key, Collection<RowData> rowKeys, Map<RowData, BinaryRowData> rows) {
            Entry entry = cache.get(key);
            if (entry == null) {
                // only whole groups are remembered
                return;
            }
            Map<RowData, BinaryRowData> merged = new HashMap<>(entry.rows);
            for (RowData rowKey : rowKeys) {
                BinaryRowData row = rows.get(rowKey);
                if (row == null) {
                    merged.remove(rowKey);
                } else {
                    merged.put(rowKey, row);
                }
            }
            written(key, merged);
        }

        @Override
        public void written(RowData key, Map<RowData, BinaryRowData> rows) {
            release(cache.remove(key));
            if (readYourWrites && rows.size() > maxCachedRows) {
                // too large to remember; it is read back, row by row or whole, when needed
                return;
            }
            Entry entry = new Entry(rows, readYourWrites ? Long.MIN_VALUE : lastBarrier);
            cache.put(key, entry);
            if (entry.pinned()) {
                pinnedRows += entry.rows.size();
            } else {
                unpinnedRows += entry.rows.size();
                evictUnpinned();
            }
        }

        private void release(@Nullable Entry entry) {
            if (entry == null) {
                return;
            }
            if (entry.pinned()) {
                pinnedRows -= entry.rows.size();
            } else {
                unpinnedRows -= entry.rows.size();
            }
        }

        @Override
        public void snapshotState(long checkpointId, boolean barrierAligned) {
            if (readYourWrites) {
                return;
            }
            Preconditions.checkState(
                    barrierAligned,
                    "Checkpoint %s is unaligned, so its completion does not prove that the sink "
                            + "received the rows emitted before it. A delta join that reads its "
                            + "output back after checkpoints needs aligned checkpoints.",
                    checkpointId);
            lastBarrier = checkpointId;
            alignedBarriers.add(checkpointId);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            boolean known = alignedBarriers.contains(checkpointId);
            alignedBarriers.headSet(checkpointId, true).clear();
            if (!known) {
                return;
            }
            Iterator<Entry> it = cache.values().iterator();
            while (it.hasNext()) {
                Entry entry = it.next();
                if (entry.pinned() && entry.lastBarrierBeforeWrite < checkpointId) {
                    pinnedRows -= entry.rows.size();
                    if (entry.rows.size() > maxCachedRows) {
                        // it can be read back now, and is too large to keep
                        it.remove();
                    } else {
                        entry.lastBarrierBeforeWrite = Long.MIN_VALUE;
                        unpinnedRows += entry.rows.size();
                    }
                }
            }
            evictUnpinned();
        }

        /** Drops the least recently used unpinned groups beyond the limit. */
        private void evictUnpinned() {
            Iterator<Entry> it = cache.values().iterator();
            while (unpinnedRows > maxCachedRows && it.hasNext()) {
                Entry entry = it.next();
                if (!entry.pinned()) {
                    it.remove();
                    unpinnedRows -= entry.rows.size();
                }
            }
        }

        @Override
        public void close() throws Exception {
            outputLookup.close();
            if (rowLookup != null) {
                rowLookup.close();
            }
        }

        int size() {
            return cache.size();
        }

        long readBacks() {
            return readBacks;
        }

        private static final class Entry {
            private final Map<RowData, BinaryRowData> rows;

            /** The last barrier before the write; {@link Long#MIN_VALUE} once unpinned. */
            private long lastBarrierBeforeWrite;

            private Entry(Map<RowData, BinaryRowData> rows, long lastBarrierBeforeWrite) {
                this.rows = rows;
                this.lastBarrierBeforeWrite = lastBarrierBeforeWrite;
            }

            private boolean pinned() {
                return lastBarrierBeforeWrite != Long.MIN_VALUE;
            }
        }
    }
}
