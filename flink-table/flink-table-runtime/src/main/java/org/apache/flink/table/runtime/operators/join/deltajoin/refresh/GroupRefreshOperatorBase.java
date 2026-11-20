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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A delta join that refreshes groups instead of joining each change: a change marks the group it
 * may affect, and a refresh reads the current rows of the group from the inputs and emits the
 * difference to what the group emitted before, see {@link GroupRefreshOutputMode}. Subclasses
 * decide how a change finds its group and how the lines of a group are read.
 *
 * <p>Refreshes of one group never overlap, and a change during a refresh leads to one more, see
 * {@link RefreshScheduler}. All work happens in the task thread.
 */
public abstract class GroupRefreshOperatorBase extends AbstractStreamOperator<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GroupRefreshOperatorBase.class);

    /** A group with more changed rows than this since its last refresh is refreshed whole. */
    protected static final int MAX_SCOPE_ROWS = 1000;

    static final String PENDING_STATE_NAME = "group-refresh-pending";
    static final String OUTPUT_STATE_NAME = "group-refresh-output";
    static final String EMITTED_STATE_NAME = "group-refresh-emitted";

    /**
     * How often in a row a refresh of a group may be inconsistent. A read while rows move can be
     * inconsistent once, and the move triggers another refresh; one that stays inconsistent is a
     * conflict in the output key.
     */
    static final int MAX_INCONSISTENT_REFRESHES = 100;

    protected final GroupRefreshSpec spec;
    protected final transient MailboxExecutor mailboxExecutor;

    private transient RefreshScheduler<RowData, Watermark> scheduler;
    private transient GroupRows rows;
    private transient @Nullable GroupOutputMemory memory;
    private transient StreamRecord<RowData> outRecord;

    private transient ValueState<Boolean> pendingState;
    private transient @Nullable MapState<RowData, RowData> outputState;

    /** {@link GroupRefreshOutputMode#UPSERT}: whether a row is emitted for the group. */
    private transient @Nullable ValueState<Boolean> emittedState;

    /** Keys whose pending flag is in state; cleared when their work is done. */
    private transient Set<RowData> persistedPending;

    /**
     * Whether keys may wait in state only: their pending flag is set, but the scheduler does not
     * know them. They are restored keys, or keys that changed while no refresh could start and the
     * scheduler held as many waiting keys as it may.
     */
    private transient boolean deferred;

    private transient boolean pullScheduled;

    /** The input ended: every group is refreshed, whatever the memory holds. */
    private transient boolean ending;

    private transient Map<RowData, Integer> inconsistentRefreshes;

    /** Keys whose next refresh must read the whole group: restored, or read inconsistently. */
    private transient Set<RowData> wholeNext;

    // whether this run started from a checkpoint: rows emitted after that checkpoint by the failed
    // run are not in the restored state, but a consumer that is not rolled back may still hold them
    private transient boolean restored;
    private transient long restoredCheckpointId;
    private transient boolean barrierAligned;

    private transient RowData.FieldGetter[] groupKeyGetters;

    // metrics, also used by tests
    private transient long refreshes;
    private transient long wholeRefreshes;
    private transient long scopedRefreshes;

    /** The most lines a refresh of a whole group read. */
    private transient long refreshedRows;

    private transient long inconsistentGroups;
    private transient long rowsEmitted;
    private transient long pagedRefreshes;
    private transient long deferredKeys;
    private transient long startsWhileFull;

    protected GroupRefreshOperatorBase(
            StreamOperatorParameters<RowData> parameters, GroupRefreshSpec spec) {
        super(parameters);
        this.spec = spec;
        this.mailboxExecutor = parameters.getMailboxExecutor();
    }

    @VisibleForTesting
    protected GroupRefreshOperatorBase(GroupRefreshSpec spec, MailboxExecutor mailboxExecutor) {
        this.spec = spec;
        this.mailboxExecutor = mailboxExecutor;
    }

    // ------------------------------------------------------------------------------------------
    // what subclasses decide
    // ------------------------------------------------------------------------------------------

    /** Opens what reads the groups. Called before refreshes start. */
    protected abstract void openGroupReader() throws Exception;

    /**
     * Reads the current lines of the group of the given key: one row per table, null for padded
     * tables. A group with more rows than {@link #pageSize} may be handed over in pages. Called in
     * the task thread with the key as the current key; the read must be called back in the task
     * thread, possibly later.
     */
    protected abstract void readGroup(RowData key, GroupRead read) throws Exception;

    /** Receives the lines of a group. Exactly one of the methods is called, once. */
    protected interface GroupRead {

        /** All lines of the group. */
        void lines(List<RowData[]> lines);

        /** The lines of a large group, in pages. */
        void pages(LinePages pages);

        void failed(Throwable error);
    }

    /** The rows of one table for one key, in pages. */
    public interface RowPages {

        /**
         * Reads the next rows. Exactly one of the callbacks is called, in the task thread: {@code
         * onRows} with the rows and whether they are the last, which closes the pages. Not called
         * after the last rows.
         */
        void next(BiConsumer<List<RowData>, Boolean> onRows, Consumer<Throwable> onError);

        /** Releases the pages; they cannot be read any more. */
        void close();
    }

    /**
     * Reads the remaining pages of one table into {@code rows}, which are then held at once, so
     * fails if they are more than the maximum group size.
     */
    protected void drain(
            RowData key,
            RowPages pages,
            List<RowData> rows,
            Consumer<List<RowData>> onRows,
            Consumer<Throwable> onError) {
        pages.next(
                (page, last) -> {
                    rows.addAll(page);
                    if (rows.size() > spec.maxGroupSize) {
                        pages.close();
                        onError.accept(groupTooLarge(key, rows.size(), "rows of one table"));
                    } else if (last) {
                        onRows.accept(rows);
                    } else {
                        drain(key, pages, rows, onRows, onError);
                    }
                },
                onError);
    }

    private TableException groupTooLarge(RowData key, int size, String what) {
        return new TableException(
                String.format(
                        "The group of key %s has %d %s, more than the %d that '%s' allows. Raise "
                                + "the limit if the memory of the task allows it.",
                        describe(key),
                        size,
                        what,
                        spec.maxGroupSize,
                        ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_MAX_GROUP_SIZE
                                .key()));
    }

    /** The lines of a group in pages. */
    protected interface LinePages {

        /**
         * Reads the next lines. Exactly one of the callbacks is called, in the task thread: {@code
         * onLines} with the lines and whether they are the last. Not called after the last lines.
         */
        void next(BiConsumer<List<RowData[]>, Boolean> onLines, Consumer<Throwable> onError);
    }

    /**
     * The number of rows above which subclasses should hand over the lines of a group in pages, or
     * 0 if groups are read at once. Pages need the rows a group emitted to be read by row key.
     */
    protected int pageSize() {
        return spec.pageSize > 0 && rowsCanBeReadByKey() ? spec.pageSize : 0;
    }

    /** The number of lookups issued to read groups. */
    protected abstract long numLookups();

    // ------------------------------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------------------------------

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        restored = context.isRestored();
        restoredCheckpointId = context.getRestoredCheckpointId().orElse(0L);
        pendingState =
                context.getKeyedStateStore()
                        .getState(
                                new ValueStateDescriptor<>(
                                        PENDING_STATE_NAME, BooleanSerializer.INSTANCE));
        if (spec.outputMode == GroupRefreshOutputMode.STATE) {
            outputState =
                    context.getKeyedStateStore()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            OUTPUT_STATE_NAME,
                                            InternalTypeInfo.of(GroupRows.rowKeyType(spec)),
                                            InternalTypeInfo.of(spec.outputType)));
        }
        if (spec.outputMode == GroupRefreshOutputMode.UPSERT) {
            emittedState =
                    context.getKeyedStateStore()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            EMITTED_STATE_NAME, BooleanSerializer.INSTANCE));
        }
        // restored keys stay in state and are pulled in batches, see pullDeferred
        deferred = restored;
    }

    @Override
    public void open() throws Exception {
        super.open();
        outRecord = new StreamRecord<>(null);
        persistedPending = new HashSet<>();
        inconsistentRefreshes = new HashMap<>();
        wholeNext = new HashSet<>();
        rows = new GroupRows(spec, getRuntimeContext());
        RowDataSerializer outputSerializer = rows.getOutputSerializer();
        switch (spec.outputMode) {
            case STATE:
                memory = new GroupOutputMemory.KeyedStateMemory(outputState, outputSerializer);
                break;
            case READ_BACK:
                spec.outputLookup.open(
                        getRuntimeContext(),
                        mailboxExecutor,
                        getProcessingTimeService(),
                        spec.lookupTimeoutMs);
                Preconditions.checkState(
                        spec.readYourWrites
                                || CheckpointingOptions.isCheckpointingEnabled(
                                        getContainingTask().getJobConfiguration()),
                        "A delta join that reads its output back after checkpoints needs "
                                + "checkpoints, or its memory grows without bound.");
                memory =
                        new GroupOutputMemory.PinnedCacheMemory(
                                spec.outputLookup,
                                outputSerializer,
                                spec.outputKey.copy(),
                                spec.maxCachedRows,
                                spec.maxPinnedRows,
                                restoredCheckpointId,
                                spec.readYourWrites,
                                spec.outputRowLookup);
                if (spec.outputRowLookup != null) {
                    spec.outputRowLookup.open(
                            getRuntimeContext(),
                            mailboxExecutor,
                            getProcessingTimeService(),
                            spec.lookupTimeoutMs);
                }
                break;
            default:
                memory = null;
                groupKeyGetters = new RowData.FieldGetter[spec.outputGroupKey.length];
                for (int i = 0; i < groupKeyGetters.length; i++) {
                    groupKeyGetters[i] =
                            RowData.createFieldGetter(
                                    spec.outputType.getTypeAt(spec.outputGroupKey[i]), i);
                }
        }
        openGroupReader();

        scheduler = new RefreshScheduler<>(spec.capacity, this::startRefresh, this::emitWatermark);
        // rows waiting in memory for a checkpoint are bounded: a refresh adds more
        scheduler.setGate(() -> ending || memory == null || !memory.full());
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        metrics.gauge("numRefreshes", () -> refreshes);
        metrics.gauge("numWholeGroupRefreshes", () -> wholeRefreshes);
        metrics.gauge("numScopedRefreshes", () -> scopedRefreshes);
        metrics.gauge("numPagedRefreshes", () -> pagedRefreshes);
        metrics.gauge("maxRefreshedGroupRows", () -> refreshedRows);
        metrics.gauge("numLookups", this::numLookups);
        metrics.gauge("numReadBacks", this::readBacks);
        metrics.gauge("numInconsistentGroups", () -> inconsistentGroups);
        metrics.gauge("numGroupRowsEmitted", () -> rowsEmitted);
        metrics.gauge("numKeysRefreshing", () -> scheduler.numRunning());
        metrics.gauge("numKeysWaiting", () -> scheduler.numWaiting());
        metrics.gauge("numCoalescedChanges", () -> scheduler.numCoalesced());
        metrics.gauge("numDeferredKeys", () -> deferredKeys);
        metrics.gauge("numPinnedRows", () -> memory == null ? 0L : memory.pinnedRows());

        if (deferred) {
            schedulePull();
        }
        LOG.info(
                "{} is active (output mode {}, capacity {}).",
                getClass().getSimpleName(),
                spec.outputMode,
                spec.capacity);
    }

    // ------------------------------------------------------------------------------------------
    // input
    // ------------------------------------------------------------------------------------------

    /**
     * The group of the given key may have changed. Blocks the input while too many keys wait for a
     * refresh that can start.
     *
     * <p>While no refresh can start before a checkpoint completes, blocking would hold back the
     * barrier of that checkpoint. Then keys beyond the limit wait in state instead.
     */
    protected void requestRefresh(RowData key) throws Exception {
        if (scheduler.isGateClosed()
                && scheduler.numWaiting() >= spec.maxWaitingKeys
                && !scheduler.keysWithWork().contains(key)) {
            defer(key);
            return;
        }
        RowData copy = copyKey(key);
        if (deferred && !scheduler.keysWithWork().contains(copy) && waitsInState(copy)) {
            // what changed before is not in its scope: restored, or dropped by defer
            persistedPending.add(copy);
            wholeNext.add(copy);
        }
        scheduler.note(copy);
        while (scheduler.numWaiting() > spec.maxWaitingKeys && !scheduler.isGateClosed()) {
            mailboxExecutor.yield();
        }
    }

    /** Lets a key wait in state only; its next refresh reads the whole group. */
    private void defer(RowData key) throws Exception {
        discardScope(key);
        Object current = getCurrentKey();
        setCurrentKey(key);
        if (pendingState.value() == null) {
            pendingState.update(true);
            deferredKeys++;
        }
        setCurrentKey(current);
        deferred = true;
    }

    private boolean waitsInState(RowData key) throws Exception {
        Object current = getCurrentKey();
        setCurrentKey(key);
        boolean pending = pendingState.value() != null;
        setCurrentKey(current);
        return pending;
    }

    private void schedulePull() {
        if (!pullScheduled) {
            pullScheduled = true;
            mailboxExecutor.execute(this::pullDeferred, "Pull deferred group refreshes");
        }
    }

    /**
     * Hands keys that wait in state to the scheduler, as many as may wait there. Scans all pending
     * flags, so it runs only once the scheduler has no waiting keys left.
     */
    private void pullDeferred() throws Exception {
        pullScheduled = false;
        int room = spec.maxWaitingKeys - scheduler.numWaiting();
        if (!deferred || room <= 0 || (!ending && scheduler.isGateClosed())) {
            return;
        }
        List<RowData> batch = new ArrayList<>();
        boolean more = false;
        try (Stream<Object> keys =
                getKeyedStateBackend().getKeys(PENDING_STATE_NAME, VoidNamespace.INSTANCE)) {
            Iterator<Object> it = keys.iterator();
            while (it.hasNext()) {
                RowData key = copyKey(it.next());
                if (scheduler.keysWithWork().contains(key)) {
                    // its flag is cleared once its work is done
                    persistedPending.add(key);
                } else if (batch.size() < room) {
                    batch.add(key);
                } else {
                    more = true;
                    break;
                }
            }
        }
        deferred = more;
        for (RowData key : batch) {
            persistedPending.add(key);
            wholeNext.add(key);
            scheduler.note(key);
        }
    }

    /**
     * Emits the rows of a line whose output depends on this change only, such as a row that can
     * never match and is padded.
     */
    protected void emitUnmatched(RowData[] line, boolean retract) throws Exception {
        for (BinaryRowData row : rows.project(line)) {
            if (retract) {
                emit(RowKind.DELETE, row);
            } else {
                emit(
                        spec.outputMode == GroupRefreshOutputMode.UPSERT
                                ? RowKind.UPDATE_AFTER
                                : RowKind.INSERT,
                        row);
            }
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        scheduler.watermark(mark);
    }

    private void emitWatermark(Watermark mark) {
        try {
            super.processWatermark(mark);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to emit watermark " + mark, e);
        }
    }

    /**
     * Blocks until every group with work was refreshed. Subclasses call it when all their inputs
     * ended, which precedes {@link #finish()}.
     */
    protected void waitUntilIdle() throws Exception {
        // no checkpoint may complete before the input ends, so the memory may grow until then
        ending = true;
        scheduler.gateOpened();
        while (deferred || !scheduler.isIdle()) {
            if (deferred && scheduler.numWaiting() < spec.maxWaitingKeys) {
                pullDeferred();
            } else {
                mailboxExecutor.yield();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // refresh
    // ------------------------------------------------------------------------------------------

    private void startRefresh(RowData key) {
        refreshes++;
        if (!ending && memory != null && memory.full()) {
            startsWhileFull++;
        }
        try {
            setCurrentKey(key);
            if (wholeNext.remove(key)) {
                discardScope(key);
                refreshWholeGroup(key);
            } else if (!startScopedRefresh(key)) {
                refreshWholeGroup(key);
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    /**
     * Refreshes the whole group of a key whose refresh started: reads all its lines and all the
     * rows it emitted before.
     */
    protected void refreshWholeGroup(RowData key) throws Exception {
        wholeRefreshes++;
        setCurrentKey(key);
        Refresh refresh = new Refresh(key);
        if (memory == null) {
            refresh.previousRead(null);
        } else if (pageSize() > 0) {
            memory.read(
                    key, pageSize(), refresh::previousRead, refresh::previousTooLarge, this::fail);
        } else {
            memory.read(key, refresh::previousRead, this::fail);
        }
        readGroup(key, refresh);
    }

    // ------------------------------------------------------------------------------------------
    // scoped refreshes: the rows of a group that some changes affect
    // ------------------------------------------------------------------------------------------

    /**
     * Starts a refresh of only the rows of the group that the changes since the last refresh
     * affect, or returns false to refresh the whole group. Called in the task thread with the key
     * as the current key. A started scoped refresh ends with {@link #completeScopedRefresh}, or
     * turns into a refresh of the whole group with {@link #refreshWholeGroup}.
     */
    protected boolean startScopedRefresh(RowData key) throws Exception {
        return false;
    }

    /** Drops what the changes since the last refresh noted, as the whole group is refreshed. */
    protected void discardScope(RowData key) {}

    /** Whether rows of a group can be read and replaced one by one. */
    protected boolean rowsCanBeReplaced() {
        return memory != null && memory.supportsRows() && rows.knowsRowKeysOfLines();
    }

    /**
     * Whether rows of a group can be read and replaced one by one, and the row keys of the rows of
     * a complete line depend on that line only, see {@link #rowKeysOfLine}.
     */
    protected boolean rowsCanBeReadByKey() {
        return memory != null && memory.supportsRows() && rows.rowKeysDependOnLinesOnly();
    }

    /** The row keys of the rows a complete line emits: none if the projection filters it out. */
    protected List<RowData> rowKeysOfLine(RowData[] line) throws Exception {
        return rows.rowKeysOf(line);
    }

    /**
     * The row key of the row a line emits, or null if it emits none; the line needs only the fields
     * that make up the row key.
     */
    protected @Nullable RowData rowKeyOf(RowData[] line) throws Exception {
        return rows.rowKeyOf(line);
    }

    /** Reads the rows with the given row keys that the group emitted before. */
    protected void readPreviousRows(
            RowData key,
            Collection<RowData> rowKeys,
            Consumer<Map<RowData, BinaryRowData>> onRows,
            Consumer<Throwable> onError)
            throws Exception {
        setCurrentKey(key);
        memory.readRows(key, rowKeys, onRows, onError);
    }

    /**
     * Replaces the rows with the given row keys of the group by the rows of the given lines, which
     * have no other row keys, and ends the refresh. Called in the task thread.
     */
    protected void completeScopedRefresh(
            RowData key,
            Set<RowData> rowKeys,
            Map<RowData, BinaryRowData> previous,
            List<RowData[]> lines)
            throws Exception {
        setCurrentKey(key);
        boolean[] consistent = {true};
        Map<RowData, BinaryRowData> next = rows.rowsOf(lines, () -> consistent[0] = false);
        if (!consistent[0] || !rowKeys.containsAll(next.keySet())) {
            // the scope does not cover the rows it produced; replace the whole group instead
            refreshWholeGroup(key);
            return;
        }
        scopedRefreshes++;
        emitDifference(previous, next);
        memory.writtenRows(key, rowKeys, next);
        // through the mailbox, so that refreshes that complete at once do not nest
        mailboxExecutor.execute(() -> finishRefresh(key), "Finish refresh of key %s", key);
    }

    private void completeRefresh(Refresh refresh) throws Exception {
        RowData key = refresh.key;
        setCurrentKey(key);
        boolean[] consistent = {true};
        Map<RowData, BinaryRowData> next = rows.rowsOf(refresh.lines, () -> consistent[0] = false);
        readConsistently(key, consistent[0]);
        if (memory == null) {
            upsert(key, next);
        } else {
            emitDifference(refresh.previous, next);
            memory.written(key, next);
        }
        refreshedRows = Math.max(refreshedRows, refresh.lines.size());
        finishRefresh(key);
    }

    /**
     * Notes whether a refresh read the group consistently; a group read inconsistently is refreshed
     * again.
     */
    private void readConsistently(RowData key, boolean consistent) {
        if (consistent) {
            inconsistentRefreshes.remove(key);
            return;
        }
        inconsistentGroups++;
        int times = inconsistentRefreshes.merge(key, 1, Integer::sum);
        Preconditions.checkState(
                times < MAX_INCONSISTENT_REFRESHES,
                "The group of key %s produced two rows with the same output key in %s refreshes "
                        + "in a row. This happens if the query replaces NULL in a column of the "
                        + "output key by a constant, e.g. COALESCE(k, 0), that is also a value of "
                        + "the column. Use a constant that the column never holds.",
                describe(key),
                times);
        // see MAX_INCONSISTENT_REFRESHES; the group keeps the first of the rows until then
        wholeNext.add(key);
        scheduler.note(key);
    }

    /** Ends the refresh of the key, and starts the next ones. */
    private void finishRefresh(RowData key) {
        // may start the refreshes of other keys, which changes the current key
        scheduler.finished(key);
        if (!scheduler.keysWithWork().contains(key)
                // a deferred key may have been noted again before it was pulled
                && (persistedPending.remove(key) || deferred)) {
            setCurrentKey(key);
            pendingState.clear();
        }
        if (deferred && scheduler.numWaiting() == 0) {
            schedulePull();
        }
    }

    private void emitDifference(
            Map<RowData, BinaryRowData> previous, Map<RowData, BinaryRowData> next) {
        // deletes first: a consumer keyed coarser than the row key sees them before upserts
        for (Map.Entry<RowData, BinaryRowData> entry : previous.entrySet()) {
            if (!next.containsKey(entry.getKey())) {
                emit(RowKind.DELETE, entry.getValue());
            }
        }
        for (Map.Entry<RowData, BinaryRowData> entry : next.entrySet()) {
            BinaryRowData old = previous.get(entry.getKey());
            if (old == null) {
                emit(RowKind.INSERT, entry.getValue());
                rowsEmitted++;
            } else if (!old.equals(entry.getValue())) {
                if (spec.emitUpdateBefore) {
                    emit(RowKind.UPDATE_BEFORE, old);
                }
                emit(RowKind.UPDATE_AFTER, entry.getValue());
                rowsEmitted++;
            }
        }
    }

    /** {@link GroupRefreshOutputMode#UPSERT}: emits the row of the group, or deletes its key. */
    private void upsert(RowData key, Map<RowData, BinaryRowData> next) throws Exception {
        Preconditions.checkState(
                next.size() <= 1,
                "The group of key %s has %s rows, but upserts expect at most one.",
                describe(key),
                next.size());
        if (next.isEmpty()) {
            // delete what was emitted; after a restore, the failed run may have emitted a row
            // after the checkpoint that the state does not know about, so delete regardless
            if (emittedState.value() != null || restored) {
                GenericRowData delete =
                        new GenericRowData(RowKind.DELETE, spec.outputType.getFieldCount());
                for (int i = 0; i < groupKeyGetters.length; i++) {
                    delete.setField(spec.outputGroupKey[i], groupKeyGetters[i].getFieldOrNull(key));
                }
                output.collect(outRecord.replace(delete));
                emittedState.clear();
            }
        } else {
            emit(RowKind.UPDATE_AFTER, next.values().iterator().next());
            emittedState.update(true);
            rowsEmitted++;
        }
    }

    private void emit(RowKind kind, BinaryRowData row) {
        BinaryRowData out = row.copy();
        out.setRowKind(kind);
        output.collect(outRecord.replace(out));
    }

    /** Fails the task from the task thread. */
    protected void fail(Throwable error) {
        mailboxExecutor.execute(
                () -> {
                    throw new FlinkRuntimeException(
                            "A group refresh of the delta join failed.", error);
                },
                "fail group refresh");
    }

    // ------------------------------------------------------------------------------------------
    // checkpoints
    // ------------------------------------------------------------------------------------------

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        // without channel state, no barrier overtakes records: every row emitted before the
        // barrier reaches the consumers before it
        barrierAligned = !checkpointOptions.needsChannelState();
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        for (RowData key : scheduler.keysWithWork()) {
            if (persistedPending.add(key)) {
                setCurrentKey(key);
                pendingState.update(true);
            }
        }
        if (memory != null) {
            memory.snapshotState(context.getCheckpointId(), barrierAligned);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        if (memory != null) {
            memory.notifyCheckpointComplete(checkpointId);
            // in the task thread, after this call: starting a refresh changes the current key
            mailboxExecutor.execute(
                    () -> {
                        scheduler.gateOpened();
                        if (deferred && scheduler.numWaiting() == 0) {
                            schedulePull();
                        }
                    },
                    "Start group refreshes after checkpoint %s",
                    checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            LOG.info(
                    "{} closes after {} refreshes ({} of whole groups, the largest with {} rows), {}"
                            + " lookups and {} read-backs of the output; {} refreshes were"
                            + " inconsistent, {} read in pages.",
                    getClass().getSimpleName(),
                    refreshes,
                    wholeRefreshes,
                    refreshedRows,
                    numLookups(),
                    readBacks(),
                    inconsistentGroups,
                    pagedRefreshes);
        }
        super.close();
        if (memory != null) {
            memory.close();
        }
        if (rows != null) {
            rows.close();
        }
    }

    private long readBacks() {
        return memory instanceof GroupOutputMemory.PinnedCacheMemory
                ? ((GroupOutputMemory.PinnedCacheMemory) memory).readBacks()
                : 0L;
    }

    /** The values of a group key, for messages. */
    protected String describe(RowData key) {
        return spec.groupKeyType == null
                ? String.valueOf(key)
                : GroupRows.format(key, spec.groupKeyType);
    }

    /** A copy of a key that may be reused by the caller. */
    protected static RowData copyKey(Object key) {
        return key instanceof BinaryRowData ? ((BinaryRowData) key).copy() : (RowData) key;
    }

    @VisibleForTesting
    public RefreshScheduler<RowData, Watermark> getScheduler() {
        return scheduler;
    }

    @VisibleForTesting
    public long getRefreshes() {
        return refreshes;
    }

    /** Refreshes that started while the memory was full; always 0 unless the input ended. */
    @VisibleForTesting
    public long getStartsWhileFull() {
        return startsWhileFull;
    }

    @VisibleForTesting
    public boolean hasDeferredKeys() {
        return deferred;
    }

    @VisibleForTesting
    public long getDeferredKeys() {
        return deferredKeys;
    }

    @VisibleForTesting
    public long getPinnedRows() {
        return memory == null ? 0 : memory.pinnedRows();
    }

    @VisibleForTesting
    public long getScopedRefreshes() {
        return scopedRefreshes;
    }

    @VisibleForTesting
    @Nullable
    GroupOutputMemory getMemory() {
        return memory;
    }

    /**
     * The two reads of one refresh. Completes in the task thread once both arrived: at once, or
     * page by page if the lines came in pages or the group emitted more rows than a page.
     */
    private final class Refresh implements GroupRead {
        private final RowData key;
        private @Nullable Map<RowData, BinaryRowData> previous;
        private boolean previousTooLarge;
        private List<RowData[]> lines;
        private @Nullable LinePages pages;
        private int missing = 2;

        private Refresh(RowData key) {
            this.key = key;
        }

        private void previousRead(@Nullable Map<RowData, BinaryRowData> rows) {
            if (rows != null && tooLarge(rows.size(), "rows emitted before")) {
                return;
            }
            previous = rows;
            arrived();
        }

        private void previousTooLarge() {
            previousTooLarge = true;
            arrived();
        }

        @Override
        public void lines(List<RowData[]> rows) {
            if (tooLarge(rows.size(), "joined rows")) {
                return;
            }
            lines = rows;
            arrived();
        }

        @Override
        public void pages(LinePages linePages) {
            if (pageSize() == 0) {
                fail(new IllegalStateException("Pages need the rows of a group by row key."));
                return;
            }
            pages = linePages;
            arrived();
        }

        @Override
        public void failed(Throwable error) {
            fail(error);
        }

        /** Fails the task if a read of the group holds more rows than allowed. */
        private boolean tooLarge(int size, String what) {
            if (size <= spec.maxGroupSize) {
                return false;
            }
            fail(groupTooLarge(key, size, what));
            return true;
        }

        private void arrived() {
            if (--missing > 0) {
                return;
            }
            // always through the mailbox, so that a refresh never completes inside the call that
            // started it
            mailboxExecutor.execute(
                    () -> {
                        try {
                            if (pages == null && !previousTooLarge) {
                                completeRefresh(this);
                            } else {
                                PagedRefresh paged =
                                        new PagedRefresh(key, pages == null ? lines : null);
                                paged.start(pages != null ? pages : onePage(lines));
                            }
                        } catch (Throwable t) {
                            throw new FlinkRuntimeException(
                                    "Could not replace the group of key " + key, t);
                        }
                    },
                    "Complete group refresh of key %s",
                    key);
        }
    }

    private static LinePages onePage(List<RowData[]> lines) {
        return (onLines, onError) -> onLines.accept(lines, true);
    }

    /**
     * Refreshes a large group in three passes, so that deletes precede inserts as in {@link
     * #emitDifference}: the first pass collects the row keys of the lines, the second deletes the
     * emitted rows with other row keys, and the third reads the lines again and replaces the rows
     * of each page. Rows that the third pass reads under other row keys than the first changed in
     * between: they are left to a refresh of the whole group that follows. Holds the row keys of
     * the group and one page.
     */
    private final class PagedRefresh {
        private final RowData key;
        private final @Nullable List<RowData[]> lines;
        private final GroupRows.Seen keys;
        private final GroupRows.Seen emitted;
        private boolean consistent = true;
        private boolean changed;
        private int replaced;

        private PagedRefresh(RowData key, @Nullable List<RowData[]> lines) {
            this.key = key;
            this.lines = lines;
            this.keys = rows.new Seen();
            this.emitted = rows.new Seen();
            // what memory holds of the group is outdated after the first page
            memory.forget(key);
        }

        private void start(LinePages pages) {
            pages.next((page, last) -> collectKeys(pages, page, last), this::fail);
        }

        private void collectKeys(LinePages pages, List<RowData[]> page, boolean last) {
            try {
                setCurrentKey(key);
                rows.rowsOf(page, keys, () -> consistent = false);
                if (last) {
                    deleteOthers();
                } else {
                    start(pages);
                }
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void deleteOthers() throws Exception {
            List<RowData> deleted = new ArrayList<>();
            memory.readPages(
                    key,
                    spec.pageSize,
                    (page, last) -> {
                        for (Map.Entry<RowData, BinaryRowData> entry : page.entrySet()) {
                            if (!keys.rowKeys.contains(entry.getKey())) {
                                emit(RowKind.DELETE, entry.getValue());
                                deleted.add(entry.getKey());
                            }
                        }
                        if (last) {
                            try {
                                setCurrentKey(key);
                                memory.writtenRows(key, deleted, Collections.emptyMap());
                                readAgain();
                            } catch (Throwable t) {
                                fail(t);
                            }
                        }
                    },
                    this::fail);
        }

        private void readAgain() throws Exception {
            if (lines != null) {
                replace(onePage(lines));
                return;
            }
            readGroup(
                    key,
                    new GroupRead() {
                        @Override
                        public void lines(List<RowData[]> all) {
                            replace(onePage(all));
                        }

                        @Override
                        public void pages(LinePages pages) {
                            replace(pages);
                        }

                        @Override
                        public void failed(Throwable error) {
                            fail(error);
                        }
                    });
        }

        private void replace(LinePages pages) {
            pages.next((page, last) -> replacePage(pages, page, last), this::fail);
        }

        private void replacePage(LinePages pages, List<RowData[]> page, boolean last) {
            try {
                setCurrentKey(key);
                Map<RowData, BinaryRowData> next =
                        rows.rowsOf(page, emitted, () -> consistent = false);
                changed |= next.keySet().removeIf(rowKey -> !keys.rowKeys.contains(rowKey));
                replaced += next.size();
                if (next.isEmpty()) {
                    afterPage(pages, last);
                    return;
                }
                memory.readRows(
                        key,
                        next.keySet(),
                        previous ->
                                mailboxExecutor.execute(
                                        () -> {
                                            setCurrentKey(key);
                                            emitDifference(previous, next);
                                            memory.writtenRows(key, next.keySet(), next);
                                            afterPage(pages, last);
                                        },
                                        "Replace a page of group %s",
                                        key),
                        this::fail);
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void afterPage(LinePages pages, boolean last) {
            if (!last) {
                replace(pages);
                return;
            }
            pagedRefreshes++;
            refreshedRows = Math.max(refreshedRows, keys.rowKeys.size());
            readConsistently(key, consistent);
            if (changed || replaced < keys.rowKeys.size()) {
                wholeNext.add(key);
                scheduler.note(key);
            }
            finishRefresh(key);
        }

        private void fail(Throwable error) {
            GroupRefreshOperatorBase.this.fail(error);
        }
    }
}
