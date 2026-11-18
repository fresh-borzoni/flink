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

package org.apache.flink.table.runtime.operators.join.deltajoin.anchor;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Turns the changes of one input table into notes for the groups they may affect, see {@link
 * AnchorNotes}: one note per anchor row that the changed row connects to in the current state of
 * the other tables.
 *
 * <p>For a table whose notes carry rows, the update before and after of one row are noted together,
 * so that the group is refreshed once with both. An update after without its update before carries
 * no row, and refreshes the whole group, unless the table is joined on equalities only.
 *
 * <p>Rows whose lookups are in flight are kept in operator state on checkpoints.
 */
public class AnchorLocateOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AnchorLocateOperator.class);

    private static final String IN_FLIGHT_STATE_NAME = "anchor-locate-in-flight";

    private final AnchorJoinTree tree;
    private final AnchorLookupSpec[] lookupSpecs;
    private final long timeoutMs;
    private final int capacity;
    private final int leaf;
    private final RowType inputType;
    private final AnchorNotes notes;

    /** The unique key of the input, which identifies the row of an update. */
    private final int[] uniqueKey;

    /**
     * Whether the input is joined towards the anchor on equalities only, so that an update after
     * alone replaces the lines of its row.
     */
    private final boolean equalityJoin;

    private final transient MailboxExecutor mailboxExecutor;

    private transient AnchorJoinEvaluator evaluator;
    private transient int inFlight;

    /** Rows restored from a checkpoint that wait for a free slot. */
    private transient ArrayDeque<Runnable> restoredWork;

    private transient RowDataSerializer inputSerializer;
    private transient ListState<RowData> inFlightState;
    private transient Map<Long, RowData> inFlightRows;

    /** A row accepted from the input that waits for a free slot. */
    private transient RowData waitingRow;

    /** Whether the old and the new row of an update are sent together. */
    private transient boolean pairsUpdates;

    /** The old row of an update, until the new one arrives. */
    private transient @Nullable RowData pendingBefore;

    private transient RowData.FieldGetter[] uniqueKeyGetters;
    private transient List<RowData> restoredRows;
    private transient long nextId;
    private transient Counter numAnchorKeysLocated;

    public AnchorLocateOperator(
            StreamOperatorParameters<RowData> parameters,
            AnchorJoinTree tree,
            AnchorLookupSpec[] lookupSpecs,
            int leaf,
            RowType inputType,
            long timeoutMs,
            int capacity,
            AnchorNotes notes,
            int[] uniqueKey,
            boolean equalityJoin) {
        super(parameters);
        Preconditions.checkArgument(capacity > 0, "The capacity must be positive: %s", capacity);
        this.tree = tree;
        this.lookupSpecs = lookupSpecs;
        this.timeoutMs = timeoutMs;
        this.capacity = capacity;
        this.mailboxExecutor = parameters.getMailboxExecutor();
        this.leaf = leaf;
        this.inputType = inputType;
        this.notes = notes;
        this.uniqueKey = uniqueKey;
        this.equalityJoin = equalityJoin;
    }

    private static GeneratedFilterCondition[][] emptyFilters(int numNodes) {
        GeneratedFilterCondition[][] filters = new GeneratedFilterCondition[numNodes][];
        for (int i = 0; i < numNodes; i++) {
            filters[i] = new GeneratedFilterCondition[0];
        }
        return filters;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        inputSerializer = new RowDataSerializer(inputType);
        inFlightState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(IN_FLIGHT_STATE_NAME, inputSerializer));
        restoredRows = new ArrayList<>();
        if (context.isRestored()) {
            for (RowData row : inFlightState.get()) {
                restoredRows.add(row);
            }
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        evaluator =
                new AnchorJoinEvaluator(
                        tree,
                        lookupSpecs,
                        new GeneratedJoinCondition[tree.getNumNodes()],
                        emptyFilters(tree.getNumNodes()),
                        timeoutMs,
                        runnable -> mailboxExecutor.execute(runnable::run, "anchor lookup result"),
                        capacity);
        evaluator.open(
                getRuntimeContext(),
                DefaultOpenContext.INSTANCE,
                tree.lookupsUsedToLocate(leaf, lookupSpecs.length),
                false);
        restoredWork = new ArrayDeque<>();
        getRuntimeContext().getMetricGroup().gauge("numLookups", evaluator::getNumLookups);
        getRuntimeContext().getMetricGroup().gauge("numInFlight", () -> inFlight);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("numSharedLookups", evaluator::getNumSharedLookups);
        LOG.info(
                "Opened {} (timeout {} ms, capacity {}).",
                getClass().getSimpleName(),
                timeoutMs,
                capacity);
        inFlightRows = new LinkedHashMap<>();
        numAnchorKeysLocated = getRuntimeContext().getMetricGroup().counter("numAnchorKeysLocated");
        pairsUpdates =
                notes.carriesRows(leaf)
                        && uniqueKey.length > 0
                        && tree.getLocateLookups(leaf).length == 0;
        uniqueKeyGetters = new RowData.FieldGetter[uniqueKey.length];
        for (int i = 0; i < uniqueKey.length; i++) {
            uniqueKeyGetters[i] =
                    RowData.createFieldGetter(inputType.getTypeAt(uniqueKey[i]), uniqueKey[i]);
        }
        for (RowData row : restoredRows) {
            // restored rows wait for free slots like new ones; they stay in flight for snapshots
            long id = nextId++;
            inFlightRows.put(id, row);
            restoredWork.add(() -> startLocate(id, row));
        }
        restoredRows = null;
        startRestoredWork();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (pairsUpdates) {
            pair(inputSerializer.copy(element.getValue()));
            return;
        }
        // the row is accepted from here on and must survive a checkpoint taken while waiting
        waitingRow = inputSerializer.copy(element.getValue());
        waitForCapacity();
        RowData row = waitingRow;
        waitingRow = null;
        locate(row);
    }

    /** Holds back the old row of an update until the new one arrives; locates without lookups. */
    private void pair(RowData row) {
        RowData before = pendingBefore;
        pendingBefore = null;
        if (before != null) {
            if (row.getRowKind() == RowKind.UPDATE_AFTER && sameRow(before, row)) {
                locateUpdate(before, row);
                return;
            }
            locate(before);
        }
        if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            pendingBefore = row;
        } else if (row.getRowKind() == RowKind.UPDATE_AFTER && !equalityJoin) {
            // the old row is unknown and may have had other lines, so the whole group is refreshed
            for (BinaryRowData key : locateNow(row)) {
                output.collect(new StreamRecord<>(notes.note(key, leaf, null, AnchorNotes.ADD)));
            }
        } else {
            locate(row);
        }
    }

    private boolean sameRow(RowData before, RowData after) {
        for (RowData.FieldGetter getter : uniqueKeyGetters) {
            if (!Objects.equals(getter.getFieldOrNull(before), getter.getFieldOrNull(after))) {
                return false;
            }
        }
        return true;
    }

    private Collection<BinaryRowData> locateNow(RowData row) {
        Collection<BinaryRowData> keys =
                evaluator.locate(leaf, row, evaluator.newStamp()).getNow(null);
        Preconditions.checkState(keys != null, "Locating needs no lookup.");
        numAnchorKeysLocated.inc(keys.size());
        return keys;
    }

    private void locateUpdate(RowData before, RowData after) {
        Collection<BinaryRowData> beforeKeys = locateNow(before);
        Collection<BinaryRowData> afterKeys = locateNow(after);
        for (BinaryRowData key : beforeKeys) {
            byte change = afterKeys.contains(key) ? AnchorNotes.REPLACED : AnchorNotes.RETRACT;
            output.collect(new StreamRecord<>(notes.note(key, leaf, before, change)));
        }
        for (BinaryRowData key : afterKeys) {
            output.collect(new StreamRecord<>(notes.note(key, leaf, after, AnchorNotes.ADD)));
        }
    }

    /** Sends the old row of an update alone, if the new one did not follow it. */
    private void flushPendingBefore() {
        if (pendingBefore != null) {
            RowData before = pendingBefore;
            pendingBefore = null;
            locate(before);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushPendingBefore();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        flushPendingBefore();
        // results of rows before the watermark must not be overtaken by it
        drain();
        super.processWatermark(mark);
    }

    @Override
    public void endInput() throws Exception {
        flushPendingBefore();
        drain();
    }

    @Override
    public void finish() throws Exception {
        flushPendingBefore();
        drain();
        super.finish();
    }

    @Override
    public void close() throws Exception {
        if (evaluator != null) {
            evaluator.close();
        }
        super.close();
    }

    /** Starts restored rows while there are free slots. */
    private void startRestoredWork() {
        while (inFlight < capacity && !restoredWork.isEmpty()) {
            restoredWork.poll().run();
        }
    }

    /** Blocks the input until fewer than {@code capacity} locates are in flight. */
    private void waitForCapacity() throws InterruptedException {
        startRestoredWork();
        while (inFlight >= capacity) {
            mailboxExecutor.yield();
        }
    }

    /** Blocks until all locates in flight, and the restored ones, are done. */
    private void drain() throws InterruptedException {
        startRestoredWork();
        while (inFlight > 0 || !restoredWork.isEmpty()) {
            mailboxExecutor.yield();
            startRestoredWork();
        }
    }

    private void fail(String message, Throwable error) {
        mailboxExecutor.execute(
                () -> {
                    throw new FlinkRuntimeException(message, error);
                },
                "fail anchor locate");
    }

    private void locate(RowData row) {
        long id = nextId++;
        inFlightRows.put(id, row);
        startLocate(id, row);
    }

    private void startLocate(long id, RowData row) {
        inFlight++;
        evaluator
                .locate(leaf, row, evaluator.newStamp())
                .whenComplete(
                        (keys, error) -> {
                            if (error != null) {
                                fail("Could not locate the anchor rows of " + row, error);
                                return;
                            }
                            try {
                                for (BinaryRowData key : keys) {
                                    output.collect(new StreamRecord<>(notes.note(key, leaf, row)));
                                }
                                numAnchorKeysLocated.inc(keys.size());
                                inFlightRows.remove(id);
                                inFlight--;
                                startRestoredWork();
                            } catch (Throwable t) {
                                fail("Could not emit the anchor keys of " + row, t);
                            }
                        });
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        List<RowData> pending = new ArrayList<>(inFlightRows.values());
        if (waitingRow != null) {
            pending.add(waitingRow);
        }
        inFlightState.update(pending);
    }
}
