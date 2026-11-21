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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRows;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The delta step of a join whose tables are read as of epochs: the change of the output that the
 * changed rows of one input make in an epoch.
 *
 * <p>The change of table {@code i} in epoch {@code e} reads the tables before it as of {@code e},
 * the tables after it as of {@code e - 1}, and the table itself as of {@code e - 1} for the lines
 * before and as of {@code e} for the lines after. Summed over all tables, these changes are the
 * change of the join from {@code e - 1} to {@code e}.
 *
 * <p>The input comes in order from one source subtask, so the operator keeps per epoch and key the
 * old row of the first change and the new row of the last. Once the watermark completes an epoch,
 * it evaluates the epoch's keys in batches and emits their lines as entries (table, row key, row):
 * {@link RowKind#INSERT} for a line after, {@link RowKind#DELETE} for a line before, timestamped
 * with the epoch. The watermark is held back until all entries of its epochs are out.
 *
 * <p>Entries may repeat, e.g. two keys under one preserved row both report its padded line, or a
 * restore evaluates a key again. The apply step keeps each entry once per epoch.
 */
public class AnchorDeltaOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private static final String PENDING_STATE_NAME = "anchor-delta-pending";

    private static final int MAX_KEYS_PER_EVALUATION = 64;

    private final AnchorJoinTree tree;
    private final AnchorLookupSpec[] lookupSpecs;
    private final GeneratedJoinCondition[] conditions;
    private final GeneratedFilterCondition[][] filters;
    private final GroupRefreshSpec refreshSpec;
    private final int leaf;
    private final RowType inputType;
    private final int[] uniqueKey;
    private final long timeoutMs;
    private final int capacity;

    private final transient MailboxExecutor mailboxExecutor;

    private transient AnchorJoinEvaluator evaluator;
    private transient GroupRows rows;
    private transient RowDataSerializer inputSerializer;
    private transient RowDataSerializer keySerializer;
    private transient RowData.FieldGetter[] keyGetters;
    private transient ListState<RowData> pendingState;

    /** Per epoch, the rows of every changed key as of the epoch before and as of the epoch. */
    private transient TreeMap<Long, Map<BinaryRowData, Images>> pending;

    /** Batches of completed epochs that wait for a free slot, in epoch order. */
    private transient ArrayDeque<Batch> waiting;

    /** Watermarks held back until the entries of their epochs are out. */
    private transient ArrayDeque<Watermark> held;

    private transient long inputWatermark;
    private transient int inFlight;
    private transient Counter numChangedKeys;
    private transient Counter numEntries;

    public AnchorDeltaOperator(
            StreamOperatorParameters<RowData> parameters,
            AnchorJoinTree tree,
            AnchorLookupSpec[] lookupSpecs,
            GeneratedJoinCondition[] conditions,
            GeneratedFilterCondition[][] filters,
            GroupRefreshSpec refreshSpec,
            int leaf,
            RowType inputType,
            int[] uniqueKey,
            long timeoutMs,
            int capacity) {
        super(parameters);
        Preconditions.checkArgument(capacity > 0, "The capacity must be positive: %s", capacity);
        Preconditions.checkArgument(
                uniqueKey.length > 0, "Input %s needs a unique key to tell its rows apart.", leaf);
        this.tree = tree;
        this.lookupSpecs = lookupSpecs;
        this.conditions = conditions;
        this.filters = filters;
        this.refreshSpec = refreshSpec;
        this.leaf = leaf;
        this.inputType = inputType;
        this.uniqueKey = uniqueKey;
        this.timeoutMs = timeoutMs;
        this.capacity = capacity;
        this.mailboxExecutor = parameters.getMailboxExecutor();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        inputSerializer = new RowDataSerializer(inputType);
        RowType stateType =
                RowType.of(new BigIntType(false), inputType.copy(true), inputType.copy(true));
        pendingState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        PENDING_STATE_NAME, new RowDataSerializer(stateType)));
        LogicalType[] keyTypes = new LogicalType[uniqueKey.length];
        keyGetters = new RowData.FieldGetter[uniqueKey.length];
        for (int i = 0; i < uniqueKey.length; i++) {
            keyTypes[i] = inputType.getTypeAt(uniqueKey[i]);
            keyGetters[i] = RowData.createFieldGetter(keyTypes[i], uniqueKey[i]);
        }
        keySerializer = new RowDataSerializer(keyTypes);
        pending = new TreeMap<>();
        if (context.isRestored()) {
            int arity = inputType.getFieldCount();
            for (RowData state : pendingState.get()) {
                BinaryRowData before = state.isNullAt(1) ? null : normalize(state.getRow(1, arity));
                BinaryRowData after = state.isNullAt(2) ? null : normalize(state.getRow(2, arity));
                pending.computeIfAbsent(state.getLong(0), e -> new LinkedHashMap<>())
                        .put(keyOf(before != null ? before : after), new Images(before, after));
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
                        conditions,
                        filters,
                        timeoutMs,
                        runnable -> mailboxExecutor.execute(runnable::run, "anchor delta lookup"),
                        capacity);
        evaluator.open(
                getRuntimeContext(),
                DefaultOpenContext.INSTANCE,
                tree.lookupsUsedForDeltas(lookupSpecs.length),
                true);
        rows = new GroupRows(refreshSpec, getRuntimeContext());
        Preconditions.checkState(
                rows.rowKeysDependOnLinesOnly(),
                "Every input needs a unique key, or the output a primary key.");
        waiting = new ArrayDeque<>();
        held = new ArrayDeque<>();
        inputWatermark = Long.MIN_VALUE;
        getRuntimeContext().getMetricGroup().gauge("numLookups", evaluator::getNumLookups);
        getRuntimeContext().getMetricGroup().gauge("numCachedReads", evaluator::getNumCachedReads);
        getRuntimeContext().getMetricGroup().gauge("numInFlight", () -> inFlight);
        numChangedKeys = getRuntimeContext().getMetricGroup().counter("numChangedKeys");
        numEntries = getRuntimeContext().getMetricGroup().counter("numDeltaEntries");
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        long epoch;
        if (inputWatermark != Long.MIN_VALUE) {
            epoch = inputWatermark + 1;
        } else {
            Preconditions.checkState(
                    element.hasTimestamp(),
                    "A change arrived without its epoch, before the watermark that tells it.");
            epoch = element.getTimestamp();
        }
        boolean retract =
                row.getRowKind() == RowKind.UPDATE_BEFORE || row.getRowKind() == RowKind.DELETE;
        BinaryRowData image = normalize(row);
        Map<BinaryRowData, Images> keys =
                pending.computeIfAbsent(epoch, e -> new LinkedHashMap<>());
        Images images = keys.computeIfAbsent(keyOf(row), k -> new Images(retract ? image : null));
        Preconditions.checkState(
                !images.started, "A change of epoch %s after its watermark.", epoch);
        images.after = retract ? null : image;
        while (waiting.size() >= capacity) {
            mailboxExecutor.yield();
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        inputWatermark = mark.getTimestamp();
        queueUpTo(inputWatermark);
        held.add(mark);
        startWaiting();
    }

    @Override
    public void endInput() throws Exception {
        queueUpTo(Long.MAX_VALUE);
        drain();
    }

    @Override
    public void finish() throws Exception {
        queueUpTo(Long.MAX_VALUE);
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

    private void queueUpTo(long epoch) {
        for (Map.Entry<Long, Map<BinaryRowData, Images>> byEpoch :
                pending.headMap(epoch, true).entrySet()) {
            Batch batch = null;
            for (Map.Entry<BinaryRowData, Images> byKey : byEpoch.getValue().entrySet()) {
                if (byKey.getValue().started) {
                    continue;
                }
                byKey.getValue().started = true;
                if (batch == null || batch.keys.size() == MAX_KEYS_PER_EVALUATION) {
                    batch = new Batch(byEpoch.getKey());
                    waiting.add(batch);
                }
                batch.keys.add(byKey.getKey());
                batch.images.add(byKey.getValue());
            }
        }
    }

    /** Starts waiting batches while slots are free, then forwards the watermarks it can. */
    private void startWaiting() {
        while (inFlight < capacity && !waiting.isEmpty()) {
            start(waiting.poll());
        }
        while (!held.isEmpty() && pending.headMap(held.peek().getTimestamp(), true).isEmpty()) {
            Watermark mark = held.poll();
            try {
                super.processWatermark(mark);
            } catch (Exception e) {
                throw new FlinkRuntimeException("Could not forward watermark " + mark, e);
            }
            // later epochs read as of this one and after it
            evaluator.evictReadsBefore(mark.getTimestamp());
        }
    }

    private void drain() throws InterruptedException {
        startWaiting();
        while (inFlight > 0 || !waiting.isEmpty() || !held.isEmpty()) {
            mailboxExecutor.yield();
            startWaiting();
        }
    }

    private void start(Batch batch) {
        long epoch = batch.epoch;
        List<RowData> befores = new ArrayList<>(batch.keys.size());
        List<RowData> afters = new ArrayList<>(batch.keys.size());
        List<BinaryRowData> changed = new ArrayList<>(batch.keys.size());
        for (int k = 0; k < batch.keys.size(); k++) {
            Images images = batch.images.get(k);
            numChangedKeys.inc();
            if (images.before != null && images.before.equals(images.after)) {
                done(epoch, batch.keys.get(k));
                continue;
            }
            changed.add(batch.keys.get(k));
            if (images.before != null) {
                befores.add(images.before);
            }
            if (images.after != null) {
                afters.add(images.after);
            }
        }
        if (changed.isEmpty()) {
            return;
        }
        inFlight++;
        evaluator
                .deltaLines(leaf, befores, afters, epochsOf(epoch, true), epochsOf(epoch, false))
                .whenComplete(
                        (lines, error) -> {
                            if (error != null) {
                                fail("Could not evaluate the changes of " + changed, error);
                                return;
                            }
                            try {
                                emit(lines.get(0), RowKind.DELETE, epoch);
                                emit(lines.get(1), RowKind.INSERT, epoch);
                                changed.forEach(key -> done(epoch, key));
                                inFlight--;
                                startWaiting();
                            } catch (Throwable t) {
                                fail("Could not emit the changes of " + changed, t);
                            }
                        });
    }

    /** Per input, the epoch that the change of this input in the given epoch reads it as of. */
    private long[] epochsOf(long epoch, boolean before) {
        long[] epochs = new long[tree.getNumLeaves()];
        for (int t = 0; t < epochs.length; t++) {
            if (t < leaf) {
                epochs[t] = epoch;
            } else if (t > leaf) {
                epochs[t] = epoch - 1;
            } else {
                epochs[t] = before ? epoch - 1 : epoch;
            }
        }
        return epochs;
    }

    private void emit(List<RowData[]> lines, RowKind kind, long epoch) throws Exception {
        GenericRowData table = GenericRowData.of(leaf);
        for (RowData[] line : lines) {
            for (RowData[] keyed : rows.keyedRowsOf(line)) {
                output.collect(
                        new StreamRecord<>(
                                new JoinedRowData(
                                        kind, table, new JoinedRowData(keyed[0], keyed[1])),
                                epoch));
                numEntries.inc();
            }
        }
    }

    private void done(long epoch, BinaryRowData key) {
        Map<BinaryRowData, Images> keys = pending.get(epoch);
        keys.remove(key);
        if (keys.isEmpty()) {
            pending.remove(epoch);
        }
    }

    private void fail(String message, Throwable error) {
        mailboxExecutor.execute(
                () -> {
                    throw new FlinkRuntimeException(message, error);
                },
                "fail anchor delta");
    }

    private BinaryRowData keyOf(RowData row) {
        GenericRowData key = new GenericRowData(keyGetters.length);
        for (int i = 0; i < keyGetters.length; i++) {
            key.setField(i, keyGetters[i].getFieldOrNull(row));
        }
        return keySerializer.toBinaryRow(key).copy();
    }

    private BinaryRowData normalize(RowData row) {
        BinaryRowData binary = inputSerializer.toBinaryRow(row).copy();
        binary.setRowKind(RowKind.INSERT);
        return binary;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        List<RowData> state = new ArrayList<>();
        for (Map.Entry<Long, Map<BinaryRowData, Images>> byEpoch : pending.entrySet()) {
            for (Images images : byEpoch.getValue().values()) {
                // a row inserted and deleted within the epoch changes nothing
                if (images.before != null || images.after != null) {
                    state.add(GenericRowData.of(byEpoch.getKey(), images.before, images.after));
                }
            }
        }
        pendingState.update(state);
    }

    /** Keys of one completed epoch, evaluated together. */
    private static final class Batch {
        final long epoch;
        final List<BinaryRowData> keys = new ArrayList<>();
        final List<Images> images = new ArrayList<>();

        Batch(long epoch) {
            this.epoch = epoch;
        }
    }

    /** A key's rows as of the epoch before and as of the epoch, null where it does not exist. */
    private static final class Images {
        final @Nullable BinaryRowData before;
        @Nullable BinaryRowData after;
        boolean started;

        Images(@Nullable BinaryRowData before) {
            this(before, null);
        }

        Images(@Nullable BinaryRowData before, @Nullable BinaryRowData after) {
            this.before = before;
            this.after = after;
        }
    }
}
