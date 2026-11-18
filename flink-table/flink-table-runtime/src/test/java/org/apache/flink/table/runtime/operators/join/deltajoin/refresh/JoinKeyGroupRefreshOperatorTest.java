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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Randomized tests of {@link JoinKeyGroupRefreshOperator} against a model of the storage.
 *
 * <p>Two tables with rows {@code (id, k, v)} are joined on {@code k}. Each table is a log of
 * committed changes. The sources emit a prefix of it, in order, with old images. A lookup returns
 * the table at any position between the last emitted change (at the time the lookup was issued) and
 * the last committed change, so lookups may be stale or read ahead. Lookups, mails and sink writes
 * complete in random order. The sink is an upsert table keyed by {@code (k, left id, right id)}; a
 * checkpoint flushes it. A failure loses the sink writes that were not applied yet, restores the
 * operator from the last checkpoint and rewinds the sources. At the end the sink must hold exactly
 * the join of the final tables.
 */
class JoinKeyGroupRefreshOperatorTest {

    private static final RowType TABLE_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(false), new IntType(), new IntType()},
                    new String[] {"id", "k", "v"});
    private static final RowType OUTPUT_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new IntType(), new IntType(), new IntType(),
                        new IntType(), new IntType(), new IntType()
                    },
                    new String[] {"l_id", "l_k", "l_v", "r_id", "r_k", "r_v"});
    private static final DataType TABLE_DATA_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD("id", DataTypes.INT().notNull()),
                            DataTypes.FIELD("k", DataTypes.INT()),
                            DataTypes.FIELD("v", DataTypes.INT()))
                    .bridgedTo(RowData.class);
    private static final DataType OUTPUT_DATA_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD("l_id", DataTypes.INT()),
                            DataTypes.FIELD("l_k", DataTypes.INT()),
                            DataTypes.FIELD("l_v", DataTypes.INT()),
                            DataTypes.FIELD("r_id", DataTypes.INT()),
                            DataTypes.FIELD("r_k", DataTypes.INT()),
                            DataTypes.FIELD("r_v", DataTypes.INT()))
                    .bridgedTo(RowData.class);

    private static final FlinkJoinType[] JOIN_TYPES = {
        FlinkJoinType.INNER, FlinkJoinType.LEFT, FlinkJoinType.RIGHT, FlinkJoinType.FULL
    };

    @Test
    void testConvergesWithKeyedStateMemory() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.STATE, false, false, false, false);
    }

    @Test
    void testConvergesWithKeyedStateMemoryAndCache() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.STATE, true, false, false, false);
    }

    @Test
    void testConvergesWithRemainingCondition() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.STATE, true, true, false, false);
    }

    @Test
    void testConvergesReadingBack() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.READ_BACK, false, true, true, false);
    }

    @Test
    void testConvergesReadingBackWithFailures() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.READ_BACK, true, true, true, true);
    }

    /**
     * The sink makes every row visible to the lookups that the operator issues after emitting it,
     * so groups are read back at any time, and no checkpoint is needed.
     */
    @Test
    void testConvergesReadingYourWrites() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.READ_BACK, true, true, false, false, true);
    }

    @Test
    void testConvergesReadingYourWritesWithFailures() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.READ_BACK, false, true, true, true, true);
    }

    private void assertConverges(
            int runs,
            GroupRefreshOutputMode mode,
            boolean cache,
            boolean remaining,
            boolean checkpoints,
            boolean failures)
            throws Exception {
        assertConverges(runs, mode, cache, remaining, checkpoints, failures, false);
    }

    /**
     * The right table has at most one row per join key, like a parent: changes of the left rows,
     * its children, refresh only their own rows, read back from keyed state.
     */
    @Test
    void testConvergesRefreshingChangedChildren() throws Exception {
        // no input cache: parent lookups may read changes that were not emitted yet
        assertConverges(600, GroupRefreshOutputMode.STATE, false, true, false, false, false, true);
    }

    /** The same, reading single rows back from the output table by its primary key. */
    @Test
    void testConvergesRefreshingChangedChildrenReadingYourWrites() throws Exception {
        assertConverges(600, GroupRefreshOutputMode.READ_BACK, false, true, true, true, true, true);
    }

    private void assertConverges(
            int runs,
            GroupRefreshOutputMode mode,
            boolean cache,
            boolean remaining,
            boolean checkpoints,
            boolean failures,
            boolean readYourWrites)
            throws Exception {
        assertConverges(runs, mode, cache, remaining, checkpoints, failures, readYourWrites, false);
    }

    private void assertConverges(
            int runs,
            GroupRefreshOutputMode mode,
            boolean cache,
            boolean remaining,
            boolean checkpoints,
            boolean failures,
            boolean readYourWrites,
            boolean parentRight)
            throws Exception {
        List<String> wrong = new ArrayList<>();
        long scoped = 0;
        for (int seed = 0; seed < runs; seed++) {
            FlinkJoinType joinType = JOIN_TYPES[seed % JOIN_TYPES.length];
            Run run =
                    new Run(
                            new Random(seed),
                            joinType,
                            mode,
                            cache,
                            remaining,
                            checkpoints,
                            failures,
                            readYourWrites,
                            parentRight);
            run.execute();
            scoped += run.scopedRefreshes;
            Map<List<Object>, List<Object>> expected = run.expected();
            if (run.duplicateChild != null) {
                wrong.add(String.format("seed %d (%s): %s", seed, joinType, run.duplicateChild));
            }
            if (!expected.equals(run.sink)) {
                wrong.add(
                        String.format(
                                "seed %d (%s): expected %s but sink has %s",
                                seed,
                                joinType,
                                new TreeMap<>(stringKeys(expected)),
                                new TreeMap<>(stringKeys(run.sink))));
            }
        }
        assertThat(wrong).as("runs whose sink differs from the join").isEmpty();
        if (parentRight) {
            assertThat(scoped).as("refreshes of only the changed children").isPositive();
        }
    }

    private static Map<String, List<Object>> stringKeys(Map<List<Object>, List<Object>> map) {
        Map<String, List<Object>> out = new HashMap<>();
        map.forEach((k, v) -> out.put(k.toString(), v));
        return out;
    }

    // ------------------------------------------------------------------------------------------

    /** One randomized run. */
    private static final class Run {
        private final Random rnd;
        private final FlinkJoinType joinType;
        private final GroupRefreshOutputMode mode;
        private final boolean useCache;
        private final boolean remaining;
        private final boolean checkpoints;
        private final boolean failures;
        private final boolean readYourWrites;

        /** Whether the right table has at most one row per join key. */
        private final boolean parentRight;

        private long scopedRefreshes;

        /** The first time the sink held a child twice within its join key, if ever. */
        private @Nullable String duplicateChild;

        /** Per table: the log of committed changes, each {@code [id, k or null, v]} or a delete. */
        private final List<List<Object[]>> logs =
                Arrays.asList(new ArrayList<>(), new ArrayList<>());

        private final int[] committed = new int[2];
        private final int[] emitted = new int[2];

        private final List<PendingLookup> pendingLookups = new ArrayList<>();
        private final List<RowData> sinkChannel = new ArrayList<>();
        private final Map<List<Object>, List<Object>> sink = new HashMap<>();

        private TaskMailbox mailbox;
        private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness;
        private @Nullable OperatorSubtaskState lastCheckpoint;
        private int[] emittedAtCheckpoint;
        private long checkpointId;

        /** A checkpoint whose barrier passed the operator but which did not complete yet. */
        private @Nullable OperatorSubtaskState inFlightCheckpoint;

        private int[] emittedAtBarrier;
        private long outputsBeforeBarrier;
        private long outputsCollected;
        private long outputsApplied;
        private int failuresLeft;
        private int harnessGeneration;

        /** Sink writes are applied with probability 1 / sinkSlowness per step. */
        private final int sinkSlowness;

        Run(
                Random rnd,
                FlinkJoinType joinType,
                GroupRefreshOutputMode mode,
                boolean useCache,
                boolean remaining,
                boolean checkpoints,
                boolean failures,
                boolean readYourWrites,
                boolean parentRight) {
            this.rnd = rnd;
            this.readYourWrites = readYourWrites;
            this.parentRight = parentRight;
            this.joinType = joinType;
            this.mode = mode;
            this.useCache = useCache;
            this.remaining = remaining;
            this.checkpoints = checkpoints;
            this.failures = failures;
            this.failuresLeft = failures ? 1 + rnd.nextInt(2) : 0;
            this.sinkSlowness = 1 + rnd.nextInt(8);
            generateLogs();
        }

        private void generateLogs() {
            boolean nulls = rnd.nextInt(3) == 0;
            int length = 4 + rnd.nextInt(26);
            List<Map<Integer, Object[]>> live = Arrays.asList(new HashMap<>(), new HashMap<>());
            for (int i = 0; i < length; i++) {
                int t = rnd.nextInt(2);
                Map<Integer, Object[]> rows = live.get(t);
                if (!rows.isEmpty() && rnd.nextInt(10) < 3) {
                    Integer id = new ArrayList<>(rows.keySet()).get(rnd.nextInt(rows.size()));
                    logs.get(t).add(new Object[] {id, null, null, true});
                    rows.remove(id);
                    continue;
                }
                int id = 1 + rnd.nextInt(5);
                Integer k = nulls && rnd.nextInt(7) == 0 ? null : 1 + rnd.nextInt(3);
                if (parentRight && t == 1) {
                    // a parent per join key: its id is its join key
                    id = 1 + rnd.nextInt(3);
                    k = id;
                }
                Object[] row = {id, k, rnd.nextInt(10), false};
                logs.get(t).add(row);
                rows.put(id, row);
            }
        }

        // ---------------------------------------------------------------- storage model

        private @Nullable Object[] rowAt(int t, int id, int position) {
            Object[] found = null;
            for (int p = 0; p < position; p++) {
                Object[] change = logs.get(t).get(p);
                if ((int) change[0] == id) {
                    found = (boolean) change[3] ? null : change;
                }
            }
            return found;
        }

        private List<Object[]> tableAt(int t, int position) {
            Map<Integer, Object[]> rows = new TreeMap<>();
            for (int p = 0; p < position; p++) {
                Object[] change = logs.get(t).get(p);
                if ((boolean) change[3]) {
                    rows.remove((int) change[0]);
                } else {
                    rows.put((int) change[0], change);
                }
            }
            return new ArrayList<>(rows.values());
        }

        private static RowData toRow(Object[] r, RowKind kind) {
            GenericRowData row = GenericRowData.of(r[0], r[1], r[2]);
            row.setRowKind(kind);
            return row;
        }

        // ---------------------------------------------------------------- operator

        private void startOperator(@Nullable OperatorSubtaskState restoreFrom) throws Exception {
            mailbox = new TaskMailboxImpl();
            MailboxProcessor processor =
                    new MailboxProcessor(c -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);
            MailboxExecutor executor =
                    new MailboxExecutorImpl(
                            mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, processor);
            int generation = ++harnessGeneration;
            RowDataKeySelector joinKey = selector(new int[] {1}, TABLE_TYPE);
            GroupRefreshSpec.Builder spec =
                    GroupRefreshSpec.builder(
                                    new RowType[] {TABLE_TYPE, TABLE_TYPE},
                                    // a parent is identified by its join key
                                    new int[][] {{0}, {parentRight ? 1 : 0}},
                                    new int[] {0, 0, 0, 1, 1, 1},
                                    new int[] {0, 1, 2, 0, 1, 2},
                                    OUTPUT_TYPE,
                                    mode)
                            .capacity(1 + rnd.nextInt(4), 1_000_000)
                            .maxUnpinnedGroups(rnd.nextInt(3))
                            .lookupTimeoutMs(60_000L)
                            .emitUpdateBefore(rnd.nextBoolean());
            if (mode == GroupRefreshOutputMode.READ_BACK) {
                // the key of the output table holds the join key, so that it is unique in the table
                spec.readBack(selector(outputKey(), OUTPUT_TYPE), outputLookup(generation))
                        .readYourWrites(readYourWrites);
                if (readYourWrites) {
                    spec.readBackRows(rowLookup(generation), keyProjection());
                }
            }
            JoinKeyGroupSpec groups =
                    new JoinKeyGroupSpec(
                            joinType,
                            TABLE_TYPE,
                            TABLE_TYPE,
                            remaining ? remainingCondition() : null,
                            tableLookup(0, generation),
                            tableLookup(1, generation),
                            selector(new int[] {0}, TABLE_TYPE),
                            selector(new int[] {parentRight ? 1 : 0}, TABLE_TYPE),
                            useCache ? 100 : 0,
                            useCache ? 100 : 0,
                            parentRight ? 0 : -1,
                            new int[] {0},
                            new int[] {1},
                            new int[] {1});
            JoinKeyGroupRefreshOperator operator =
                    new JoinKeyGroupRefreshOperator(spec.build(), groups, executor);
            harness =
                    new KeyedTwoInputStreamOperatorTestHarness<>(
                            operator,
                            joinKey,
                            joinKey,
                            joinKey.getProducedType(),
                            1,
                            1,
                            0,
                            InternalTypeInfo.of(TABLE_TYPE).toSerializer(),
                            InternalTypeInfo.of(TABLE_TYPE).toSerializer());
            harness.getEnvironment()
                    .getJobConfiguration()
                    .set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
            if (restoreFrom != null) {
                harness.initializeState(restoreFrom);
            }
            harness.open();
        }

        private GroupLookup tableLookup(int table, int generation) {
            return new GroupLookup(
                    fetcher(table, generation),
                    TABLE_DATA_TYPE,
                    null,
                    new RowDataSerializer(TABLE_TYPE),
                    selector(new int[] {1}, TABLE_TYPE));
        }

        /**
         * The primary key of the output table: the ids and join keys of both sides, where a parent
         * is identified by its join key.
         */
        private int[] outputKey() {
            return parentRight ? new int[] {0, 1, 4} : new int[] {0, 1, 3, 4};
        }

        /** The key of the output from a joined row. */
        private GeneratedFunction<FlatMapFunction<RowData, RowData>> keyProjection() {
            return new GeneratedFunction<FlatMapFunction<RowData, RowData>>("", "", new Object[0]) {
                @Override
                public FlatMapFunction<RowData, RowData> newInstance(ClassLoader classLoader) {
                    return (row, out) -> {
                        Object[] key = new Object[outputKey().length];
                        int[] fields = outputKey();
                        for (int i = 0; i < fields.length; i++) {
                            key[i] = row.isNullAt(fields[i]) ? null : row.getInt(fields[i]);
                        }
                        out.collect(GenericRowData.of(key));
                    };
                }
            };
        }

        /** Reads a row of the output by its key: the ids and join keys of both sides. */
        private GroupLookup rowLookup(int generation) {
            return new GroupLookup(
                    fetcher(-2, generation),
                    OUTPUT_DATA_TYPE,
                    null,
                    new RowDataSerializer(OUTPUT_TYPE),
                    selector(outputKey(), OUTPUT_TYPE));
        }

        private GroupLookup outputLookup(int generation) {
            return new GroupLookup(
                    fetcher(-1, generation),
                    OUTPUT_DATA_TYPE,
                    null,
                    new RowDataSerializer(OUTPUT_TYPE),
                    new OwnerKeySelector());
        }

        private GeneratedFunction<AsyncFunction<RowData, Object>> fetcher(
                int table, int generation) {
            Run run = this;
            return new GeneratedFunction<AsyncFunction<RowData, Object>>("", "", new Object[0]) {
                @Override
                public AsyncFunction<RowData, Object> newInstance(ClassLoader classLoader) {
                    return new RichAsyncFunction<RowData, Object>() {
                        @Override
                        public void open(OpenContext openContext) {}

                        @Override
                        public void asyncInvoke(RowData key, ResultFuture<Object> future) {
                            if (table < 0 && run.readYourWrites) {
                                // the sink makes what was emitted so far visible first
                                run.collectOutput();
                                run.applySink(Integer.MAX_VALUE);
                            }
                            run.pendingLookups.add(
                                    new PendingLookup(
                                            table,
                                            key.isNullAt(0) ? null : key.getInt(0),
                                            table == -2 ? fields(key) : null,
                                            table < 0 ? 0 : run.emitted[table],
                                            future,
                                            generation));
                        }
                    };
                }
            };
        }

        private GeneratedFilterCondition remainingCondition() {
            return new GeneratedFilterCondition("", "", new Object[0]) {
                @Override
                public FilterCondition newInstance(ClassLoader classLoader) {
                    return new ResidualCondition();
                }
            };
        }

        // ---------------------------------------------------------------- driver

        void execute() throws Exception {
            startOperator(null);
            int steps = 0;
            while (true) {
                if (++steps > 200_000) {
                    throw new IllegalStateException("no progress");
                }
                collectOutput();
                List<Runnable> options = new ArrayList<>();
                for (int t = 0; t < 2; t++) {
                    int table = t;
                    if (committed[t] < logs.get(t).size()) {
                        options.add(
                                () ->
                                        committed[table] =
                                                Math.min(
                                                        logs.get(table).size(),
                                                        committed[table] + 1 + rnd.nextInt(2)));
                    }
                    if (emitted[t] < committed[t]) {
                        options.add(() -> emit(table));
                    }
                }
                if (!pendingLookups.isEmpty()) {
                    options.add(this::completeLookup);
                }
                if (mailbox.hasMail()) {
                    options.add(this::runMail);
                }
                if (!sinkChannel.isEmpty() && (rnd.nextInt(sinkSlowness) == 0 || !anyOther())) {
                    options.add(() -> applySink(1));
                }
                if (checkpoints && inFlightCheckpoint == null && rnd.nextInt(30) == 0) {
                    options.add(this::checkpointBarrier);
                }
                if (inFlightCheckpoint != null) {
                    options.add(this::completeCheckpoint);
                }
                if (failuresLeft > 0 && lastCheckpoint != null && rnd.nextInt(40) == 0) {
                    options.add(this::fail);
                }
                if (options.isEmpty()) {
                    if (failuresLeft > 0 && lastCheckpoint != null) {
                        fail();
                        continue;
                    }
                    break;
                }
                options.get(rnd.nextInt(options.size())).run();
            }
            assertThat(harness.getOperator() instanceof JoinKeyGroupRefreshOperator).isTrue();
            assertThat(
                            ((JoinKeyGroupRefreshOperator) harness.getOperator())
                                    .getScheduler()
                                    .isIdle())
                    .isTrue();
            scopedRefreshes =
                    ((JoinKeyGroupRefreshOperator) harness.getOperator()).getScopedRefreshes();
            harness.close();
        }

        /** Whether anything but applying sink writes can happen. */
        private boolean anyOther() {
            return committed[0] < logs.get(0).size()
                    || committed[1] < logs.get(1).size()
                    || emitted[0] < committed[0]
                    || emitted[1] < committed[1]
                    || !pendingLookups.isEmpty()
                    || mailbox.hasMail()
                    || inFlightCheckpoint != null;
        }

        private void emit(int t) {
            int p = emitted[t]++;
            Object[] change = logs.get(t).get(p);
            Object[] old = rowAt(t, (int) change[0], p);
            boolean delete = (boolean) change[3];
            try {
                if (old != null) {
                    process(t, toRow(old, delete ? RowKind.DELETE : RowKind.UPDATE_BEFORE));
                }
                if (!delete) {
                    process(t, toRow(change, old == null ? RowKind.INSERT : RowKind.UPDATE_AFTER));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void process(int t, RowData row) throws Exception {
            if (t == 0) {
                harness.processElement1(new StreamRecord<>(row));
            } else {
                harness.processElement2(new StreamRecord<>(row));
            }
        }

        private void completeLookup() {
            PendingLookup lookup = pendingLookups.remove(rnd.nextInt(pendingLookups.size()));
            List<Object> result = new ArrayList<>();
            if (lookup.table == -2) {
                for (List<Object> row : sink.values()) {
                    List<Object> rowKey = new ArrayList<>();
                    for (int field : outputKey()) {
                        rowKey.add(row.get(field));
                    }
                    if (rowKey.equals(lookup.rowKey)) {
                        result.add(GenericRowData.of(row.toArray()));
                    }
                }
            } else if (lookup.table < 0) {
                for (List<Object> row : sink.values()) {
                    if (Objects.equals(ownerKey(row), lookup.key)) {
                        result.add(GenericRowData.of(row.toArray()));
                    }
                }
            } else {
                int position =
                        lookup.bound + rnd.nextInt(committed[lookup.table] - lookup.bound + 1);
                for (Object[] row : tableAt(lookup.table, position)) {
                    if (Objects.equals(row[1], lookup.key)) {
                        result.add(toRow(row, RowKind.INSERT));
                    }
                }
            }
            lookup.future.complete(result);
        }

        private void runMail() {
            try {
                Optional<Mail> mail = mailbox.tryTake(TaskMailbox.MIN_PRIORITY);
                if (mail.isPresent()) {
                    mail.get().run();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void collectOutput() {
            Object element;
            while ((element = harness.getOutput().poll()) != null) {
                if (element instanceof StreamRecord) {
                    sinkChannel.add((RowData) ((StreamRecord<?>) element).getValue());
                    outputsCollected++;
                }
            }
        }

        private void applySink(int n) {
            for (int i = 0; i < n && !sinkChannel.isEmpty(); i++) {
                RowData row = sinkChannel.remove(0);
                outputsApplied++;
                List<Object> values = new ArrayList<>();
                for (int f = 0; f < 6; f++) {
                    values.add(row.isNullAt(f) ? null : row.getInt(f));
                }
                List<Object> key = Arrays.asList(ownerKey(values), values.get(0), values.get(3));
                switch (row.getRowKind()) {
                    case INSERT:
                    case UPDATE_AFTER:
                        sink.put(key, values);
                        break;
                    case DELETE:
                        sink.remove(key);
                        break;
                    default:
                        // an upsert sink ignores update-before
                }
                if (parentRight && !failures && duplicateChild == null) {
                    duplicateChild = duplicateChild();
                }
            }
        }

        /**
         * A child that the sink holds twice within its join key, for example matched with the
         * parent and padded at once; null if none. With one parent per join key, every refresh
         * replaces the rows of a child, deletes first, so the sink never holds both.
         */
        private @Nullable String duplicateChild() {
            Map<List<Object>, Integer> children = new HashMap<>();
            for (List<Object> key : sink.keySet()) {
                if (key.get(0) != null && key.get(1) != null) {
                    List<Object> child = Arrays.asList(key.get(0), key.get(1));
                    if (children.merge(child, 1, Integer::sum) > 1) {
                        return "join key " + key.get(0) + ", child " + key.get(1) + " in " + sink;
                    }
                }
            }
            return null;
        }

        private void checkpointBarrier() {
            try {
                checkpointId++;
                inFlightCheckpoint = harness.snapshot(checkpointId, checkpointId);
                collectOutput();
                outputsBeforeBarrier = outputsCollected;
                emittedAtBarrier = emitted.clone();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void completeCheckpoint() {
            try {
                // the sink flushed every write it received before the barrier
                applySink((int) (outputsBeforeBarrier - outputsApplied));
                harness.notifyOfCompletedCheckpoint(checkpointId);
                lastCheckpoint = inFlightCheckpoint;
                emittedAtCheckpoint = emittedAtBarrier;
                inFlightCheckpoint = null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void fail() {
            try {
                failuresLeft--;
                // writes the failed attempt did not apply yet never land (they are fenced)
                collectOutput();
                outputsApplied += sinkChannel.size();
                outputsCollected = outputsApplied;
                sinkChannel.clear();
                pendingLookups.clear();
                inFlightCheckpoint = null;
                harness.close();
                System.arraycopy(emittedAtCheckpoint, 0, emitted, 0, 2);
                startOperator(lastCheckpoint);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // ---------------------------------------------------------------- expectation

        Map<List<Object>, List<Object>> expected() {
            List<Object[]> left = tableAt(0, logs.get(0).size());
            List<Object[]> right = tableAt(1, logs.get(1).size());
            Map<List<Object>, List<Object>> out = new HashMap<>();
            boolean[] rightMatched = new boolean[right.size()];
            boolean keepLeft = joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
            boolean keepRight = joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
            for (Object[] l : left) {
                boolean matched = false;
                for (int i = 0; i < right.size(); i++) {
                    Object[] r = right.get(i);
                    if (l[1] != null
                            && l[1].equals(r[1])
                            && (!remaining || residual((Integer) l[2], (Integer) r[2]))) {
                        put(out, l, r);
                        matched = true;
                        rightMatched[i] = true;
                    }
                }
                if (!matched && keepLeft) {
                    put(out, l, null);
                }
            }
            for (int i = 0; i < right.size(); i++) {
                if (!rightMatched[i] && keepRight) {
                    put(out, null, right.get(i));
                }
            }
            return out;
        }

        private static void put(
                Map<List<Object>, List<Object>> out, @Nullable Object[] l, @Nullable Object[] r) {
            List<Object> values =
                    Arrays.asList(
                            l == null ? null : l[0],
                            l == null ? null : l[1],
                            l == null ? null : l[2],
                            r == null ? null : r[0],
                            r == null ? null : r[1],
                            r == null ? null : r[2]);
            out.put(Arrays.asList(ownerKey(values), values.get(0), values.get(3)), values);
        }
    }

    private static boolean residual(int leftValue, int rightValue) {
        return (leftValue + rightValue) % 3 != 0;
    }

    private static List<Object> fields(RowData row) {
        List<Object> fields = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            fields.add(row.isNullAt(i) ? null : row.getInt(i));
        }
        return fields;
    }

    private static @Nullable Object ownerKey(List<Object> outputRow) {
        return outputRow.get(1) != null ? outputRow.get(1) : outputRow.get(4);
    }

    private static RowDataKeySelector selector(int[] fields, RowType type) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                fields, type.getChildren().toArray(new LogicalType[0]));
    }

    /** The remaining join condition of the tests, on the values of both rows. */
    private static final class ResidualCondition extends AbstractRichFunction
            implements FilterCondition {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean apply(Context ctx, RowData in) {
            return residual(in.getInt(2), in.getInt(5));
        }
    }

    /** Selects the join key of an output row: the left one, or the right one if padded. */
    private static final class OwnerKeySelector implements RowDataKeySelector {
        private static final long serialVersionUID = 1L;
        private static final RowType KEY_TYPE =
                RowType.of(new LogicalType[] {new IntType()}, new String[] {"k"});
        private final RowDataSerializer serializer = new RowDataSerializer(KEY_TYPE);

        @Override
        public RowData getKey(RowData row) {
            GenericRowData key =
                    GenericRowData.of(
                            row.isNullAt(1)
                                    ? (row.isNullAt(4) ? null : row.getInt(4))
                                    : row.getInt(1));
            return serializer.toBinaryRow(key).copy();
        }

        @Override
        public InternalTypeInfo<RowData> getProducedType() {
            return InternalTypeInfo.of(KEY_TYPE);
        }

        @Override
        public RowDataKeySelector copy() {
            return new OwnerKeySelector();
        }
    }

    private static final class PendingLookup {
        private final int table;
        private final @Nullable Integer key;
        private final @Nullable List<Object> rowKey;
        private final int bound;
        private final ResultFuture<Object> future;
        private final int generation;

        private PendingLookup(
                int table,
                @Nullable Integer key,
                @Nullable List<Object> rowKey,
                int bound,
                ResultFuture<Object> future,
                int generation) {
            this.table = table;
            this.key = key;
            this.rowKey = rowKey;
            this.bound = bound;
            this.future = future;
            this.generation = generation;
        }
    }
}
