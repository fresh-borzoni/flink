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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the locate operator for the input B of {@code A LEFT JOIN B ON A.k = B.k}, anchored on
 * A. The test decides when every lookup completes.
 */
class AnchorLocateOperatorTest {

    private static final RowType ROW = RowType.of(new IntType(), new IntType());

    private static final byte ADD = AnchorNotes.ADD;
    private static final byte RETRACT = AnchorNotes.RETRACT;

    /** Notes without rows: the anchor key, the table and the change. */
    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {new IntType(), new IntType(), new TinyIntType()});

    @Test
    void testBothImagesOfAnUpdateLocateTheirAnchors() throws Exception {
        ControllableLookup aByK = new ControllableLookup();
        OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness(aByK);
        harness.open();

        // B(10) moves from k = 1 to k = 2: A(1) loses its partner, A(2) gains one
        harness.processElement(record(RowKind.UPDATE_BEFORE, 10, 1));
        harness.processElement(record(RowKind.UPDATE_AFTER, 10, 2));
        runMails(harness);
        assertThat(aByK.keys()).containsExactly(1, 2);
        aByK.completeNext(List.of(GenericRowData.of(1, 1)));
        aByK.completeNext(List.of(GenericRowData.of(2, 2)));
        runMails(harness);

        assertor.assertOutputEqualsSorted(
                "the old image reaches the old partner",
                List.of(insertRecord(1, 1, RETRACT), insertRecord(2, 1, ADD)),
                harness.getOutput());
        harness.close();
    }

    @Test
    void testRowsInFlightSurviveCheckpoints() throws Exception {
        ControllableLookup aByK = new ControllableLookup();
        OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness(aByK);
        harness.open();
        harness.processElement(record(RowKind.DELETE, 10, 1));
        runMails(harness);
        assertThat(aByK.keys()).containsExactly(1);
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        // abandon the failed attempt: closing it would wait for its lookup forever

        ControllableLookup restoredLookup = new ControllableLookup();
        OneInputStreamOperatorTestHarness<RowData, RowData> restored =
                createHarness(restoredLookup);
        restored.initializeState(snapshot);
        restored.open();
        runMails(restored);
        assertThat(restoredLookup.keys()).as("the deleted row is located again").containsExactly(1);
        restoredLookup.completeNext(List.of(GenericRowData.of(1, 1)));
        runMails(restored);

        assertor.assertOutputEquals(
                "the anchor of the deleted row",
                List.of(insertRecord(1, 1, RETRACT)),
                restored.getOutput());
        restored.close();
    }

    /** Rows in flight on two subtasks are located again after restoring with one subtask. */
    @Test
    void testRowsInFlightAreRedistributedOnRescaling() throws Exception {
        List<OperatorSubtaskState> snapshots = new ArrayList<>();
        for (int subtask = 0; subtask < 2; subtask++) {
            OneInputStreamOperatorTestHarness<RowData, RowData> before =
                    createHarness(new ControllableLookup(), 128, 2, subtask);
            before.open();
            before.processElement(record(RowKind.DELETE, 10 + subtask, subtask + 1));
            runMails(before);
            snapshots.add(before.snapshot(1L, 1L));
            // abandoned: its lookup never completes
        }
        OperatorSubtaskState merged =
                AbstractStreamOperatorTestHarness.repackageState(
                        snapshots.toArray(new OperatorSubtaskState[0]));

        ControllableLookup aByK = new ControllableLookup();
        OneInputStreamOperatorTestHarness<RowData, RowData> after = createHarness(aByK, 128, 1, 0);
        after.initializeState(
                AbstractStreamOperatorTestHarness.repartitionOperatorState(merged, 128, 2, 1, 0));
        after.open();
        runMails(after);
        assertThat(aByK.keys()).containsExactlyInAnyOrder(1, 2);
    }

    /**
     * With {@code A LEFT JOIN B ON A.id = B.k}, the anchor key is B.k: B's rows give it without a
     * lookup of A, also for an anchor row that does not exist. A NULL key reaches no anchor.
     */
    @Test
    void testAnchorKeyIsTakenFromTheRowWhenTheJoinCoversIt() throws Exception {
        ControllableLookup aByK = new ControllableLookup();
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(aByK, 0, 1, 1, 0);
        harness.open();
        harness.processElement(record(RowKind.UPDATE_BEFORE, 10, 1));
        harness.processElement(record(RowKind.UPDATE_AFTER, 10, 2));
        GenericRowData nullKey = GenericRowData.of(11, null);
        nullKey.setRowKind(RowKind.INSERT);
        harness.processElement(new StreamRecord<>(nullKey));
        runMails(harness);

        assertThat(aByK.keys()).as("no lookup of A").isEmpty();
        assertor.assertOutputEqualsSorted(
                "the keys of both images",
                List.of(insertRecord(1, 1, RETRACT), insertRecord(2, 1, ADD)),
                harness.getOutput());
        harness.close();
    }

    /**
     * B's rows name their anchor, so B's changes refresh only their own lines: the notes carry the
     * rows, in input order. The old row of an update that stays in its group is marked as replaced.
     */
    @Test
    void testNotesCarryTheRowsOfTablesThatRefreshTheirOwnLines() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(new ControllableLookup(), 0, 1, 1, 0, true);
        harness.open();
        // B(10) moves from A(1) to A(2), then B(11) changes within A(3)
        harness.processElement(record(RowKind.UPDATE_BEFORE, 10, 1));
        harness.processElement(record(RowKind.UPDATE_AFTER, 10, 2));
        harness.processElement(record(RowKind.UPDATE_BEFORE, 11, 3));
        harness.processElement(record(RowKind.UPDATE_AFTER, 11, 3));
        runMails(harness);

        List<RowData> notes = new ArrayList<>();
        for (Object record : harness.getOutput()) {
            notes.add(((StreamRecord<RowData>) record).getValue());
        }
        assertThat(notes).hasSize(4);
        assertNote(notes.get(0), 1, RETRACT, 10, 1);
        assertNote(notes.get(1), 2, ADD, 10, 2);
        assertNote(notes.get(2), 3, AnchorNotes.REPLACED, 11, 3);
        assertNote(notes.get(3), 3, ADD, 11, 3);
        harness.close();
    }

    /** The old row of an update that waits for the new one is sent before a barrier. */
    @Test
    void testOldRowOfAnUpdateIsSentBeforeTheBarrier() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(new ControllableLookup(), 0, 1, 1, 0, true);
        harness.open();
        harness.processElement(record(RowKind.UPDATE_BEFORE, 10, 1));
        assertThat(harness.getOutput()).as("waits for the new row").isEmpty();

        harness.prepareSnapshotPreBarrier(1L);
        assertThat(harness.getOutput()).hasSize(1);
        assertNote(notes(harness).get(0), 1, RETRACT, 10, 1);
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        harness.close();

        OneInputStreamOperatorTestHarness<RowData, RowData> restored =
                createHarness(new ControllableLookup(), 0, 1, 1, 0, true);
        restored.initializeState(snapshot);
        restored.open();
        runMails(restored);
        assertThat(restored.getOutput()).isEmpty();
        restored.close();
    }

    /**
     * Only the two rows of one update are noted together; a new row without its old one carries no
     * row, so that its group is refreshed whole.
     */
    @Test
    void testOnlyRowsWithTheSameKeyArePaired() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(new ControllableLookup(), 0, 1, 1, 0, true);
        harness.open();
        harness.processElement(record(RowKind.UPDATE_BEFORE, 10, 1));
        harness.processElement(record(RowKind.UPDATE_AFTER, 11, 1));
        runMails(harness);

        List<RowData> notes = notes(harness);
        assertThat(notes).hasSize(2);
        assertNote(notes.get(0), 1, RETRACT, 10, 1);
        assertThat(notes.get(1).getInt(0)).isEqualTo(1);
        assertThat(notes.get(1).getByte(2)).isEqualTo(ADD);
        assertThat(notes.get(1).isNullAt(3)).as("the old row is unknown").isTrue();
        harness.close();
    }

    /** With a join on equalities only, a new row without its old one replaces its own lines. */
    @Test
    void testNewRowAloneCarriesItsRowWithEqualityJoin() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(new ControllableLookup(), 0, 1, 1, 0, true, true);
        harness.open();
        harness.processElement(record(RowKind.UPDATE_AFTER, 11, 1));
        runMails(harness);

        List<RowData> notes = notes(harness);
        assertThat(notes).hasSize(1);
        assertNote(notes.get(0), 1, ADD, 11, 1);
        harness.close();
    }

    private static List<RowData> notes(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        List<RowData> notes = new ArrayList<>();
        for (Object record : harness.getOutput()) {
            notes.add(((StreamRecord<RowData>) record).getValue());
        }
        return notes;
    }

    private static void assertNote(RowData note, int anchorKey, byte change, int id, int k) {
        assertThat(note.getInt(0)).isEqualTo(anchorKey);
        assertThat(note.getByte(2)).isEqualTo(change);
        assertThat(note.getRow(3, 2).getInt(0)).isEqualTo(id);
        assertThat(note.getRow(3, 2).getInt(1)).isEqualTo(k);
    }

    // --------------------------------------------------------------------------------------------

    private static StreamRecord<RowData> record(RowKind kind, int id, int k) {
        GenericRowData row = GenericRowData.of(id, k);
        row.setRowKind(kind);
        return new StreamRecord<>(row);
    }

    private static void runMails(OneInputStreamOperatorTestHarness<RowData, RowData> harness)
            throws Exception {
        TaskMailbox mailbox = harness.getTaskMailbox();
        Optional<Mail> mail;
        while ((mail = mailbox.tryTake(TaskMailbox.MIN_PRIORITY)).isPresent()) {
            mail.get().run();
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            ControllableLookup aByK) throws Exception {
        return createHarness(aByK, 1, 1, 0);
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            ControllableLookup aByK, int maxParallelism, int parallelism, int subtask)
            throws Exception {
        return createHarness(aByK, 1, maxParallelism, parallelism, subtask);
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            ControllableLookup aByK, int aField, int maxParallelism, int parallelism, int subtask)
            throws Exception {
        return createHarness(aByK, aField, maxParallelism, parallelism, subtask, false);
    }

    /**
     * {@code A LEFT JOIN B ON A.f = B.k}, where {@code f} is field {@code aField} of A; the notes
     * of B carry its rows if {@code withRows} is set.
     */
    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            ControllableLookup aByK,
            int aField,
            int maxParallelism,
            int parallelism,
            int subtask,
            boolean withRows)
            throws Exception {
        return createHarness(aByK, aField, maxParallelism, parallelism, subtask, withRows, false);
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            ControllableLookup aByK,
            int aField,
            int maxParallelism,
            int parallelism,
            int subtask,
            boolean withRows,
            boolean equalityJoin)
            throws Exception {
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[3];
        nodes[0] = AnchorJoinTree.Node.leaf(0, 2, 2);
        nodes[1] = AnchorJoinTree.Node.leaf(1, 2, 2);
        nodes[2] =
                AnchorJoinTree.Node.join(
                        0,
                        1,
                        nodes[0],
                        nodes[1],
                        FlinkJoinType.LEFT,
                        0,
                        nodes[0].getOutput().concat(nodes[1].getOutput()),
                        Collections.emptyList());
        AnchorJoinTree tree =
                new AnchorJoinTree(
                        new RowType[] {ROW, ROW},
                        nodes,
                        new AnchorJoinTree.Edge[] {
                            new AnchorJoinTree.Edge(0, new int[] {aField}, 1, new int[] {1}, 1, 2)
                        },
                        0,
                        new int[] {0},
                        0);
        AnchorLookupSpec[] lookups = {
            lookupSpec(new ControllableLookup(), new int[] {0}, "A by id"),
            lookupSpec(new ControllableLookup(), new int[] {aField}, "B by A.f"),
            lookupSpec(aByK, new int[] {1}, "A by B.k")
        };
        return new OneInputStreamOperatorTestHarness<>(
                new AnchorLocateOperatorFactory(
                        tree,
                        lookups,
                        1,
                        ROW,
                        0,
                        16,
                        AnchorNotes.of(tree, new boolean[] {false, withRows}),
                        withRows ? new int[] {0} : new int[0],
                        equalityJoin,
                        0),
                maxParallelism,
                parallelism,
                subtask);
    }

    private static AnchorLookupSpec lookupSpec(
            ControllableLookup lookup, int[] keyFields, String description) {
        GeneratedFunction<AsyncFunction<RowData, Object>> fetcher =
                new GeneratedFunction<>(description, "", new Object[0]) {
                    @Override
                    public AsyncFunction<RowData, Object> newInstance(ClassLoader classLoader) {
                        return lookup;
                    }
                };
        return new AnchorLookupSpec(
                fetcher,
                TypeConversions.fromLogicalToDataType(ROW).bridgedTo(RowData.class),
                null,
                ROW,
                keyFields,
                new LogicalType[] {new IntType()},
                description);
    }

    /** A lookup whose requests complete only when the test says so, in request order. */
    private static final class ControllableLookup extends AbstractRichFunction
            implements AsyncFunction<RowData, Object> {

        private final List<RowData> inputs = new ArrayList<>();
        private final List<ResultFuture<Object>> futures = new ArrayList<>();

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) {
            inputs.add(input);
            futures.add(resultFuture);
        }

        List<Integer> keys() {
            List<Integer> keys = new ArrayList<>();
            for (RowData input : inputs) {
                keys.add(input.getInt(1));
            }
            return keys;
        }

        void completeNext(Collection<? extends RowData> rows) {
            inputs.remove(0);
            futures.remove(0).complete(new ArrayList<>(rows));
        }
    }
}
