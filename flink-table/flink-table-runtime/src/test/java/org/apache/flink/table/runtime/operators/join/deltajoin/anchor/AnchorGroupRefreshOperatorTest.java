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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOutputMode;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic tests of the group refresh operator of anchor groups for {@code A LEFT JOIN B ON
 * A.k = B.k}, anchored on {@code A(id, k)}. The test decides when every lookup completes and what
 * state it reads, to produce the interleavings that matter.
 */
class AnchorGroupRefreshOperatorTest {

    private static final RowType ROW = RowType.of(new IntType(), new IntType());
    private static final RowType KEY = RowType.of(new IntType());
    private static final RowType OUTPUT =
            RowType.of(new IntType(), new IntType(), new IntType(), new IntType());

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(OUTPUT.getChildren().toArray(new LogicalType[0]));

    private ControllableLookup anchorLookup;
    private ControllableLookup lookupB;
    private ControllableLookup lookupA;

    private final TreeMap<Integer, List<GenericRowData>> tableA = new TreeMap<>();
    private final TreeMap<Integer, List<GenericRowData>> tableB = new TreeMap<>();

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;

    @BeforeEach
    void setup() {
        anchorLookup = new ControllableLookup();
        lookupB = new ControllableLookup();
        lookupA = new ControllableLookup();
    }

    @AfterEach
    void close() throws Exception {
        // a harness with work in flight is abandoned: closing it would wait for its lookups
        if (harness != null && operator().getScheduler().numRunning() == 0) {
            harness.close();
        }
    }

    @Test
    void testNoteDuringRecomputeTriggersAnotherRecompute() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);

        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        // the recompute now waits for B; B gets a matching row and a note
        putB(10, 7);
        harness.processElement(note(1));
        // but the running recompute read B before the insert
        lookupB.completeNext(Collections.emptyList());
        runMails();
        assertThat(operator().getScheduler().numRunning())
                .as("dirty key is recomputed")
                .isEqualTo(1);
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(tableB.get(7));
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertor.assertOutputEqualsSorted(
                "the last recompute must see the new row",
                List.of(
                        insertRecord(1, 7, null, null),
                        deleteRecord(1, 7, null, null),
                        insertRecord(1, 7, 10, 7)),
                harness.getOutput());
    }

    @Test
    void testGroupReplacementCountsDuplicateRows() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);

        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        // two identical rows of B (a table without a primary key)
        lookupB.completeNext(List.of(row(10, 7), row(10, 7)));
        runMails();

        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(List.of(row(10, 7)));
        runMails();

        assertor.assertOutputEquals(
                "one of the two equal rows is retracted",
                List.of(
                        insertRecord(1, 7, 10, 7),
                        insertRecord(1, 7, 10, 7),
                        deleteRecord(1, 7, 10, 7)),
                harness.getOutput());
    }

    @Test
    void testUpsertModeDeletesByAnchorKey() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.UPSERT);
        harness.open();
        putA(1, 7);

        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(Collections.emptyList());
        runMails();

        // the anchor row is deleted
        tableA.remove(1);
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(Collections.emptyList());
        runMails();

        assertor.assertOutputEquals(
                "upsert, then a delete that only carries the anchor key",
                List.of(updateAfterRecord(1, 7, null, null), deleteRecord(1, null, null, null)),
                harness.getOutput());
    }

    @Test
    void testPendingKeysSurviveCheckpoints() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);
        putA(2, 8);

        // both recomputes are running when the checkpoint is taken
        harness.processElement(note(1));
        harness.processElement(note(2));
        runMails();
        assertThat(anchorLookup.pending()).isEqualTo(2);
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        // abandon the failed attempt: closing it would wait for its lookups forever

        anchorLookup = new ControllableLookup();
        lookupB = new ControllableLookup();
        lookupA = new ControllableLookup();
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.initializeState(snapshot);
        harness.open();
        runMails();

        assertThat(anchorLookup.pending()).as("both keys are recomputed").isEqualTo(2);
        while (anchorLookup.pending() > 0 || lookupB.pending() > 0) {
            if (anchorLookup.pending() > 0) {
                int key = anchorLookup.nextKey();
                anchorLookup.completeNext(tableA.get(key));
            } else {
                lookupB.completeNext(Collections.emptyList());
            }
            runMails();
        }
        assertThat(operator().getScheduler().numRunning()).isZero();
        assertor.assertOutputEqualsSorted(
                "both groups are emitted after the restore",
                List.of(insertRecord(1, 7, null, null), insertRecord(2, 8, null, null)),
                harness.getOutput());
    }

    /**
     * Keys pending on two subtasks are recomputed after restoring with three subtasks, each exactly
     * once and on the subtask that owns it.
     */
    @Test
    void testPendingKeysAreRecomputedAfterRescaling() throws Exception {
        int maxParallelism = 128;
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, KEY.getChildren().toArray(new LogicalType[0]));
        List<OperatorSubtaskState> snapshots = new ArrayList<>();
        for (int subtask = 0; subtask < 2; subtask++) {
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> before =
                    createHarness(
                            GroupRefreshOutputMode.STATE,
                            new ControllableLookup(),
                            new ControllableLookup(),
                            new ControllableLookup(),
                            maxParallelism,
                            2,
                            subtask);
            before.open();
            for (int key = 1; key <= 12; key++) {
                if (ownerOf(keySelector, key, maxParallelism, 2) == subtask) {
                    before.processElement(note(key));
                }
            }
            runMails(before);
            snapshots.add(before.snapshot(1L, 1L));
            // abandoned: its lookups never complete
        }
        OperatorSubtaskState merged =
                AbstractStreamOperatorTestHarness.repackageState(
                        snapshots.toArray(new OperatorSubtaskState[0]));

        List<Integer> recomputed = new ArrayList<>();
        for (int subtask = 0; subtask < 3; subtask++) {
            ControllableLookup anchor = new ControllableLookup();
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> after =
                    createHarness(
                            GroupRefreshOutputMode.STATE,
                            anchor,
                            new ControllableLookup(),
                            new ControllableLookup(),
                            maxParallelism,
                            3,
                            subtask);
            after.initializeState(
                    AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            merged, maxParallelism, 2, 3, subtask));
            after.open();
            runMails(after);
            for (int key : anchor.allKeys()) {
                assertThat(ownerOf(keySelector, key, maxParallelism, 3))
                        .as("key %s is recomputed by its owner", key)
                        .isEqualTo(subtask);
                recomputed.add(key);
            }
        }
        Collections.sort(recomputed);
        assertThat(recomputed).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    }

    private static int ownerOf(
            RowDataKeySelector keySelector, int key, int maxParallelism, int parallelism)
            throws Exception {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                keySelector.getKey(GenericRowData.of(key)), maxParallelism, parallelism);
    }

    @Test
    void testGroupStateAndDirtyKeysSurviveCheckpoints() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(Collections.emptyList());
        runMails();

        // a recompute is running and the key is dirty when the checkpoint is taken
        putB(10, 7);
        harness.processElement(note(1));
        harness.processElement(note(1));
        runMails();
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        // abandon the failed attempt: closing it would wait for its lookups forever

        anchorLookup = new ControllableLookup();
        lookupB = new ControllableLookup();
        lookupA = new ControllableLookup();
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.initializeState(snapshot);
        harness.open();
        runMails();
        assertThat(anchorLookup.pending()).isEqualTo(1);
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(tableB.get(7));
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertor.assertOutputEqualsSorted(
                "the restored group is retracted, so it was restored",
                List.of(deleteRecord(1, 7, null, null), insertRecord(1, 7, 10, 7)),
                harness.getOutput());
    }

    /**
     * {@code A LEFT JOIN (B JOIN C ON B.id = C.b_id) ON A.id = B.a_id}, anchored on A, with the
     * declared key {@code (A.id, C.id)}. One recompute reads C once under each of two B rows while
     * C moves from one to the other, so it sees the same C row twice. Emitting both rows and later
     * retracting the old one would delete the surviving row from a sink keyed by {@code (A.id,
     * C.id)}.
     */
    @Test
    void testGroupStaysUniqueOnDeclaredKeysWhenReadsAreMixed() throws Exception {
        assertMovingRowIsEmittedOnce(7, 7, false);
    }

    /** The same with a binary key, which must be compared by content. */
    @Test
    void testBinaryDeclaredKeysAreComparedByContent() throws Exception {
        assertMovingRowIsEmittedOnce(new byte[] {7}, new byte[] {7}, false);
    }

    /**
     * The same with a declared key {@code (A.id, C.id, D.id)} whose last column is NULL because D
     * is padded: NULLs are equal, as in keyed state and in consumers.
     */
    @Test
    void testDeclaredKeysWithNullsAreCompared() throws Exception {
        assertMovingRowIsEmittedOnce(7, 7, true);
    }

    @Test
    void testDeduplicatedGroupSurvivesCheckpoints() throws Exception {
        Lookups lookups = new Lookups();
        harness = createMovingRowHarness(lookups, INT, false);
        harness.open();
        readMovingRowTwice(lookups, 7, 7, false);
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        // abandon the failed attempt: closing it would wait for its lookups forever

        Lookups restoredLookups = new Lookups();
        harness = createMovingRowHarness(restoredLookups, INT, false);
        harness.initializeState(snapshot);
        harness.open();
        runMails();
        readMovingRowOnce(restoredLookups, 7, false);
        assertThat(operator().getScheduler().numRunning()).isZero();
        assertor(INT, false)
                .assertOutputEquals(
                        "the remembered deduplicated group is replaced after the restore",
                        List.of(deleteRecord(1, 10, 1, 7, 10, 1), insertRecord(1, 20, 1, 7, 20, 2)),
                        harness.getOutput());
    }

    @Test
    void testUpsertModeNeverDeletesWhatItDidNotEmit() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.UPSERT);
        harness.open();

        // the anchor row does not exist (yet): nothing was emitted, so nothing is deleted
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(Collections.emptyList());
        runMails();
        assertThat(harness.getOutput()).isEmpty();

        putA(1, 7);
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(Collections.emptyList());
        runMails();
        tableA.remove(1);
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(Collections.emptyList());
        runMails();

        assertor.assertOutputEquals(
                "only the emitted row is deleted",
                List.of(updateAfterRecord(1, 7, null, null), deleteRecord(1, null, null, null)),
                harness.getOutput());
    }

    /**
     * The failed run emitted a row for anchor 1 after the checkpoint and the anchor was deleted
     * before its delete was emitted. The restored flag does not know about the row, but a consumer
     * that is not rolled back holds it, so the restored run deletes the empty group anyway.
     */
    @Test
    void testUpsertModeDeletesEmptyGroupsAfterRestore() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.UPSERT);
        harness.open();
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        putA(1, 7);
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        lookupB.completeNext(Collections.emptyList());
        runMails();
        assertor.assertOutputEquals(
                "emitted after the checkpoint",
                List.of(updateAfterRecord(1, 7, null, null)),
                harness.getOutput());
        harness.close();

        tableA.remove(1);
        anchorLookup = new ControllableLookup();
        lookupB = new ControllableLookup();
        lookupA = new ControllableLookup();
        harness = createHarness(GroupRefreshOutputMode.UPSERT);
        harness.initializeState(snapshot);
        harness.open();
        harness.processElement(note(1));
        runMails();
        anchorLookup.completeNext(Collections.emptyList());
        runMails();
        assertor.assertOutputEquals(
                "the restored run deletes the empty group without a flag",
                List.of(deleteRecord(1, null, null, null)),
                harness.getOutput());
    }

    /** A failed lookup that two recomputes share fails both; neither becomes an empty result. */
    @Test
    void testFailedSharedLookupFailsEveryRecompute() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);
        putA(2, 7);
        harness.processElement(note(1));
        harness.processElement(note(2));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        anchorLookup.completeNext(tableA.get(2));
        runMails();
        assertThat(lookupB.pending()).as("anchor 2 shares the lookup of anchor 1").isOne();

        lookupB.failNext(new IOException("storage unavailable"));
        int failures = 0;
        for (int i = 0; i < 2; i++) {
            try {
                runMails();
            } catch (FlinkRuntimeException e) {
                assertThat(e).hasRootCauseInstanceOf(IOException.class);
                failures++;
            }
        }
        assertThat(failures).as("both recomputes fail").isEqualTo(2);
        assertThat(harness.getOutput()).as("no padded rows").isEmpty();
        harness = null;
    }

    /** A checkpoint taken while lookups wait for capacity recomputes their key after a restore. */
    @Test
    void testLookupsWaitingForCapacitySurviveCheckpoints() throws Exception {
        Lookups lookups = new Lookups();
        harness = createMovingRowHarness(lookups, INT, false, 1);
        harness.open();
        harness.processElement(note(1));
        runMails();
        lookups.anchor.completeNext(List.of(GenericRowData.of(1)));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(10, 1), GenericRowData.of(20, 1)));
        runMails();
        assertThat(lookups.cByB.pending()).as("the second lookup of C waits").isOne();
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        // abandoned: its lookups never complete

        Lookups restoredLookups = new Lookups();
        harness = createMovingRowHarness(restoredLookups, INT, false, 1);
        harness.initializeState(snapshot);
        harness.open();
        runMails();
        assertThat(restoredLookups.anchor.pending()).as("the key is recomputed").isOne();
        restoredLookups.anchor.completeNext(List.of(GenericRowData.of(1)));
        runMails();
        restoredLookups.bByA.completeNext(Collections.emptyList());
        runMails();
        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(harness.getOutput()).as("the recomputed group is emitted").isNotEmpty();
    }

    /**
     * Recomputes of anchors 1 and 2 both look up B by k = 7. The lookup of anchor 2 shares the one
     * of anchor 1, issued after anchor 2's recompute started. Anchor 3's recompute starts after
     * that lookup was issued, so it must issue its own: the shared one may read an older state.
     */
    @Test
    void testIdenticalLookupsAreSharedOnlyIfIssuedAfterTheStart() throws Exception {
        harness = createHarness(GroupRefreshOutputMode.STATE);
        harness.open();
        putA(1, 7);
        putA(2, 7);
        putA(3, 7);

        harness.processElement(note(1));
        harness.processElement(note(2));
        runMails();
        anchorLookup.completeNext(tableA.get(1));
        runMails();
        assertThat(lookupB.pending()).isOne();
        anchorLookup.completeNext(tableA.get(2));
        runMails();
        assertThat(lookupB.pending()).as("anchor 2 shares the lookup").isOne();

        harness.processElement(note(3));
        runMails();
        anchorLookup.completeNext(tableA.get(3));
        runMails();
        assertThat(lookupB.pending()).as("anchor 3 started later and issues its own").isEqualTo(2);

        lookupB.completeNext(Collections.emptyList());
        lookupB.completeNext(Collections.emptyList());
        runMails();
        assertor.assertOutputEqualsSorted(
                "three padded rows",
                List.of(
                        insertRecord(1, 7, null, null),
                        insertRecord(2, 7, null, null),
                        insertRecord(3, 7, null, null)),
                harness.getOutput());
    }

    @Test
    void testLookupsInFlightAreBounded() throws Exception {
        Lookups lookups = new Lookups();
        harness = createMovingRowHarness(lookups, INT, false, 1);
        harness.open();
        harness.processElement(note(1));
        runMails();
        lookups.anchor.completeNext(List.of(GenericRowData.of(1)));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(10, 1), GenericRowData.of(20, 1)));
        runMails();
        assertThat(lookups.cByB.pending()).as("the second lookup of C waits").isOne();
        lookups.cByB.completeNext(Collections.emptyList());
        runMails();
        assertThat(lookups.cByB.pending()).isOne();
        lookups.cByB.completeNext(Collections.emptyList());
        runMails();
        assertThat(operator().getScheduler().numRunning()).isZero();
    }

    private void assertMovingRowIsEmittedOnce(Object cId, Object sameCId, boolean withD)
            throws Exception {
        LogicalType cIdType = cId instanceof byte[] ? new VarBinaryType() : INT;
        Lookups lookups = new Lookups();
        harness = createMovingRowHarness(lookups, cIdType, withD);
        harness.open();
        readMovingRowTwice(lookups, cId, sameCId, withD);
        assertThat(operator().getScheduler().numRunning())
                .as("the inconsistent group is recomputed")
                .isOne();
        readMovingRowOnce(lookups, sameCId, withD);
        assertThat(operator().getScheduler().numRunning()).isZero();

        Object[] x =
                withD
                        ? new Object[] {1, 10, 1, cId, 10, 1, null, null}
                        : new Object[] {1, 10, 1, cId, 10, 1};
        Object[] y =
                withD
                        ? new Object[] {1, 20, 1, sameCId, 20, 2, null, null}
                        : new Object[] {1, 20, 1, sameCId, 20, 2};
        assertor(cIdType, withD)
                .assertOutputEquals(
                        "never two rows with the same declared key",
                        List.of(insertRecord(x), deleteRecord(x), insertRecord(y)),
                        harness.getOutput());
    }

    /** A recompute that reads C(id) under B(10) before it moves and under B(20) after it. */
    private void readMovingRowTwice(Lookups lookups, Object cId, Object sameCId, boolean withD)
            throws Exception {
        harness.processElement(note(1));
        runMails();
        lookups.anchor.completeNext(List.of(GenericRowData.of(1)));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(10, 1), GenericRowData.of(20, 1)));
        runMails();
        lookups.cByB.completeNext(List.of(GenericRowData.of(cId, 10, 1)));
        lookups.cByB.completeNext(List.of(GenericRowData.of(sameCId, 20, 2)));
        runMails();
        if (withD) {
            // both rows look up D by the same C.id, so the lookups are shared
            assertThat(lookups.dByC.pending()).isOne();
            lookups.dByC.completeNext(Collections.emptyList());
            runMails();
        }
    }

    /** A recompute that reads the final state: C(id) belongs to B(20). */
    private void readMovingRowOnce(Lookups lookups, Object cId, boolean withD) throws Exception {
        lookups.anchor.completeNext(List.of(GenericRowData.of(1)));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(10, 1), GenericRowData.of(20, 1)));
        runMails();
        lookups.cByB.completeNext(Collections.emptyList());
        lookups.cByB.completeNext(List.of(GenericRowData.of(cId, 20, 2)));
        runMails();
        if (withD) {
            lookups.dByC.completeNext(Collections.emptyList());
            runMails();
        }
    }

    /**
     * In {@code A LEFT JOIN B ON A.id = B.a LEFT JOIN C ON A.id = C.a}, the lookups of B and C need
     * only the anchor key: they start together with the lookup of A, in any completion order.
     */
    @Test
    void testLookupsKeyedByTheAnchorKeyStartWithTheAnchorLookup() throws Exception {
        Lookups lookups = new Lookups();
        harness = createStarHarness(lookups, 0);
        harness.open();
        harness.processElement(note(1));
        runMails();
        assertThat(lookups.anchor.pending()).isOne();
        assertThat(lookups.bByA.pending()).as("B starts with A").isOne();
        assertThat(lookups.cByA.pending()).as("C starts with A").isOne();

        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.bByA.completeNext(List.of(GenericRowData.of(20, 1)));
        runMails();
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(lookups.bByA.pending()).as("A's row reuses the lookup of B").isZero();
        assertThat(lookups.cByA.pending()).as("A's row reuses the lookup of C").isZero();
        assertThat(operator().getScheduler().numRunning()).isZero();
        starAssertor()
                .assertOutputEqualsSorted(
                        "one line", List.of(insertRecord(1, 5, 20, 1, 30, 1)), harness.getOutput());
    }

    /** Without an anchor row, the lookups that started with it are not used. */
    @Test
    void testLookupsKeyedByTheAnchorKeyAreIgnoredWithoutAnchorRow() throws Exception {
        Lookups lookups = new Lookups();
        harness = createStarHarness(lookups, 0);
        harness.open();
        harness.processElement(note(1));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(20, 1)));
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(Collections.emptyList());
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(harness.getOutput()).as("no anchor row, no line").isEmpty();
    }

    /**
     * In {@code A LEFT JOIN B ON A.k = B.a LEFT JOIN C ON A.k = C.a}, the lookups of B and C need
     * A's row, and both start when it arrives: C does not wait for B.
     */
    @Test
    void testLookupsStartAsSoonAsTheirKeyIsKnown() throws Exception {
        Lookups lookups = new Lookups();
        harness = createStarHarness(lookups, 1);
        harness.open();
        harness.processElement(note(1));
        runMails();
        assertThat(lookups.bByA.pending()).as("B needs A's row").isZero();
        assertThat(lookups.cByA.pending()).as("C needs A's row").isZero();

        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 7)));
        runMails();
        assertThat(lookups.bByA.pending()).isOne();
        assertThat(lookups.cByA.pending()).as("C starts before B completed").isOne();

        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 7)));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(20, 7), GenericRowData.of(21, 7)));
        runMails();

        assertThat(lookups.cByA.pending()).as("both lines reuse the lookup of C").isZero();
        assertThat(operator().getScheduler().numRunning()).isZero();
        starAssertor()
                .assertOutputEqualsSorted(
                        "two lines",
                        List.of(insertRecord(1, 7, 20, 7, 30, 7), insertRecord(1, 7, 21, 7, 30, 7)),
                        harness.getOutput());
    }

    /** A group with more joined rows than allowed fails the task instead of using the memory. */
    @Test
    void testGroupLargerThanAllowedFails() throws Exception {
        Lookups lookups = new Lookups();
        harness = createStarHarness(lookups, 0, 1);
        harness.open();
        harness.processElement(note(1));
        runMails();
        lookups.bByA.completeNext(List.of(GenericRowData.of(20, 1), GenericRowData.of(21, 1)));
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));

        assertThatThrownBy(this::runMails)
                .hasRootCauseInstanceOf(TableException.class)
                .rootCause()
                .hasMessageMatching(
                        "The group of key .* has 2 joined rows, more than the 1 that "
                                + "'table.exec.delta-join.group-refresh.max-group-size' allows. "
                                + "Raise the limit if the memory of the task allows it.");
        assertThat(harness.getOutput()).isEmpty();
        harness = null;
    }

    // --------------------------------------------------------------------------------------------
    // refreshes of the lines of changed rows, in A LEFT JOIN B ON A.id = B.a LEFT JOIN C ON
    // A.id = C.a with A(id, k), B(id, a, v), C(id, a); see createParentChildHarness
    // --------------------------------------------------------------------------------------------

    /** An update of a B row replaces its own line, without reading B or B's other rows. */
    @Test
    void testUpdatedChildReplacesOnlyItsLine() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        refreshWhole(lookups, List.of(bRow(20, 1, 100), bRow(21, 1, 200)));
        assertThat(harness.getOutput()).hasSize(2);
        harness.getOutput().clear();

        harness.processElement(bNote(1, AnchorNotes.REPLACED, bRow(20, 1, 100)));
        assertThat(lookups.anchor.pending()).as("waits for the new row").isZero();
        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(20, 1, 101)));
        runMails();
        assertThat(lookups.bByA.pending()).as("B is not read").isZero();
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(operator().getScopedRefreshes()).isOne();
        parentChildAssertor()
                .assertOutputEqualsSorted(
                        "only the line of the changed row",
                        List.of(
                                updateBeforeRecord(1, 5, 20, 1, 100, 30, 1),
                                updateAfterRecord(1, 5, 20, 1, 101, 30, 1)),
                        harness.getOutput());
    }

    /** The first B row of an A row removes A's padded line. */
    @Test
    void testFirstChildRemovesThePaddedLine() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        refreshWhole(lookups, List.of());
        harness.getOutput().clear();

        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(20, 1, 100)));
        runMails();
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(lookups.bByA.pending()).isZero();
        assertThat(operator().getScopedRefreshes()).isOne();
        parentChildAssertor()
                .assertOutputEqualsSorted(
                        "the padded line goes",
                        List.of(
                                deleteRecord(1, 5, null, null, null, 30, 1),
                                insertRecord(1, 5, 20, 1, 100, 30, 1)),
                        harness.getOutput());
    }

    /**
     * A deleted B row that the group emitted may have been A's last match, which only B knows: the
     * whole group is refreshed.
     */
    @Test
    void testDeletedChildOfPreservedParentRefreshesTheGroup() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        refreshWhole(lookups, List.of(bRow(20, 1, 100)));
        harness.getOutput().clear();

        harness.processElement(bNote(1, AnchorNotes.RETRACT, bRow(20, 1, 100)));
        runMails();
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();
        assertThat(lookups.bByA.pending()).as("B is read").isOne();
        lookups.bByA.completeNext(List.of());
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(operator().getScopedRefreshes()).isZero();
        parentChildAssertor()
                .assertOutputEqualsSorted(
                        "A keeps its padded line",
                        List.of(
                                deleteRecord(1, 5, 20, 1, 100, 30, 1),
                                insertRecord(1, 5, null, null, null, 30, 1)),
                        harness.getOutput());
    }

    /** Without a preserved parent, a deleted B row only takes its own line. */
    @Test
    void testDeletedChildOfInnerJoinRemovesItsLine() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.INNER);
        harness.open();
        refreshWhole(lookups, List.of(bRow(20, 1, 100)));
        harness.getOutput().clear();

        harness.processElement(bNote(1, AnchorNotes.RETRACT, bRow(20, 1, 100)));
        runMails();
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(lookups.bByA.pending()).isZero();
        assertThat(operator().getScopedRefreshes()).isOne();
        parentChildAssertor()
                .assertOutputEqualsSorted(
                        "the line goes",
                        List.of(deleteRecord(1, 5, 20, 1, 100, 30, 1)),
                        harness.getOutput());
    }

    /**
     * A B row that moves to another A row leaves its old line there and is added to the new one.
     */
    @Test
    void testChildMovingToAnotherParent() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.INNER);
        harness.open();
        refreshWhole(lookups, List.of(bRow(20, 1, 100)));
        refreshWhole(lookups, 2, GenericRowData.of(2, 6), List.of(), List.of());
        harness.getOutput().clear();

        // the locate operator sends the old row to A(1) and the new one to A(2)
        harness.processElement(bNote(1, AnchorNotes.RETRACT, bRow(20, 1, 100)));
        harness.processElement(bNote(2, AnchorNotes.ADD, bRow(20, 2, 100)));
        runMails();
        assertThat(lookups.anchor.allKeys()).containsExactly(1, 2);
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.cByA.completeNext(List.of());
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(2, 6)));
        runMails();

        assertThat(lookups.bByA.pending()).isZero();
        assertThat(operator().getScopedRefreshes()).isEqualTo(2);
        parentChildAssertor()
                .assertOutputEqualsSorted(
                        "the line moves",
                        List.of(
                                deleteRecord(1, 5, 20, 1, 100, 30, 1),
                                insertRecord(2, 6, 20, 2, 100, null, null)),
                        harness.getOutput());
    }

    /** Changes of two tables, or of the anchor table, refresh the whole group. */
    @Test
    void testChangesOfSeveralTablesRefreshTheGroup() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        // the first note starts a refresh; the two others wait for it and merge
        harness.processElement(note(1));
        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(20, 1, 100)));
        harness.processElement(cNote(1, AnchorNotes.ADD, GenericRowData.of(30, 1)));
        runMails();
        lookups.bByA.completeNext(List.of());
        lookups.cByA.completeNext(List.of());
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();
        assertThat(lookups.bByA.pending()).as("the second refresh reads B").isOne();
        lookups.bByA.completeNext(List.of(bRow(20, 1, 100)));
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(operator().getScopedRefreshes()).isZero();
        assertThat(operator().getRefreshes()).isEqualTo(2);
    }

    /** A refresh started by another note before the new row of a replaced row arrived is whole. */
    @Test
    void testRefreshOvertakingTheNewRowIsWhole() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        refreshWhole(lookups, List.of(bRow(20, 1, 100)));
        harness.processElement(bNote(1, AnchorNotes.REPLACED, bRow(20, 1, 100)));
        // a note from another sender arrives before the new row
        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(21, 1, 200)));
        runMails();
        assertThat(lookups.bByA.pending()).as("B is read").isOne();
    }

    /**
     * After lines of B, a change of C refreshes the whole group: B's lines may have been emitted
     * with C rows that changed since. So does the first change of a group the operator never saw.
     */
    @Test
    void testOnlyOneTableRefreshesItsLinesBetweenWholeRefreshes() throws Exception {
        Lookups lookups = new Lookups();
        harness = createParentChildHarness(lookups, FlinkJoinType.LEFT);
        harness.open();
        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(20, 1, 100)));
        runMails();
        assertThat(lookups.bByA.pending()).as("an unknown group is refreshed whole").isOne();
        lookups.bByA.completeNext(List.of(bRow(20, 1, 100)));
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        harness.processElement(bNote(1, AnchorNotes.ADD, bRow(21, 1, 200)));
        runMails();
        assertThat(lookups.bByA.pending()).as("lines of B").isZero();
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        harness.processElement(cNote(1, AnchorNotes.ADD, GenericRowData.of(31, 1)));
        runMails();
        assertThat(lookups.bByA.pending()).as("C after B: the whole group").isOne();
        lookups.bByA.completeNext(List.of(bRow(20, 1, 100), bRow(21, 1, 200)));
        lookups.cByA.completeNext(List.of(GenericRowData.of(30, 1), GenericRowData.of(31, 1)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();

        harness.processElement(cNote(1, AnchorNotes.ADD, GenericRowData.of(32, 1)));
        runMails();
        assertThat(lookups.cByA.pending()).as("lines of C after the whole group").isZero();
        lookups.bByA.completeNext(List.of(bRow(20, 1, 100), bRow(21, 1, 200)));
        lookups.anchor.completeNext(List.of(GenericRowData.of(1, 5)));
        runMails();
        assertThat(operator().getScheduler().numRunning()).isZero();
        assertThat(operator().getScopedRefreshes()).isEqualTo(2);
    }

    private void refreshWhole(Lookups lookups, List<GenericRowData> bRows) throws Exception {
        refreshWhole(lookups, 1, GenericRowData.of(1, 5), bRows, List.of(GenericRowData.of(30, 1)));
    }

    private void refreshWhole(
            Lookups lookups,
            int anchorKey,
            GenericRowData anchor,
            List<GenericRowData> bRows,
            List<GenericRowData> cRows)
            throws Exception {
        harness.processElement(note(anchorKey));
        runMails();
        lookups.bByA.completeNext(bRows);
        lookups.cByA.completeNext(cRows);
        lookups.anchor.completeNext(List.of(anchor));
        runMails();
        assertThat(operator().getScheduler().numRunning()).isZero();
    }

    private static GenericRowData bRow(int id, int a, int v) {
        return GenericRowData.of(id, a, v);
    }

    /** A note of a change of B, whose rows are field 3 of a note. */
    private static StreamRecord<RowData> bNote(int anchorKey, byte change, RowData row) {
        return new StreamRecord<>(GenericRowData.of(anchorKey, 1, change, row, null));
    }

    /** A note of a change of C, whose rows are field 4 of a note. */
    private static StreamRecord<RowData> cNote(int anchorKey, byte change, RowData row) {
        return new StreamRecord<>(GenericRowData.of(anchorKey, 2, change, null, row));
    }

    // --------------------------------------------------------------------------------------------

    private static final LogicalType INT = new IntType();

    /** The lookups of the moving-row tree. */
    private static final class Lookups {
        final ControllableLookup anchor = new ControllableLookup();
        final ControllableLookup bByA = new ControllableLookup();
        final ControllableLookup cByB = new ControllableLookup();
        final ControllableLookup dByC = new ControllableLookup();
        final ControllableLookup cByA = new ControllableLookup();
    }

    private static RowType movingRowOutput(LogicalType cIdType, boolean withD) {
        return withD
                ? RowType.of(INT, INT, INT, cIdType, INT, INT, INT, cIdType)
                : RowType.of(INT, INT, INT, cIdType, INT, INT);
    }

    private static RowDataHarnessAssertor assertor(LogicalType cIdType, boolean withD) {
        return new RowDataHarnessAssertor(
                movingRowOutput(cIdType, withD).getChildren().toArray(new LogicalType[0]));
    }

    /**
     * {@code A LEFT JOIN (B JOIN C ON B.id = C.b_id) ON A.id = B.a_id}, optionally {@code LEFT JOIN
     * D ON C.id = D.c_id} on top, anchored on A. Rows: A(id), B(id, a_id), C(id, b_id, v), D(id,
     * c_id). Declared key: {@code (A.id, C.id)}, or {@code (A.id, C.id, D.id)} with D.
     */
    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>
            createMovingRowHarness(Lookups lookups, LogicalType cIdType, boolean withD)
                    throws Exception {
        return createMovingRowHarness(lookups, cIdType, withD, 16);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>
            createMovingRowHarness(
                    Lookups lookups, LogicalType cIdType, boolean withD, int capacity)
                    throws Exception {
        int numLeaves = withD ? 4 : 3;
        RowType a = RowType.of(INT);
        RowType b = RowType.of(INT, INT);
        RowType c = RowType.of(cIdType, INT, INT);
        RowType d = RowType.of(INT, cIdType);
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[2 * numLeaves - 1];
        nodes[0] = AnchorJoinTree.Node.leaf(0, numLeaves, 1);
        nodes[1] = AnchorJoinTree.Node.leaf(1, numLeaves, 2);
        nodes[2] = AnchorJoinTree.Node.leaf(2, numLeaves, 3);
        int bc = withD ? 4 : 3;
        int abc = withD ? 5 : 4;
        if (withD) {
            nodes[3] = AnchorJoinTree.Node.leaf(3, numLeaves, 2);
        }
        nodes[bc] =
                AnchorJoinTree.Node.join(
                        1,
                        2,
                        nodes[1],
                        nodes[2],
                        FlinkJoinType.INNER,
                        0,
                        nodes[1].getOutput().concat(nodes[2].getOutput()),
                        Collections.emptyList());
        nodes[abc] =
                AnchorJoinTree.Node.join(
                        0,
                        bc,
                        nodes[0],
                        nodes[bc],
                        FlinkJoinType.LEFT,
                        1,
                        nodes[0].getOutput().concat(nodes[bc].getOutput()),
                        Collections.emptyList());
        List<AnchorJoinTree.Edge> edges = new ArrayList<>();
        edges.add(new AnchorJoinTree.Edge(1, new int[] {0}, 2, new int[] {1}, 1, 2));
        edges.add(new AnchorJoinTree.Edge(0, new int[] {0}, 1, new int[] {1}, 3, 4));
        List<AnchorLookupSpec> specs = new ArrayList<>();
        specs.add(lookupSpec(lookups.anchor, new int[] {0}, a, "A by id"));
        specs.add(lookupSpec(lookups.cByB, new int[] {0}, c, "C by B.id"));
        specs.add(lookupSpec(new ControllableLookup(), new int[] {1}, b, "B by C.b_id"));
        specs.add(lookupSpec(lookups.bByA, new int[] {0}, b, "B by A.id"));
        specs.add(lookupSpec(new ControllableLookup(), new int[] {1}, a, "A by B.a_id"));
        GeneratedJoinCondition[] conditions = new GeneratedJoinCondition[nodes.length];
        conditions[bc] = condition(0, INT, 1, INT, "B.id = C.b_id");
        conditions[abc] = condition(0, INT, 1, INT, "A.id = B.a_id");
        int[][] declaredKeys = {{0, 3}};
        if (withD) {
            nodes[6] =
                    AnchorJoinTree.Node.join(
                            abc,
                            3,
                            nodes[abc],
                            nodes[3],
                            FlinkJoinType.LEFT,
                            2,
                            nodes[abc].getOutput().concat(nodes[3].getOutput()),
                            Collections.emptyList());
            edges.add(new AnchorJoinTree.Edge(2, new int[] {0}, 3, new int[] {1}, 5, 6));
            specs.add(
                    lookupSpec(
                            lookups.dByC,
                            new int[] {0},
                            new LogicalType[] {cIdType},
                            d,
                            "D by C.id"));
            specs.add(
                    lookupSpec(
                            new ControllableLookup(),
                            new int[] {1},
                            new LogicalType[] {cIdType},
                            c,
                            "C by D.c_id"));
            conditions[6] = condition(3, cIdType, 1, cIdType, "C.id = D.c_id");
            declaredKeys = new int[][] {{0, 3, 6}};
        }
        AnchorJoinTree tree =
                new AnchorJoinTree(
                        withD ? new RowType[] {a, b, c, d} : new RowType[] {a, b, c},
                        nodes,
                        edges.toArray(new AnchorJoinTree.Edge[0]),
                        0,
                        new int[] {0},
                        0);
        GeneratedFilterCondition[][] filters = new GeneratedFilterCondition[nodes.length][];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = new GeneratedFilterCondition[0];
        }
        AnchorGroupRefreshOperatorFactory factory =
                new AnchorGroupRefreshOperatorFactory(
                        spec(
                                tree,
                                movingRowOutput(cIdType, withD),
                                GroupRefreshOutputMode.STATE,
                                declaredKeys,
                                capacity),
                        tree,
                        specs.toArray(new AnchorLookupSpec[0]),
                        conditions,
                        filters,
                        AnchorNotes.of(tree, new boolean[tree.getNumLeaves()]));
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, KEY.getChildren().toArray(new LogicalType[0]));
        return new KeyedOneInputStreamOperatorTestHarness<>(
                factory, keySelector, keySelector.getProducedType(), 1, 1, 0);
    }

    private static RowDataHarnessAssertor parentChildAssertor() {
        return new RowDataHarnessAssertor(new LogicalType[] {INT, INT, INT, INT, INT, INT, INT});
    }

    /**
     * {@code A [bJoin] JOIN B ON A.id = B.a LEFT JOIN C ON A.id = C.a}, anchored on A, with rows
     * A(id, k), B(id, a, v), C(id, a) whose unique keys are their ids. B and C name their anchor,
     * so their notes carry their rows. Output mode STATE.
     */
    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>
            createParentChildHarness(Lookups lookups, FlinkJoinType bJoin) throws Exception {
        RowType a = RowType.of(INT, INT);
        RowType b = RowType.of(INT, INT, INT);
        RowType c = RowType.of(INT, INT);
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[5];
        nodes[0] = AnchorJoinTree.Node.leaf(0, 3, 2);
        nodes[1] = AnchorJoinTree.Node.leaf(1, 3, 3);
        nodes[2] = AnchorJoinTree.Node.leaf(2, 3, 2);
        nodes[3] =
                AnchorJoinTree.Node.join(
                        0,
                        1,
                        nodes[0],
                        nodes[1],
                        bJoin,
                        0,
                        nodes[0].getOutput().concat(nodes[1].getOutput()),
                        Collections.emptyList());
        nodes[4] =
                AnchorJoinTree.Node.join(
                        3,
                        2,
                        nodes[3],
                        nodes[2],
                        FlinkJoinType.LEFT,
                        1,
                        nodes[3].getOutput().concat(nodes[2].getOutput()),
                        Collections.emptyList());
        AnchorJoinTree tree =
                new AnchorJoinTree(
                        new RowType[] {a, b, c},
                        nodes,
                        new AnchorJoinTree.Edge[] {
                            new AnchorJoinTree.Edge(0, new int[] {0}, 1, new int[] {1}, 1, 2),
                            new AnchorJoinTree.Edge(0, new int[] {0}, 2, new int[] {1}, 3, 4)
                        },
                        0,
                        new int[] {0},
                        0);
        AnchorLookupSpec[] specs = {
            lookupSpec(lookups.anchor, new int[] {0}, a, "A by id"),
            lookupSpec(lookups.bByA, new int[] {0}, b, "B by A.id"),
            lookupSpec(new ControllableLookup(), new int[] {1}, a, "A by B.a"),
            lookupSpec(lookups.cByA, new int[] {0}, c, "C by A.id"),
            lookupSpec(new ControllableLookup(), new int[] {1}, a, "A by C.a")
        };
        GeneratedJoinCondition[] conditions = new GeneratedJoinCondition[nodes.length];
        conditions[3] = condition(0, INT, 1, INT, "A.id = B.a");
        conditions[4] = condition(0, INT, 1, INT, "A.id = C.a");
        GeneratedFilterCondition[][] filters = new GeneratedFilterCondition[nodes.length][];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = new GeneratedFilterCondition[0];
        }
        AnchorJoinTree.FieldMap joined = tree.getNode(tree.getRootNode()).getOutput();
        GroupRefreshSpec spec =
                GroupRefreshSpec.builder(
                                new RowType[] {a, b, c},
                                new int[][] {{0}, {0}, {0}},
                                joined.getLeaves(),
                                joined.getFields(),
                                RowType.of(INT, INT, INT, INT, INT, INT, INT),
                                GroupRefreshOutputMode.STATE)
                        .capacity(16, 160)
                        .build();
        AnchorGroupRefreshOperatorFactory factory =
                new AnchorGroupRefreshOperatorFactory(
                        spec,
                        tree,
                        specs,
                        conditions,
                        filters,
                        AnchorNotes.of(tree, new boolean[] {false, true, true}));
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, KEY.getChildren().toArray(new LogicalType[0]));
        return new KeyedOneInputStreamOperatorTestHarness<>(
                factory, keySelector, keySelector.getProducedType(), 1, 1, 0);
    }

    private static RowDataHarnessAssertor starAssertor() {
        return new RowDataHarnessAssertor(new LogicalType[] {INT, INT, INT, INT, INT, INT});
    }

    /**
     * {@code A LEFT JOIN B ON A.x = B.a LEFT JOIN C ON A.x = C.a}, anchored on A, where {@code x}
     * is field {@code joinField} of A. Rows: A(id, k), B(id, a), C(id, a). Lookups of B and C use
     * {@code lookups.bByA} and {@code lookups.cByA}.
     */
    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createStarHarness(
            Lookups lookups, int joinField) throws Exception {
        return createStarHarness(lookups, joinField, Integer.MAX_VALUE);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createStarHarness(
            Lookups lookups, int joinField, int maxGroupSize) throws Exception {
        RowType row = RowType.of(INT, INT);
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[5];
        nodes[0] = AnchorJoinTree.Node.leaf(0, 3, 2);
        nodes[1] = AnchorJoinTree.Node.leaf(1, 3, 2);
        nodes[2] = AnchorJoinTree.Node.leaf(2, 3, 2);
        nodes[3] =
                AnchorJoinTree.Node.join(
                        0,
                        1,
                        nodes[0],
                        nodes[1],
                        FlinkJoinType.LEFT,
                        0,
                        nodes[0].getOutput().concat(nodes[1].getOutput()),
                        Collections.emptyList());
        nodes[4] =
                AnchorJoinTree.Node.join(
                        3,
                        2,
                        nodes[3],
                        nodes[2],
                        FlinkJoinType.LEFT,
                        1,
                        nodes[3].getOutput().concat(nodes[2].getOutput()),
                        Collections.emptyList());
        AnchorJoinTree tree =
                new AnchorJoinTree(
                        new RowType[] {row, row, row},
                        nodes,
                        new AnchorJoinTree.Edge[] {
                            new AnchorJoinTree.Edge(
                                    0, new int[] {joinField}, 1, new int[] {1}, 1, 2),
                            new AnchorJoinTree.Edge(
                                    0, new int[] {joinField}, 2, new int[] {1}, 3, 4)
                        },
                        0,
                        new int[] {0},
                        0);
        AnchorLookupSpec[] specs = {
            lookupSpec(lookups.anchor, new int[] {0}, row, "A by id"),
            lookupSpec(lookups.bByA, new int[] {joinField}, row, "B by A"),
            lookupSpec(new ControllableLookup(), new int[] {1}, row, "A by B.a"),
            lookupSpec(lookups.cByA, new int[] {joinField}, row, "C by A"),
            lookupSpec(new ControllableLookup(), new int[] {1}, row, "A by C.a")
        };
        GeneratedJoinCondition[] conditions = new GeneratedJoinCondition[nodes.length];
        conditions[3] = condition(joinField, INT, 1, INT, "A.x = B.a");
        conditions[4] = condition(joinField, INT, 1, INT, "A.x = C.a");
        GeneratedFilterCondition[][] filters = new GeneratedFilterCondition[nodes.length][];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = new GeneratedFilterCondition[0];
        }
        AnchorGroupRefreshOperatorFactory factory =
                new AnchorGroupRefreshOperatorFactory(
                        spec(
                                tree,
                                RowType.of(INT, INT, INT, INT, INT, INT),
                                GroupRefreshOutputMode.STATE,
                                new int[0][],
                                16,
                                maxGroupSize),
                        tree,
                        specs,
                        conditions,
                        filters,
                        AnchorNotes.of(tree, new boolean[tree.getNumLeaves()]));
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, KEY.getChildren().toArray(new LogicalType[0]));
        return new KeyedOneInputStreamOperatorTestHarness<>(
                factory, keySelector, keySelector.getProducedType(), 1, 1, 0);
    }

    /** {@code left[leftField] = right[rightField]}, NULL matching nothing. */
    private static GeneratedJoinCondition condition(
            int leftField,
            LogicalType leftType,
            int rightField,
            LogicalType rightType,
            String name) {
        return new GeneratedJoinCondition(name, "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new FieldsEqual(
                        RowData.createFieldGetter(leftType, leftField),
                        RowData.createFieldGetter(rightType, rightField));
            }
        };
    }

    private static final class FieldsEqual extends AbstractRichFunction implements JoinCondition {
        private final RowData.FieldGetter left;
        private final RowData.FieldGetter right;

        FieldsEqual(RowData.FieldGetter left, RowData.FieldGetter right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean apply(RowData in1, RowData in2) {
            Object l = left.getFieldOrNull(in1);
            Object r = right.getFieldOrNull(in2);
            return l != null && r != null && Objects.deepEquals(l, r);
        }
    }

    private AnchorGroupRefreshOperator operator() {
        return (AnchorGroupRefreshOperator) (AbstractStreamOperator<?>) harness.getOperator();
    }

    private void putA(int id, int k) {
        tableA.put(id, List.of(row(id, k)));
    }

    private void putB(int id, int k) {
        tableB.computeIfAbsent(k, key -> new ArrayList<>()).add(row(id, k));
    }

    private static GenericRowData row(int id, int k) {
        return GenericRowData.of(id, k);
    }

    /** A note of a change of the anchor table, which refreshes the whole group. */
    private static StreamRecord<RowData> note(int anchorKey) {
        return new StreamRecord<>(GenericRowData.of(anchorKey, 0, AnchorNotes.ADD));
    }

    private void runMails() throws Exception {
        runMails(harness);
    }

    private static void runMails(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness)
            throws Exception {
        TaskMailbox mailbox = harness.getTaskMailbox();
        Optional<Mail> mail;
        while ((mail = mailbox.tryTake(TaskMailbox.MIN_PRIORITY)).isPresent()) {
            mail.get().run();
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            GroupRefreshOutputMode mode) throws Exception {
        return createHarness(mode, anchorLookup, lookupB, lookupA, 1, 1, 0);
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            GroupRefreshOutputMode mode,
            ControllableLookup anchorLookup,
            ControllableLookup lookupB,
            ControllableLookup lookupA,
            int maxParallelism,
            int parallelism,
            int subtask)
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
                            new AnchorJoinTree.Edge(0, new int[] {1}, 1, new int[] {1}, 1, 2)
                        },
                        0,
                        new int[] {0},
                        0);
        AnchorLookupSpec[] lookups = {
            lookupSpec(anchorLookup, new int[] {0}, "anchor"),
            lookupSpec(lookupB, new int[] {1}, "B by A.k"),
            lookupSpec(lookupA, new int[] {1}, "A by B.k")
        };
        GeneratedJoinCondition sameK =
                new GeneratedJoinCondition("SameK", "", new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new SameKCondition();
                    }
                };
        GeneratedFilterCondition[][] filters = {
            new GeneratedFilterCondition[0],
            new GeneratedFilterCondition[0],
            new GeneratedFilterCondition[0]
        };
        AnchorGroupRefreshOperatorFactory factory =
                new AnchorGroupRefreshOperatorFactory(
                        spec(tree, OUTPUT, mode, new int[0][], 16),
                        tree,
                        lookups,
                        new GeneratedJoinCondition[] {null, null, sameK},
                        filters,
                        AnchorNotes.of(tree, new boolean[tree.getNumLeaves()]));
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, KEY.getChildren().toArray(new LogicalType[0]));
        return new KeyedOneInputStreamOperatorTestHarness<>(
                factory,
                keySelector,
                keySelector.getProducedType(),
                maxParallelism,
                parallelism,
                subtask);
    }

    /**
     * The tables have no unique keys, so that the rows of a group are a bag: equal rows are kept as
     * often as they occur.
     */
    private static GroupRefreshSpec spec(
            AnchorJoinTree tree,
            RowType outputType,
            GroupRefreshOutputMode mode,
            int[][] declaredKeys,
            int capacity) {
        return spec(tree, outputType, mode, declaredKeys, capacity, Integer.MAX_VALUE);
    }

    private static GroupRefreshSpec spec(
            AnchorJoinTree tree,
            RowType outputType,
            GroupRefreshOutputMode mode,
            int[][] declaredKeys,
            int capacity,
            int maxGroupSize) {
        RowType[] tables = new RowType[tree.getNumLeaves()];
        for (int i = 0; i < tables.length; i++) {
            tables[i] = tree.getLeafType(i);
        }
        AnchorJoinTree.FieldMap joined = tree.getNode(tree.getRootNode()).getOutput();
        GroupRefreshSpec.Builder spec =
                GroupRefreshSpec.builder(
                                tables,
                                new int[tables.length][0],
                                joined.getLeaves(),
                                joined.getFields(),
                                outputType,
                                mode)
                        .declaredKeys(declaredKeys)
                        .capacity(capacity, 10 * capacity)
                        .maxGroupSize(maxGroupSize);
        if (mode == GroupRefreshOutputMode.UPSERT) {
            spec.upsertByGroupKey(new int[] {0});
        }
        return spec.build();
    }

    private static AnchorLookupSpec lookupSpec(
            ControllableLookup lookup, int[] keyFields, String description) {
        return lookupSpec(lookup, keyFields, ROW, description);
    }

    private static AnchorLookupSpec lookupSpec(
            ControllableLookup lookup, int[] keyFields, RowType resultType, String description) {
        LogicalType[] keyTypes = new LogicalType[keyFields.length];
        java.util.Arrays.fill(keyTypes, INT);
        return lookupSpec(lookup, keyFields, keyTypes, resultType, description);
    }

    private static AnchorLookupSpec lookupSpec(
            ControllableLookup lookup,
            int[] keyFields,
            LogicalType[] keyTypes,
            RowType resultType,
            String description) {
        GeneratedFunction<AsyncFunction<RowData, Object>> fetcher =
                new GeneratedFunction<>(description, "", new Object[0]) {
                    @Override
                    public AsyncFunction<RowData, Object> newInstance(ClassLoader classLoader) {
                        return lookup;
                    }
                };
        return new AnchorLookupSpec(
                fetcher,
                TypeConversions.fromLogicalToDataType(resultType).bridgedTo(RowData.class),
                null,
                resultType,
                keyFields,
                keyTypes,
                description);
    }

    /** {@code left.k = right.k}. */
    private static final class SameKCondition extends AbstractRichFunction
            implements JoinCondition {
        @Override
        public boolean apply(RowData in1, RowData in2) {
            return !in1.isNullAt(1) && !in2.isNullAt(1) && in1.getInt(1) == in2.getInt(1);
        }
    }

    /** A lookup whose requests complete only when the test says so, in request order. */
    private static final class ControllableLookup extends AbstractRichFunction
            implements AsyncFunction<RowData, Object> {

        private final List<RowData> keys = new ArrayList<>();
        private final List<ResultFuture<Object>> futures = new ArrayList<>();

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) {
            keys.add(input);
            futures.add(resultFuture);
        }

        int pending() {
            return futures.size();
        }

        int nextKey() {
            return keys.get(0).getInt(0);
        }

        List<Integer> allKeys() {
            List<Integer> all = new ArrayList<>();
            for (RowData key : keys) {
                all.add(key.getInt(0));
            }
            return all;
        }

        void completeNext(Collection<? extends RowData> rows) {
            keys.remove(0);
            futures.remove(0).complete(new ArrayList<>(rows == null ? List.of() : rows));
        }

        void failNext(Throwable error) {
            keys.remove(0);
            futures.remove(0).completeExceptionally(error);
        }
    }
}
