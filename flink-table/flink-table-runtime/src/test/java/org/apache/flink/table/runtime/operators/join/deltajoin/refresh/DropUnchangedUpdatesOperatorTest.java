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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link DropUnchangedUpdatesOperator}. */
class DropUnchangedUpdatesOperatorTest {

    private static final RowType TYPE = RowType.of(new IntType(), new IntType());

    @Test
    void testDropsAdjacentUnchangedUpdate() throws Exception {
        assertThat(
                        run(
                                row(RowKind.INSERT, 1, 1),
                                row(RowKind.UPDATE_BEFORE, 1, 1),
                                row(RowKind.UPDATE_AFTER, 1, 1),
                                row(RowKind.DELETE, 1, 1)))
                .containsExactly("+I(1,1)", "-D(1,1)");
    }

    @Test
    void testPassesChangedUpdate() throws Exception {
        assertThat(run(row(RowKind.UPDATE_BEFORE, 1, 1), row(RowKind.UPDATE_AFTER, 1, 2)))
                .containsExactly("-U(1,1)", "+U(1,2)");
    }

    /** A filter dropped the UPDATE_AFTER: the row left the input, which is a change. */
    @Test
    void testPassesUpdateBeforeWithoutItsUpdateAfter() throws Exception {
        assertThat(
                        run(
                                row(RowKind.UPDATE_BEFORE, 1, 1),
                                row(RowKind.INSERT, 2, 2),
                                row(RowKind.UPDATE_AFTER, 1, 1)))
                .containsExactly("-U(1,1)", "+I(2,2)", "+U(1,1)");
    }

    @Test
    void testPassesHeldBackRowsBeforeWatermarksAndAtTheEnd() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = harness()) {
            harness.processElement(new StreamRecord<>(row(RowKind.UPDATE_BEFORE, 1, 1)));
            harness.processWatermark(new Watermark(5));
            harness.processElement(new StreamRecord<>(row(RowKind.UPDATE_AFTER, 1, 1)));
            harness.processElement(new StreamRecord<>(row(RowKind.UPDATE_BEFORE, 2, 2)));
            harness.endInput();
            List<String> out = new ArrayList<>();
            for (Object o : harness.getOutput()) {
                out.add(
                        o instanceof Watermark
                                ? "W"
                                : show((RowData) ((StreamRecord<?>) o).getValue()));
            }
            assertThat(out).containsExactly("-U(1,1)", "W", "+U(1,1)", "-U(2,2)");
        }
    }

    /** A held back row is passed on before a barrier, so that no checkpoint has to keep it. */
    @Test
    void testPassesHeldBackRowBeforeBarrier() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = harness()) {
            harness.processElement(new StreamRecord<>(row(RowKind.UPDATE_BEFORE, 1, 1)));
            assertThat(harness.getOutput()).isEmpty();
            harness.prepareSnapshotPreBarrier(1L);
            harness.snapshot(1L, 1L);
            harness.processElement(new StreamRecord<>(row(RowKind.UPDATE_AFTER, 1, 1)));
            List<String> out = new ArrayList<>();
            for (Object o : harness.getOutput()) {
                out.add(show((RowData) ((StreamRecord<?>) o).getValue()));
            }
            assertThat(out).containsExactly("-U(1,1)", "+U(1,1)");
        }
    }

    /**
     * With a group key (field 1) and a unique key (field 0), an update that keeps both passes as
     * its new row alone; one that changes either key, or has a NULL group key, passes whole.
     */
    @Test
    void testDropsUpdateBeforeOfUpdateThatKeepsItsKeys() throws Exception {
        DropUnchangedUpdatesOperator keys =
                new DropUnchangedUpdatesOperator(KEYED, new int[] {1}, new int[] {0});
        assertThat(
                        run(
                                keys,
                                keyed(RowKind.UPDATE_BEFORE, 1, 7, 5),
                                keyed(RowKind.UPDATE_AFTER, 1, 7, 6)))
                .as("same keys")
                .containsExactly("+U(1,7,6)");
        keys = new DropUnchangedUpdatesOperator(KEYED, new int[] {1}, new int[] {0});
        assertThat(
                        run(
                                keys,
                                keyed(RowKind.UPDATE_BEFORE, 1, 7, 5),
                                keyed(RowKind.UPDATE_AFTER, 1, 8, 5)))
                .as("another group")
                .containsExactly("-U(1,7,5)", "+U(1,8,5)");
        keys = new DropUnchangedUpdatesOperator(KEYED, new int[] {1}, new int[] {0});
        assertThat(
                        run(
                                keys,
                                keyed(RowKind.UPDATE_BEFORE, 1, 7, 5),
                                keyed(RowKind.UPDATE_AFTER, 2, 7, 5)))
                .as("another row")
                .containsExactly("-U(1,7,5)", "+U(2,7,5)");
        keys = new DropUnchangedUpdatesOperator(KEYED, new int[] {1}, new int[] {0});
        assertThat(
                        run(
                                keys,
                                keyed(RowKind.UPDATE_BEFORE, 1, null, 5),
                                keyed(RowKind.UPDATE_AFTER, 1, null, 6)))
                .as("no group")
                .containsExactly("-U(1,null,5)", "+U(1,null,6)");
    }

    private static final RowType KEYED = RowType.of(new IntType(), new IntType(), new IntType());

    private static RowData keyed(RowKind kind, int id, Integer group, int value) {
        GenericRowData row = GenericRowData.of(id, group, value);
        row.setRowKind(kind);
        return row;
    }

    private static List<String> run(DropUnchangedUpdatesOperator operator, RowData... rows)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            harness.endInput();
            List<String> out = new ArrayList<>();
            for (Object o : harness.getOutput()) {
                RowData row = (RowData) ((StreamRecord<?>) o).getValue();
                out.add(
                        String.format(
                                "%s(%s,%s,%s)",
                                row.getRowKind().shortString(),
                                row.getInt(0),
                                row.isNullAt(1) ? "null" : row.getInt(1),
                                row.getInt(2)));
            }
            return out;
        }
    }

    private static List<String> run(RowData... rows) throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = harness()) {
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            harness.endInput();
            List<String> out = new ArrayList<>();
            for (Object o : harness.getOutput()) {
                out.add(show((RowData) ((StreamRecord<?>) o).getValue()));
            }
            return out;
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> harness() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(new DropUnchangedUpdatesOperator(TYPE));
        harness.open();
        return harness;
    }

    private static RowData row(RowKind kind, int a, int b) {
        GenericRowData row = GenericRowData.of(a, b);
        row.setRowKind(kind);
        return row;
    }

    private static String show(RowData row) {
        return row.getRowKind().shortString() + "(" + row.getInt(0) + "," + row.getInt(1) + ")";
    }
}
