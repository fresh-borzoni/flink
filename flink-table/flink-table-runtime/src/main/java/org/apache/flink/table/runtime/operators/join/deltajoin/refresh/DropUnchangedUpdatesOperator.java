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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Drops an UPDATE_BEFORE directly followed by an UPDATE_AFTER with the same row, an update of
 * columns the query does not read. Runs where both rows of an update are adjacent, before they are
 * redistributed; a held UPDATE_BEFORE is passed on before watermarks, barriers and the end of
 * input.
 *
 * <p>Given a group key and a unique key, it also drops the UPDATE_BEFORE of an update that keeps
 * both keys and has no NULL in the group key: the UPDATE_AFTER alone refreshes the same group.
 */
public class DropUnchangedUpdatesOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final @Nullable int[] groupKey;
    private final @Nullable int[] uniqueKey;

    private transient RowDataSerializer serializer;
    private transient RowData.FieldGetter[] groupKeyGetters;
    private transient RowData.FieldGetter[] uniqueKeyGetters;
    private transient @Nullable BinaryRowData before;
    private transient @Nullable StreamRecord<RowData> beforeRecord;
    private transient long dropped;

    public DropUnchangedUpdatesOperator(RowType rowType) {
        this(rowType, null, null);
    }

    /**
     * @param groupKey the fields of the group key; with {@code uniqueKey}, the UPDATE_BEFORE of an
     *     update that keeps both keys is dropped
     * @param uniqueKey the fields of a unique key
     */
    public DropUnchangedUpdatesOperator(
            RowType rowType, @Nullable int[] groupKey, @Nullable int[] uniqueKey) {
        Preconditions.checkArgument(
                (groupKey == null) == (uniqueKey == null)
                        && (uniqueKey == null || uniqueKey.length > 0),
                "Both keys, and a unique key with fields, are needed.");
        this.rowType = rowType;
        this.groupKey = groupKey;
        this.uniqueKey = uniqueKey;
    }

    @Override
    public void open() throws Exception {
        super.open();
        serializer = new RowDataSerializer(rowType);
        groupKeyGetters = getters(groupKey);
        uniqueKeyGetters = getters(uniqueKey);
        getRuntimeContext().getMetricGroup().gauge("numUnchangedUpdatesDropped", () -> dropped);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        if (row.getRowKind() == RowKind.UPDATE_AFTER && before != null) {
            BinaryRowData after = serializer.toBinaryRow(row).copy();
            after.setRowKind(RowKind.UPDATE_BEFORE);
            if (after.equals(before)) {
                before = null;
                beforeRecord = null;
                dropped++;
                return;
            }
            if (keepsKeys(before, after)) {
                before = null;
                beforeRecord = null;
                output.collect(element);
                return;
            }
        }
        passBefore();
        if (row.getRowKind() == RowKind.UPDATE_BEFORE) {
            before = serializer.toBinaryRow(row).copy();
            beforeRecord = element.copy(before);
            return;
        }
        output.collect(element);
    }

    private RowData.FieldGetter[] getters(@Nullable int[] fields) {
        if (fields == null) {
            return new RowData.FieldGetter[0];
        }
        RowData.FieldGetter[] getters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            getters[i] = RowData.createFieldGetter(rowType.getTypeAt(fields[i]), fields[i]);
        }
        return getters;
    }

    /** Whether the rows have the same unique key and the same group key without NULLs. */
    private boolean keepsKeys(RowData before, RowData after) {
        if (groupKeyGetters.length == 0) {
            return false;
        }
        for (RowData.FieldGetter getter : groupKeyGetters) {
            // a row with a NULL in the group key is emitted padded from the change, which needs
            // its retraction
            Object value = getter.getFieldOrNull(before);
            if (value == null || !value.equals(getter.getFieldOrNull(after))) {
                return false;
            }
        }
        for (RowData.FieldGetter getter : uniqueKeyGetters) {
            if (!Objects.equals(getter.getFieldOrNull(before), getter.getFieldOrNull(after))) {
                return false;
            }
        }
        return true;
    }

    private void passBefore() {
        if (beforeRecord != null) {
            output.collect(beforeRecord);
            before = null;
            beforeRecord = null;
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        passBefore();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        passBefore();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void endInput() {
        passBefore();
    }

    @Override
    public void finish() throws Exception {
        passBefore();
        super.finish();
    }
}
