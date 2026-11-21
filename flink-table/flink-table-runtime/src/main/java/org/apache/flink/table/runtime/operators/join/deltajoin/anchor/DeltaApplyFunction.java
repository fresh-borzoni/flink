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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * The apply step of a join whose tables are read as of epochs, keyed by the row key of the output.
 * It collects the entries of the {@link AnchorDeltaOperator delta steps} per epoch and, once the
 * watermark completes the epoch, emits its net change.
 *
 * <p>An entry may arrive more than once, so each distinct entry counts once. Per row, a positive
 * sum is the row of the key after the epoch, a negative sum the row it replaces or deletes.
 */
public class DeltaApplyFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final String ENTRIES_STATE_NAME = "delta-apply-entries";

    private final RowType entryType;
    private final int rowKeyArity;
    private final RowType outputType;
    private final boolean emitUpdateBefore;

    private transient MapState<Long, List<RowData>> entries;
    private transient RowDataSerializer entrySerializer;
    private transient RowDataSerializer outputSerializer;
    private transient ProjectedRowData outputOfEntry;

    public DeltaApplyFunction(
            RowType entryType, int rowKeyArity, RowType outputType, boolean emitUpdateBefore) {
        this.entryType = entryType;
        this.rowKeyArity = rowKeyArity;
        this.outputType = outputType;
        this.emitUpdateBefore = emitUpdateBefore;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        entrySerializer = new RowDataSerializer(entryType);
        outputSerializer = new RowDataSerializer(outputType);
        entries =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        ENTRIES_STATE_NAME,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(entrySerializer)));
        int[] outputFields = new int[outputType.getFieldCount()];
        for (int i = 0; i < outputFields.length; i++) {
            outputFields[i] = 1 + rowKeyArity + i;
        }
        outputOfEntry = ProjectedRowData.from(outputFields);
    }

    @Override
    public void processElement(RowData entry, Context ctx, Collector<RowData> out)
            throws Exception {
        long epoch = ctx.timestamp();
        List<RowData> ofEpoch = entries.get(epoch);
        if (ofEpoch == null) {
            ofEpoch = new ArrayList<>();
            ctx.timerService().registerEventTimeTimer(epoch);
        }
        ofEpoch.add(entrySerializer.toBinaryRow(entry).copy());
        entries.put(epoch, ofEpoch);
    }

    @Override
    public void onTimer(long epoch, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        List<RowData> ofEpoch = entries.get(epoch);
        if (ofEpoch == null) {
            return;
        }
        entries.remove(epoch);
        Map<BinaryRowData, Integer> sums = new LinkedHashMap<>();
        for (RowData entry : new LinkedHashSet<>(ofEpoch)) {
            BinaryRowData row =
                    outputSerializer.toBinaryRow(outputOfEntry.replaceRow(entry)).copy();
            row.setRowKind(RowKind.INSERT);
            sums.merge(row, entry.getRowKind() == RowKind.INSERT ? 1 : -1, Integer::sum);
        }
        BinaryRowData after = null;
        BinaryRowData before = null;
        for (Map.Entry<BinaryRowData, Integer> sum : sums.entrySet()) {
            if (sum.getValue() == 0) {
                continue;
            }
            boolean added = sum.getValue() > 0;
            if ((added ? after : before) != null) {
                throw new IllegalStateException(
                        String.format(
                                "Epoch %s of key %s has two rows %s: %s and %s.",
                                epoch,
                                ctx.getCurrentKey(),
                                added ? "after" : "before",
                                added ? after : before,
                                sum.getKey()));
            }
            if (added) {
                after = sum.getKey();
            } else {
                before = sum.getKey();
            }
        }
        if (after == null) {
            if (before != null) {
                before.setRowKind(RowKind.DELETE);
                out.collect(before);
            }
        } else if (before == null) {
            out.collect(after);
        } else {
            if (emitUpdateBefore) {
                before.setRowKind(RowKind.UPDATE_BEFORE);
                out.collect(before);
            }
            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(after);
        }
    }
}
