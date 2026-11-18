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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.CalcCollectionCollector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Turns the lines of a group into the rows the operator emits, indexed by what tells them apart
 * within the group, the row key:
 *
 * <ul>
 *   <li>the primary key of the output table, if the operator reads its output back;
 *   <li>otherwise the unique keys of the tables of the line, NULL for padded tables;
 *   <li>if a table has no unique key, the emitted row itself and its number among equal rows, so
 *       that equal rows are kept as often as they occur.
 * </ul>
 *
 * <p>Two rows of a group with the same row key, or the same value of a declared key, are
 * inconsistent: the reads of the group saw a row twice, in two versions, or a constant that
 * replaces NULL in a key column of the output is also a value of that column. Only the first such
 * row is kept.
 *
 * <p>Rows are normalized to {@link BinaryRowData} with {@link RowKind#INSERT}, so that equal rows
 * compare equal. Not thread safe.
 */
final class GroupRows {

    private enum RowKeyKind {
        OUTPUT_KEY,
        TABLE_KEYS,
        ROW_AND_OCCURRENCE
    }

    private final RowKeyKind rowKeyKind;
    private final RowType rowKeyType;
    private final LineRowData joined;
    private final RowDataSerializer outputSerializer;
    private final RowDataSerializer rowKeySerializer;
    private final @Nullable RowDataKeySelector outputKey;
    private final @Nullable FlatMapFunction<RowData, RowData> calc;
    private final @Nullable CalcCollectionCollector collector;

    /** The primary key of the output from a joined row; for row keys of lines. */
    private final @Nullable FlatMapFunction<RowData, RowData> keyCalc;

    private final @Nullable CalcCollectionCollector keyCollector;

    /** Per table, the getters of its key fields; for {@link RowKeyKind#TABLE_KEYS}. */
    private final RowData.FieldGetter[][] tableKeyGetters;

    /** The getters of all emitted fields; for {@link RowKeyKind#ROW_AND_OCCURRENCE}. */
    private final RowData.FieldGetter[] outputGetters;

    private final RowData.FieldGetter[][] declaredKeyGetters;
    private final RowDataSerializer[] declaredKeySerializers;

    GroupRows(GroupRefreshSpec spec, RuntimeContext runtimeContext) throws Exception {
        this.joined = new LineRowData(spec.joinedTables, spec.joinedFields);
        this.outputSerializer = new RowDataSerializer(spec.outputType);
        this.rowKeyKind = rowKeyKind(spec);
        this.rowKeyType = rowKeyType(spec);
        this.rowKeySerializer = new RowDataSerializer(rowKeyType);
        this.outputKey = spec.outputKey == null ? null : spec.outputKey.copy();
        if (spec.outputCalc != null) {
            calc = spec.outputCalc.newInstance(runtimeContext.getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(calc, runtimeContext);
            FunctionUtils.openFunction(calc, DefaultOpenContext.INSTANCE);
            collector = new CalcCollectionCollector(outputSerializer);
        } else {
            calc = null;
            collector = null;
        }
        if (spec.outputKeyCalc != null && rowKeyKind == RowKeyKind.OUTPUT_KEY) {
            keyCalc = spec.outputKeyCalc.newInstance(runtimeContext.getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(keyCalc, runtimeContext);
            FunctionUtils.openFunction(keyCalc, DefaultOpenContext.INSTANCE);
            keyCollector = new CalcCollectionCollector(rowKeySerializer);
        } else {
            keyCalc = null;
            keyCollector = null;
        }

        tableKeyGetters = new RowData.FieldGetter[spec.tableTypes.length][];
        for (int t = 0; t < spec.tableTypes.length; t++) {
            int[] key = spec.tableKeys[t];
            tableKeyGetters[t] = new RowData.FieldGetter[key.length];
            for (int i = 0; i < key.length; i++) {
                tableKeyGetters[t][i] =
                        RowData.createFieldGetter(spec.tableTypes[t].getTypeAt(key[i]), key[i]);
            }
        }
        outputGetters = getters(spec.outputType, allFields(spec.outputType.getFieldCount()));

        declaredKeyGetters = new RowData.FieldGetter[spec.declaredKeys.length][];
        declaredKeySerializers = new RowDataSerializer[spec.declaredKeys.length];
        for (int k = 0; k < spec.declaredKeys.length; k++) {
            declaredKeyGetters[k] = getters(spec.outputType, spec.declaredKeys[k]);
            LogicalType[] types = new LogicalType[spec.declaredKeys[k].length];
            for (int i = 0; i < types.length; i++) {
                types[i] = spec.outputType.getTypeAt(spec.declaredKeys[k][i]);
            }
            declaredKeySerializers[k] = new RowDataSerializer(types);
        }
    }

    /** The type of the row keys, which tell the rows of a group apart. */
    static RowType rowKeyType(GroupRefreshSpec spec) {
        List<LogicalType> types = new ArrayList<>();
        switch (rowKeyKind(spec)) {
            case OUTPUT_KEY:
                return spec.outputKey.getProducedType().toRowType();
            case TABLE_KEYS:
                for (int t = 0; t < spec.tableTypes.length; t++) {
                    for (int field : spec.tableKeys[t]) {
                        // padded tables have NULL keys
                        types.add(spec.tableTypes[t].getTypeAt(field).copy(true));
                    }
                }
                break;
            default:
                for (LogicalType type : spec.outputType.getChildren()) {
                    types.add(type.copy(true));
                }
                types.add(new IntType(false));
        }
        return RowType.of(types.toArray(new LogicalType[0]));
    }

    private static RowKeyKind rowKeyKind(GroupRefreshSpec spec) {
        if (spec.outputKey != null) {
            return RowKeyKind.OUTPUT_KEY;
        }
        for (int[] key : spec.tableKeys) {
            if (key.length == 0) {
                return RowKeyKind.ROW_AND_OCCURRENCE;
            }
        }
        return RowKeyKind.TABLE_KEYS;
    }

    RowType getRowKeyType() {
        return rowKeyType;
    }

    RowDataSerializer getOutputSerializer() {
        return outputSerializer;
    }

    /**
     * Whether {@link #rowKeyOf} can tell the row key of a line from its unique keys and join keys
     * alone. The projection of the output is never applied to such partial lines: it may read
     * columns that cannot be NULL, or call functions that do not accept NULL.
     */
    boolean knowsRowKeysOfLines() {
        return rowKeyKind == RowKeyKind.TABLE_KEYS || keyCalc != null;
    }

    /**
     * The row key of the row that the given line emits, from the unique keys and join keys of the
     * line; null if the line emits no row.
     */
    @Nullable
    RowData rowKeyOf(RowData[] line) throws Exception {
        if (rowKeyKind == RowKeyKind.TABLE_KEYS) {
            return rowKey(line, null, null);
        }
        keyCollector.reset();
        keyCalc.flatMap(joined.replace(line), keyCollector);
        if (keyCollector.getCollection().isEmpty()) {
            return null;
        }
        return rowKeySerializer.toBinaryRow(keyCollector.getCollection().iterator().next()).copy();
    }

    /**
     * Whether the row key of a row depends on its line only. Numbers among equal rows depend on the
     * whole group.
     */
    boolean rowKeysDependOnLinesOnly() {
        return rowKeyKind != RowKeyKind.ROW_AND_OCCURRENCE;
    }

    /** The row keys of the rows that a complete line emits. */
    List<RowData> rowKeysOf(RowData[] line) throws Exception {
        Preconditions.checkState(rowKeysDependOnLinesOnly(), "Row keys depend on the group.");
        List<BinaryRowData> rows = project(line);
        List<RowData> keys = new ArrayList<>(rows.size());
        for (BinaryRowData row : rows) {
            keys.add(rowKey(line, row, null));
        }
        return keys;
    }

    /** The emitted rows of one line: none if the projection filters it out. */
    List<BinaryRowData> project(RowData[] line) throws Exception {
        RowData row = joined.replace(line);
        if (calc == null) {
            return Collections.singletonList(normalize(row));
        }
        collector.reset();
        calc.flatMap(row, collector);
        List<BinaryRowData> rows = new ArrayList<>(collector.getCollection().size());
        for (RowData projected : collector.getCollection()) {
            rows.add(normalize(projected));
        }
        return rows;
    }

    /**
     * The emitted rows of the given lines by their row key. If rows are inconsistent, keeps the
     * first of them and calls {@code onInconsistent}.
     */
    Map<RowData, BinaryRowData> rowsOf(List<RowData[]> lines, Runnable onInconsistent)
            throws Exception {
        return rowsOf(lines, new Seen(), onInconsistent);
    }

    /**
     * The emitted rows of the given lines, which are one page of the lines of a group, by their row
     * key. Rows inconsistent with each other or with the rows of earlier pages, which {@code seen}
     * remembers, are dropped, and {@code onInconsistent} is called.
     */
    Map<RowData, BinaryRowData> rowsOf(List<RowData[]> lines, Seen seen, Runnable onInconsistent)
            throws Exception {
        Map<RowData, BinaryRowData> rows = new LinkedHashMap<>();
        Map<BinaryRowData, Integer> occurrences =
                rowKeyKind == RowKeyKind.ROW_AND_OCCURRENCE ? new HashMap<>() : null;
        boolean consistent = true;
        for (RowData[] line : lines) {
            for (BinaryRowData row : project(line)) {
                RowData rowKey = rowKey(line, row, occurrences);
                if (seen.rowKeys.contains(rowKey) || !addDeclaredKeys(row, seen.declaredKeys)) {
                    consistent = false;
                    continue;
                }
                seen.rowKeys.add(rowKey);
                rows.put(rowKey, row);
            }
        }
        if (!consistent) {
            onInconsistent.run();
        }
        return rows;
    }

    /** The values of a row, for messages. */
    static String format(RowData row, RowType type) {
        StringJoiner values = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < type.getFieldCount(); i++) {
            values.add(
                    String.valueOf(
                            RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(row)));
        }
        return values.toString();
    }

    /** The row keys and declared keys of the rows of a group read so far. */
    final class Seen {
        final Set<RowData> rowKeys = new HashSet<>();
        final List<Set<BinaryRowData>> declaredKeys = new ArrayList<>(declaredKeyGetters.length);

        Seen() {
            for (int k = 0; k < declaredKeyGetters.length; k++) {
                declaredKeys.add(new HashSet<>());
            }
        }
    }

    void close() throws Exception {
        if (calc != null) {
            FunctionUtils.closeFunction(calc);
        }
        if (keyCalc != null) {
            FunctionUtils.closeFunction(keyCalc);
        }
    }

    private BinaryRowData normalize(RowData row) {
        BinaryRowData binary = outputSerializer.toBinaryRow(row).copy();
        binary.setRowKind(RowKind.INSERT);
        return binary;
    }

    private RowData rowKey(
            RowData[] line,
            @Nullable BinaryRowData row,
            @Nullable Map<BinaryRowData, Integer> occurrences)
            throws Exception {
        GenericRowData key;
        switch (rowKeyKind) {
            case OUTPUT_KEY:
                RowData selected = outputKey.getKey(row);
                return selected instanceof BinaryRowData
                        ? ((BinaryRowData) selected).copy()
                        : selected;
            case TABLE_KEYS:
                key = new GenericRowData(rowKeyType.getFieldCount());
                int pos = 0;
                for (int t = 0; t < tableKeyGetters.length; t++) {
                    for (RowData.FieldGetter getter : tableKeyGetters[t]) {
                        key.setField(
                                pos++, line[t] == null ? null : getter.getFieldOrNull(line[t]));
                    }
                }
                break;
            default:
                key = new GenericRowData(outputGetters.length + 1);
                for (int i = 0; i < outputGetters.length; i++) {
                    key.setField(i, outputGetters[i].getFieldOrNull(row));
                }
                key.setField(outputGetters.length, occurrences.merge(row, 1, Integer::sum) - 1);
        }
        return rowKeySerializer.toBinaryRow(key).copy();
    }

    /** Adds the declared keys of the row, unless one of them was seen before. */
    private boolean addDeclaredKeys(BinaryRowData row, List<Set<BinaryRowData>> seen) {
        if (declaredKeyGetters.length == 0) {
            return true;
        }
        List<BinaryRowData> keys = new ArrayList<>(declaredKeyGetters.length);
        for (int k = 0; k < declaredKeyGetters.length; k++) {
            RowData.FieldGetter[] getters = declaredKeyGetters[k];
            GenericRowData key = new GenericRowData(getters.length);
            for (int i = 0; i < getters.length; i++) {
                key.setField(i, getters[i].getFieldOrNull(row));
            }
            // compared in binary form, as keyed state and consumers compare them: NULLs are equal
            // to each other and binary values by content
            BinaryRowData binary = declaredKeySerializers[k].toBinaryRow(key).copy();
            if (seen.get(k).contains(binary)) {
                return false;
            }
            keys.add(binary);
        }
        for (int k = 0; k < keys.size(); k++) {
            seen.get(k).add(keys.get(k));
        }
        return true;
    }

    private static RowData.FieldGetter[] getters(RowType type, int[] fields) {
        RowData.FieldGetter[] getters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            getters[i] = RowData.createFieldGetter(type.getTypeAt(fields[i]), fields[i]);
        }
        return getters;
    }

    private static int[] allFields(int count) {
        int[] fields = new int[count];
        for (int i = 0; i < count; i++) {
            fields[i] = i;
        }
        return fields;
    }
}
