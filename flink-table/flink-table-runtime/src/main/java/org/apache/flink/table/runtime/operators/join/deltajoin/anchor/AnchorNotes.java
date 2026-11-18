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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The layout of the notes that {@link AnchorLocateOperator locate operators} send to the {@link
 * AnchorGroupRefreshOperator}: the anchor key of a group that a change may affect, the table of the
 * change, whether the change retracts its row and, for tables whose changes refresh only their own
 * lines (see {@link AnchorJoinTree#canRefreshRowsOf}), the changed row itself.
 *
 * <p>Fields: the anchor key fields, then {@code leaf INT}, {@code change TINYINT} (see {@link
 * #ADD}, {@link #RETRACT}, {@link #REPLACED}) and one nullable row field per table that sends its
 * rows.
 */
public final class AnchorNotes implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The row is added to the group, or replaces an earlier version of it. */
    public static final byte ADD = 0;

    /** The row leaves the group. */
    public static final byte RETRACT = 1;

    /**
     * The row is replaced: the next note from the same sender adds its new version to the same
     * group. A refresh waits for that note.
     */
    public static final byte REPLACED = 2;

    private final RowType type;
    private final int keyArity;

    /** Per table, the field that holds its changed rows, or -1 if it sends none. */
    private final int[] rowFields;

    private final RowType[] leafTypes;

    private transient RowData.FieldGetter[] keyGetters;

    private AnchorNotes(RowType type, int keyArity, int[] rowFields, RowType[] leafTypes) {
        this.type = type;
        this.keyArity = keyArity;
        this.rowFields = rowFields;
        this.leafTypes = leafTypes;
    }

    /**
     * @param withRows the tables that send their changed rows; each must satisfy {@link
     *     AnchorJoinTree#canRefreshRowsOf}
     */
    public static AnchorNotes of(AnchorJoinTree tree, boolean[] withRows) {
        Preconditions.checkArgument(withRows.length == tree.getNumLeaves());
        RowType anchorType = tree.getLeafType(tree.getAnchorLeaf());
        int[] keyFields = tree.getAnchorKeyFields();
        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < keyFields.length; i++) {
            fields.add(new RowType.RowField("key" + i, anchorType.getTypeAt(keyFields[i])));
        }
        fields.add(new RowType.RowField("leaf", new IntType(false)));
        fields.add(new RowType.RowField("change", new TinyIntType(false)));
        int[] rowFields = new int[tree.getNumLeaves()];
        RowType[] leafTypes = new RowType[tree.getNumLeaves()];
        for (int leaf = 0; leaf < rowFields.length; leaf++) {
            leafTypes[leaf] = tree.getLeafType(leaf);
            rowFields[leaf] = -1;
            if (withRows[leaf]) {
                Preconditions.checkArgument(
                        tree.canRefreshRowsOf(leaf),
                        "Table %s cannot refresh only its own lines.",
                        leaf);
                rowFields[leaf] = fields.size();
                fields.add(new RowType.RowField("row" + leaf, leafTypes[leaf].copy(true)));
            }
        }
        return new AnchorNotes(new RowType(false, fields), keyFields.length, rowFields, leafTypes);
    }

    public RowType getType() {
        return type;
    }

    /** The fields of a note that hold the anchor key. */
    public int[] getKeyFields() {
        int[] fields = new int[keyArity];
        for (int i = 0; i < keyArity; i++) {
            fields[i] = i;
        }
        return fields;
    }

    /** Whether notes of the given table carry the changed row. */
    public boolean carriesRows(int leaf) {
        return rowFields[leaf] >= 0;
    }

    /** The note that a change of the given row of the given table may affect the given group. */
    RowData note(RowData anchorKey, int leaf, RowData row) {
        boolean retracts =
                row.getRowKind() == RowKind.UPDATE_BEFORE || row.getRowKind() == RowKind.DELETE;
        return note(anchorKey, leaf, row, retracts ? RETRACT : ADD);
    }

    /**
     * The note of the given change of the given row of the given table for the given group. A note
     * without the row refreshes the whole group.
     */
    RowData note(RowData anchorKey, int leaf, @Nullable RowData row, byte change) {
        if (keyGetters == null) {
            keyGetters = new RowData.FieldGetter[keyArity];
            for (int i = 0; i < keyArity; i++) {
                keyGetters[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
            }
        }
        GenericRowData note = new GenericRowData(type.getFieldCount());
        for (int i = 0; i < keyArity; i++) {
            note.setField(i, keyGetters[i].getFieldOrNull(anchorKey));
        }
        note.setField(keyArity, leaf);
        note.setField(keyArity + 1, change);
        if (rowFields[leaf] >= 0) {
            note.setField(rowFields[leaf], row);
        }
        return note;
    }

    int leafOf(RowData note) {
        return note.getInt(keyArity);
    }

    byte changeOf(RowData note) {
        return note.getByte(keyArity + 1);
    }

    /** The changed row of a note, or null if its table sends no rows. */
    @Nullable
    RowData rowOf(RowData note) {
        int leaf = leafOf(note);
        int field = rowFields[leaf];
        if (field < 0 || note.isNullAt(field)) {
            return null;
        }
        return note.getRow(field, leafTypes[leaf].getFieldCount());
    }
}
