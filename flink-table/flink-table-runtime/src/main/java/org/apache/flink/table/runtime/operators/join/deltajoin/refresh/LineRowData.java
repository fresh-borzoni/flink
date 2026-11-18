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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.variant.Variant;

/**
 * A {@link RowData} view of a line: one row per table of a join, null for tables that were padded
 * with NULLs. Field {@code i} of the view is field {@code fields[i]} of the row of table {@code
 * leaves[i]}; fields of padded tables are NULL.
 */
public final class LineRowData implements RowData {

    private final int[] leaves;
    private final int[] fields;
    private RowData[] line;

    public LineRowData(int[] leaves, int[] fields) {
        this.leaves = leaves;
        this.fields = fields;
    }

    public LineRowData replace(RowData[] line) {
        this.line = line;
        return this;
    }

    private RowData row(int pos) {
        return line[leaves[pos]];
    }

    @Override
    public int getArity() {
        return leaves.length;
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind kind) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int pos) {
        RowData row = row(pos);
        return row == null || row.isNullAt(fields[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row(pos).getBoolean(fields[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return row(pos).getByte(fields[pos]);
    }

    @Override
    public short getShort(int pos) {
        return row(pos).getShort(fields[pos]);
    }

    @Override
    public int getInt(int pos) {
        return row(pos).getInt(fields[pos]);
    }

    @Override
    public long getLong(int pos) {
        return row(pos).getLong(fields[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return row(pos).getFloat(fields[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return row(pos).getDouble(fields[pos]);
    }

    @Override
    public StringData getString(int pos) {
        return row(pos).getString(fields[pos]);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row(pos).getDecimal(fields[pos], precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row(pos).getTimestamp(fields[pos], precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row(pos).getRawValue(fields[pos]);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row(pos).getBinary(fields[pos]);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row(pos).getArray(fields[pos]);
    }

    @Override
    public MapData getMap(int pos) {
        return row(pos).getMap(fields[pos]);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return row(pos).getRow(fields[pos], numFields);
    }

    @Override
    public Variant getVariant(int pos) {
        return row(pos).getVariant(fields[pos]);
    }

    @Override
    public Bitmap getBitmap(int pos) {
        return row(pos).getBitmap(fields[pos]);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("A line view is not comparable.");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("A line view is not hashable.");
    }
}
