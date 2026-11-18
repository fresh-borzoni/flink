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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Joins the rows of one group: all rows of both inputs with the same join key. Rows are matched by
 * the join condition (whose equalities on the join key hold by construction), and rows of a
 * preserved input without a match are padded with nulls, after all matching was done. The result
 * are lines: the left and the right row, null if padded.
 */
final class GroupJoiner {

    private final FlinkJoinType joinType;
    private final @Nullable FilterCondition condition;
    private final JoinedRowData joined = new JoinedRowData();

    GroupJoiner(FlinkJoinType joinType, @Nullable FilterCondition condition) {
        Preconditions.checkArgument(
                joinType == FlinkJoinType.INNER
                        || joinType == FlinkJoinType.LEFT
                        || joinType == FlinkJoinType.RIGHT
                        || joinType == FlinkJoinType.FULL,
                "Unsupported join type %s",
                joinType);
        this.joinType = joinType;
        this.condition = condition;
    }

    List<RowData[]> join(List<RowData> left, List<RowData> right) throws Exception {
        boolean[] rightMatched = new boolean[right.size()];
        List<RowData[]> out = joinPart(left, true, right, rightMatched);
        out.addAll(padUnmatched(right, false, rightMatched));
        return out;
    }

    /**
     * Joins a part of the rows of one input with all rows of the other: the lines of the part, a
     * row of the part padded if it matches nothing and its input is preserved. Rows of the other
     * input that match are marked in {@code otherMatched}; {@link #padUnmatched} pads the others
     * after the last part.
     */
    List<RowData[]> joinPart(
            List<RowData> part, boolean partIsLeft, List<RowData> other, boolean[] otherMatched)
            throws Exception {
        List<RowData[]> out = new ArrayList<>();
        for (RowData row : part) {
            boolean matched = false;
            for (int i = 0; i < other.size(); i++) {
                RowData l = partIsLeft ? row : other.get(i);
                RowData r = partIsLeft ? other.get(i) : row;
                if (matches(l, r)) {
                    out.add(new RowData[] {l, r});
                    matched = true;
                    otherMatched[i] = true;
                }
            }
            if (!matched && preserves(partIsLeft)) {
                out.add(pad(row, partIsLeft));
            }
        }
        return out;
    }

    /** The padded lines of the rows of the other input that no part matched, if it is preserved. */
    List<RowData[]> padUnmatched(List<RowData> other, boolean otherIsLeft, boolean[] otherMatched) {
        List<RowData[]> out = new ArrayList<>();
        if (preserves(otherIsLeft)) {
            for (int i = 0; i < other.size(); i++) {
                if (!otherMatched[i]) {
                    out.add(pad(other.get(i), otherIsLeft));
                }
            }
        }
        return out;
    }

    boolean matches(RowData left, RowData right) throws Exception {
        return condition == null
                || condition.apply(
                        FilterCondition.Context.INVALID_CONTEXT, joined.replace(left, right));
    }

    /** Whether rows of the given input appear in the output even without a match. */
    boolean preserves(boolean isLeft) {
        return isLeft ? preservesLeft() : preservesRight();
    }

    /** The line of a row of a preserved input that has no match. */
    static RowData[] pad(RowData row, boolean isLeft) {
        return isLeft ? new RowData[] {row, null} : new RowData[] {null, row};
    }

    private boolean preservesLeft() {
        return joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
    }

    private boolean preservesRight() {
        return joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
    }
}
