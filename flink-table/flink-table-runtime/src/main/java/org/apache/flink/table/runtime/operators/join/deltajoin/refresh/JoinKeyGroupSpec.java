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

import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/** How a {@link JoinKeyGroupRefreshOperator} reads the group of a join key. */
public final class JoinKeyGroupSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    final FlinkJoinType joinType;
    final RowType leftType;
    final RowType rightType;
    final @Nullable GeneratedFilterCondition condition;
    final GroupLookup leftLookup;
    final GroupLookup rightLookup;
    final RowDataKeySelector leftUniqueKey;
    final RowDataKeySelector rightUniqueKey;
    final long leftCacheSize;
    final long rightCacheSize;
    final int scopedSide;
    final int[] scopedUniqueKey;
    final int[] scopedJoinKey;
    final int[] parentJoinKey;

    /**
     * @param joinType INNER, LEFT, RIGHT or FULL
     * @param leftType the rows of the left input
     * @param rightType the rows of the right input
     * @param condition the join condition, on the left row followed by the right row
     * @param leftLookup reads the left rows of a join key
     * @param rightLookup reads the right rows of a join key
     * @param leftUniqueKey a unique key of the left rows
     * @param rightUniqueKey a unique key of the right rows
     * @param leftCacheSize the number of join keys whose left rows are cached; 0 disables it
     * @param rightCacheSize the number of join keys whose right rows are cached; 0 disables it
     */
    public JoinKeyGroupSpec(
            FlinkJoinType joinType,
            RowType leftType,
            RowType rightType,
            @Nullable GeneratedFilterCondition condition,
            GroupLookup leftLookup,
            GroupLookup rightLookup,
            RowDataKeySelector leftUniqueKey,
            RowDataKeySelector rightUniqueKey,
            long leftCacheSize,
            long rightCacheSize) {
        this(
                joinType,
                leftType,
                rightType,
                condition,
                leftLookup,
                rightLookup,
                leftUniqueKey,
                rightUniqueKey,
                leftCacheSize,
                rightCacheSize,
                -1,
                new int[0],
                new int[0],
                new int[0]);
    }

    /**
     * @param scopedSide 0 or 1 if the other input has at most one row per join key: a change of a
     *     row of this input then refreshes only the rows of that row; -1 otherwise
     * @param scopedUniqueKey the fields of the unique key of the scoped input
     * @param scopedJoinKey the fields of the join key of the scoped input
     * @param parentJoinKey the fields of the join key of the other input, which hold its unique key
     */
    public JoinKeyGroupSpec(
            FlinkJoinType joinType,
            RowType leftType,
            RowType rightType,
            @Nullable GeneratedFilterCondition condition,
            GroupLookup leftLookup,
            GroupLookup rightLookup,
            RowDataKeySelector leftUniqueKey,
            RowDataKeySelector rightUniqueKey,
            long leftCacheSize,
            long rightCacheSize,
            int scopedSide,
            int[] scopedUniqueKey,
            int[] scopedJoinKey,
            int[] parentJoinKey) {
        Preconditions.checkArgument(
                scopedSide < 0 || scopedUniqueKey.length > 0,
                "The rows of the scoped input need a unique key.");
        Preconditions.checkArgument(
                (leftCacheSize > 0) == (rightCacheSize > 0),
                "Either both inputs are cached or none.");
        this.joinType = joinType;
        this.leftType = leftType;
        this.rightType = rightType;
        this.condition = condition;
        this.leftLookup = leftLookup;
        this.rightLookup = rightLookup;
        this.leftUniqueKey = leftUniqueKey;
        this.rightUniqueKey = rightUniqueKey;
        this.leftCacheSize = leftCacheSize;
        this.rightCacheSize = rightCacheSize;
        this.scopedSide = scopedSide;
        this.scopedUniqueKey = scopedUniqueKey;
        this.scopedJoinKey = scopedJoinKey;
        this.parentJoinKey = parentJoinKey;
    }
}
