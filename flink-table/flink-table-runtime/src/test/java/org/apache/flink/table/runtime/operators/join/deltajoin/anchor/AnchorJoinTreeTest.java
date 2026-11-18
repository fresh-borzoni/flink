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

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AnchorJoinTree}. */
class AnchorJoinTreeTest {

    private static final RowType ROW = RowType.of(new IntType(), new IntType());

    /** A, B and C, where B and C are joined to A on A's key: both refresh their own lines. */
    @Test
    void testTablesJoinedToTheAnchorRefreshTheirRows() {
        AnchorJoinTree tree = tree(0);
        assertThat(tree.canRefreshRowsOf(0)).isFalse();
        assertThat(tree.canRefreshRowsOf(1)).isTrue();
        assertThat(tree.canRefreshRowsOf(2)).isTrue();
    }

    /**
     * A, B and C, where B is joined to A on A's key and C to B: the old lines of a row of B depend
     * on C as well, so B refreshes the whole group.
     */
    @Test
    void testTableWithATableBeyondItRefreshesTheWholeGroup() {
        AnchorJoinTree tree = tree(1);
        assertThat(tree.canRefreshRowsOf(1)).isFalse();
        assertThat(tree.canRefreshRowsOf(2)).isFalse();
    }

    /**
     * A LEFT JOIN B LEFT JOIN C, where B's field 1 is A's key and C's field 1 equals B's field 1:
     * the joins tie C's field 1 to the anchor key, so a row of C names its group itself.
     */
    @Test
    void testRowsOfAChainHoldTheAnchorKey() {
        AnchorJoinTree tree = chain(FlinkJoinType.LEFT, 1, null);
        assertThat(tree.getAnchorKeyFieldsOf(1)).containsExactly(1);
        assertThat(tree.getAnchorKeyFieldsOf(2)).containsExactly(1);
        assertThat(tree.getLocateTable(2)).isEqualTo(2);
        assertThat(tree.getLocateLookups(2)).isEmpty();
    }

    /** C joined on B's field 0, which is not the anchor key: C is located through B. */
    @Test
    void testChainTableJoinedOnOtherFieldsIsLocatedThroughItsParent() {
        AnchorJoinTree tree = chain(FlinkJoinType.LEFT, 0, null);
        assertThat(tree.getAnchorKeyFieldsOf(2)).isNull();
        assertThat(tree.getLocateTable(2)).isEqualTo(1);
        // the lookup from C to B
        assertThat(tree.getLocateLookups(2)).containsExactly(4);
    }

    /** A RIGHT join keeps rows of C without a match in B: they do not hold the anchor key. */
    @Test
    void testRowsKeptWithoutAMatchDoNotHoldTheAnchorKey() {
        AnchorJoinTree tree = chain(FlinkJoinType.RIGHT, 1, null);
        assertThat(tree.getAnchorKeyFieldsOf(2)).isNull();
        assertThat(tree.getLocateTable(2)).isEqualTo(1);
    }

    @Test
    void testOnlyTablesThatHoldTheAnchorKeyAreReadByGroup() {
        assertThat(chain(FlinkJoinType.LEFT, 1, new int[] {-1, -1, 5}).getGroupLookup(2))
                .isEqualTo(5);
        assertThatThrownBy(() -> chain(FlinkJoinType.LEFT, 0, new int[] {-1, -1, 5}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A LEFT JOIN B ON A.0 = B.1, then C joined to B by the given join type on B's given field =
     * C.1.
     */
    private static AnchorJoinTree chain(
            FlinkJoinType secondJoin, int bField, @Nullable int[] groupLookups) {
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[5];
        for (int leaf = 0; leaf < 3; leaf++) {
            nodes[leaf] = AnchorJoinTree.Node.leaf(leaf, 3, 2);
        }
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
                        secondJoin,
                        1,
                        nodes[3].getOutput().concat(nodes[2].getOutput()),
                        Collections.emptyList());
        AnchorJoinTree.Edge[] edges = {
            new AnchorJoinTree.Edge(0, new int[] {0}, 1, new int[] {1}, 1, 2),
            new AnchorJoinTree.Edge(1, new int[] {bField}, 2, new int[] {1}, 3, 4)
        };
        return new AnchorJoinTree(
                new RowType[] {ROW, ROW, ROW}, nodes, edges, 0, new int[] {0}, 0, groupLookups);
    }

    /** A tree of A, B and C: B joined to A, and C joined to the given table on its field 1. */
    private static AnchorJoinTree tree(int cJoinedTo) {
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[5];
        for (int leaf = 0; leaf < 3; leaf++) {
            nodes[leaf] = AnchorJoinTree.Node.leaf(leaf, 3, 2);
        }
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
        AnchorJoinTree.Edge[] edges = {
            new AnchorJoinTree.Edge(0, new int[] {0}, 1, new int[] {1}, 1, 2),
            new AnchorJoinTree.Edge(cJoinedTo, new int[] {0}, 2, new int[] {1}, 3, 4)
        };
        return new AnchorJoinTree(new RowType[] {ROW, ROW, ROW}, nodes, edges, 0, new int[] {0}, 0);
    }
}
