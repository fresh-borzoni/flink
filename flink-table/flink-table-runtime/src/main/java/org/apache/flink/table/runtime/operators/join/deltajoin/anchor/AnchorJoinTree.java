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
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.LineRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * The join tree evaluated by an anchor delta join.
 *
 * <p>The leaves of the tree are the input tables. Every join connects one table of its left subtree
 * with one table of its right subtree through equality columns (an {@link Edge}), so the tables and
 * edges form a tree as well. The anchor is the table whose row is present in every output row of
 * the join.
 *
 * <p>A line is an array with one row per table; a table that was padded with NULLs has no row.
 * Field positions are described by {@link FieldMap}s from a node's output fields to (table, field)
 * pairs, which lets conditions and filters of every node read a line without copying it.
 */
public final class AnchorJoinTree implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType[] leafTypes;
    private final Node[] nodes;
    private final Edge[] edges;
    private final int anchorLeaf;
    private final int[] anchorKeyFields;
    private final int anchorLookup;

    /** For every table, the lookups that lead from a row of that table to the anchor table. */
    private final int[][] pathsToAnchor;

    /** For every table, the table next to it on the way to the anchor table, or -1. */
    private final int[] nextTowardsAnchor;

    /**
     * For every table joined directly to the anchor table, the fields whose values are the anchor
     * key; null for other tables and if the equality columns do not cover the anchor key with the
     * same types.
     */
    private final int[][] anchorKeyFieldsInNeighbour;

    /** Per table, the fields holding the anchor key, see {@link #getAnchorKeyFieldsOf}. */
    private final int[][] anchorKeyFieldsOf;

    /** For every table, the lookup that returns its rows of a whole group, or -1. */
    private final int[] groupLookups;

    public AnchorJoinTree(
            RowType[] leafTypes,
            Node[] nodes,
            Edge[] edges,
            int anchorLeaf,
            int[] anchorKeyFields,
            int anchorLookup) {
        this(leafTypes, nodes, edges, anchorLeaf, anchorKeyFields, anchorLookup, null);
    }

    /**
     * @param groupLookups per table, the lookup that returns the rows of the table of a whole group
     *     for an anchor key row, or -1; null for none
     */
    public AnchorJoinTree(
            RowType[] leafTypes,
            Node[] nodes,
            Edge[] edges,
            int anchorLeaf,
            int[] anchorKeyFields,
            int anchorLookup,
            @Nullable int[] groupLookups) {
        this.leafTypes = leafTypes;
        this.nodes = nodes;
        this.edges = edges;
        this.anchorLeaf = anchorLeaf;
        this.anchorKeyFields = anchorKeyFields;
        this.anchorLookup = anchorLookup;
        Preconditions.checkArgument(nodes.length == 2 * leafTypes.length - 1);
        Preconditions.checkArgument(edges.length == leafTypes.length - 1);
        this.nextTowardsAnchor = new int[leafTypes.length];
        this.pathsToAnchor = computePathsToAnchor();
        this.anchorKeyFieldsInNeighbour = computeAnchorKeyFieldsInNeighbours();
        this.anchorKeyFieldsOf = computeAnchorKeyFieldsOf();
        if (groupLookups == null) {
            groupLookups = new int[leafTypes.length];
            Arrays.fill(groupLookups, -1);
        }
        Preconditions.checkArgument(groupLookups.length == leafTypes.length);
        for (int leaf = 0; leaf < leafTypes.length; leaf++) {
            Preconditions.checkArgument(
                    groupLookups[leaf] < 0 || anchorKeyFieldsOf[leaf] != null,
                    "Table %s is read by group, but its rows do not hold the anchor key.",
                    leaf);
        }
        this.groupLookups = groupLookups;
    }

    public int getNumLeaves() {
        return leafTypes.length;
    }

    public RowType getLeafType(int leaf) {
        return leafTypes[leaf];
    }

    public Node getNode(int node) {
        return nodes[node];
    }

    public int getNumNodes() {
        return nodes.length;
    }

    /** The top join. Nodes are stored in post order. */
    public int getRootNode() {
        return nodes.length - 1;
    }

    public Edge getEdge(int edge) {
        return edges[edge];
    }

    public int getAnchorLeaf() {
        return anchorLeaf;
    }

    public int[] getAnchorKeyFields() {
        return anchorKeyFields;
    }

    /** The lookup that returns anchor rows for an anchor key row. */
    public int getAnchorLookup() {
        return anchorLookup;
    }

    /**
     * The fields holding the anchor key in every line with a row of the table, or null. Beyond the
     * anchor's neighbours, only through joins that drop unmatched rows of the table.
     */
    public @Nullable int[] getAnchorKeyFieldsOf(int leaf) {
        return anchorKeyFieldsOf[leaf];
    }

    /**
     * The first table on the path to the anchor, from the table itself, whose rows hold the anchor
     * key; -1 if none. Locating reads the key from it; keys of missing anchors only cost a refresh.
     */
    public int getLocateTable(int leaf) {
        for (int current = leaf; current != anchorLeaf; current = nextTowardsAnchor[current]) {
            if (anchorKeyFieldsOf[current] != null) {
                return current;
            }
        }
        return -1;
    }

    /**
     * The lookups that locating anchors for rows of the given table calls, in order: the path to
     * the anchor, up to the table of {@link #getLocateTable}.
     */
    public int[] getLocateLookups(int leaf) {
        int[] path = pathsToAnchor[leaf];
        int table = getLocateTable(leaf);
        if (table < 0) {
            return path;
        }
        int hops = 0;
        for (int current = leaf; current != table; current = nextTowardsAnchor[current]) {
            hops++;
        }
        return Arrays.copyOf(path, hops);
    }

    /** The lookup that returns the rows of the given table of a whole group, or -1. */
    public int getGroupLookup(int leaf) {
        return groupLookups[leaf];
    }

    /**
     * Whether changes of the given table can refresh only the lines that contain the changed rows:
     * the table is joined to the anchor table directly, on equality columns that cover the anchor
     * key, and no table is joined beyond it. Its rows then name their group themselves, and the
     * lines of an old version of a row depend on the anchor row only.
     */
    public boolean canRefreshRowsOf(int leaf) {
        if (leaf == anchorLeaf
                || nextTowardsAnchor[leaf] != anchorLeaf
                || anchorKeyFieldsInNeighbour[leaf] == null) {
            return false;
        }
        for (int other = 0; other < nextTowardsAnchor.length; other++) {
            if (other != anchorLeaf && nextTowardsAnchor[other] == leaf) {
                return false;
            }
        }
        return true;
    }

    /**
     * The join whose equality columns connect the given table to the next one towards the anchor.
     */
    public int getJoinTowardsAnchor(int leaf) {
        Preconditions.checkArgument(leaf != anchorLeaf, "The anchor table has no such join.");
        int towards = nextTowardsAnchor[leaf];
        for (int n = 0; n < nodes.length; n++) {
            Node node = nodes[n];
            if (node.isLeaf()) {
                continue;
            }
            Edge edge = edges[node.getEdge()];
            if ((edge.leftLeaf == leaf && edge.rightLeaf == towards)
                    || (edge.rightLeaf == leaf && edge.leftLeaf == towards)) {
                return n;
            }
        }
        throw new IllegalStateException("No join connects tables " + leaf + " and " + towards);
    }

    /** The lookups that locating anchors for rows of the given table uses. */
    public boolean[] lookupsUsedToLocate(int leaf, int numLookups) {
        boolean[] used = new boolean[numLookups];
        for (int lookup : getLocateLookups(leaf)) {
            used[lookup] = true;
        }
        return used;
    }

    /** The tables one step further away from the anchor table than the given table. */
    public int[] getLeavesAwayFromAnchor(int leaf) {
        List<Integer> away = new ArrayList<>();
        for (int other = 0; other < leafTypes.length; other++) {
            if (nextTowardsAnchor[other] == leaf) {
                away.add(other);
            }
        }
        return away.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * The lookups that recomputing a group uses: the anchor lookup and, for every edge, the lookup
     * directed away from the anchor.
     */
    public boolean[] lookupsUsedToRecompute(int numLookups) {
        boolean[] towardsAnchor = new boolean[numLookups];
        for (int[] path : pathsToAnchor) {
            for (int lookup : path) {
                towardsAnchor[lookup] = true;
            }
        }
        boolean[] used = new boolean[numLookups];
        used[anchorLookup] = true;
        for (Edge edge : edges) {
            used[towardsAnchor[edge.lookupRight] ? edge.lookupLeft : edge.lookupRight] = true;
        }
        for (int lookup : groupLookups) {
            if (lookup >= 0) {
                used[lookup] = true;
            }
        }
        return used;
    }

    /** The lookups of a delta step: both directions of every edge. */
    public boolean[] lookupsUsedForDeltas(int numLookups) {
        boolean[] used = new boolean[numLookups];
        for (Edge edge : edges) {
            used[edge.lookupLeft] = true;
            used[edge.lookupRight] = true;
        }
        return used;
    }

    private int[][] computePathsToAnchor() {
        int numLeaves = leafTypes.length;
        // adjacency: for each table, (edge, neighbour, lookup from this table to the neighbour)
        List<List<int[]>> adjacency = new ArrayList<>();
        for (int i = 0; i < numLeaves; i++) {
            adjacency.add(new ArrayList<>());
        }
        for (Edge edge : edges) {
            adjacency.get(edge.leftLeaf).add(new int[] {edge.rightLeaf, edge.lookupRight});
            adjacency.get(edge.rightLeaf).add(new int[] {edge.leftLeaf, edge.lookupLeft});
        }
        // breadth first from the anchor; for every table remember the lookup towards the anchor
        int[] nextLeaf = new int[numLeaves];
        int[] lookupToNext = new int[numLeaves];
        Arrays.fill(nextLeaf, -2);
        nextLeaf[anchorLeaf] = -1;
        Deque<Integer> queue = new ArrayDeque<>();
        queue.add(anchorLeaf);
        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (int[] neighbour : adjacency.get(current)) {
                int other = neighbour[0];
                if (nextLeaf[other] != -2) {
                    continue;
                }
                nextLeaf[other] = current;
                // the lookup from `other` towards `current`
                lookupToNext[other] = getLookupBetween(other, current);
                queue.add(other);
            }
        }
        int[][] paths = new int[numLeaves][];
        for (int leaf = 0; leaf < numLeaves; leaf++) {
            Preconditions.checkState(nextLeaf[leaf] != -2, "Table %s is not connected.", leaf);
            nextTowardsAnchor[leaf] = nextLeaf[leaf];
            List<Integer> path = new ArrayList<>();
            for (int cur = leaf; nextLeaf[cur] != -1; cur = nextLeaf[cur]) {
                path.add(lookupToNext[cur]);
            }
            paths[leaf] = path.stream().mapToInt(Integer::intValue).toArray();
        }
        return paths;
    }

    private int[][] computeAnchorKeyFieldsInNeighbours() {
        int[][] result = new int[leafTypes.length][];
        for (Edge edge : edges) {
            if (edge.leftLeaf == anchorLeaf) {
                result[edge.rightLeaf] =
                        anchorKeyFieldsIn(edge.rightLeaf, edge.rightFields, edge.leftFields);
            } else if (edge.rightLeaf == anchorLeaf) {
                result[edge.leftLeaf] =
                        anchorKeyFieldsIn(edge.leftLeaf, edge.leftFields, edge.rightFields);
            }
        }
        return result;
    }

    private int[][] computeAnchorKeyFieldsOf() {
        int numLeaves = leafTypes.length;
        int[][] result = new int[numLeaves][];
        Integer[] byDistance = new Integer[numLeaves];
        for (int leaf = 0; leaf < numLeaves; leaf++) {
            byDistance[leaf] = leaf;
        }
        // the table towards the anchor first
        Arrays.sort(byDistance, (a, b) -> pathsToAnchor[a].length - pathsToAnchor[b].length);
        for (int leaf : byDistance) {
            int next = nextTowardsAnchor[leaf];
            if (leaf == anchorLeaf) {
                continue;
            }
            if (next == anchorLeaf) {
                result[leaf] = anchorKeyFieldsInNeighbour[leaf];
                continue;
            }
            if (result[next] == null || !keptOnlyWhereMatched(leaf)) {
                continue;
            }
            Edge edge = edges[nodes[getJoinTowardsAnchor(leaf)].getEdge()];
            boolean leafIsLeft = edge.leftLeaf == leaf;
            result[leaf] =
                    anchorKeyFieldsIn(
                            leaf,
                            leafIsLeft ? edge.leftFields : edge.rightFields,
                            leafIsLeft ? edge.rightFields : edge.leftFields,
                            result[next]);
        }
        return result;
    }

    /** Whether the join towards the anchor keeps rows of the table only where they matched. */
    private boolean keptOnlyWhereMatched(int leaf) {
        Node join = nodes[getJoinTowardsAnchor(leaf)];
        boolean inLeft = nodes[join.getLeft()].containsLeaf(leaf);
        switch (join.getJoinType()) {
            case INNER:
                return true;
            case LEFT:
                return !inLeft;
            case RIGHT:
                return inLeft;
            default:
                return false;
        }
    }

    /**
     * The fields of the neighbour equal to the anchor key fields, if the equality columns between
     * them cover the anchor key and the types agree; null otherwise.
     */
    private @Nullable int[] anchorKeyFieldsIn(
            int neighbour, int[] neighbourFields, int[] anchorFields) {
        return anchorKeyFieldsIn(neighbour, neighbourFields, anchorFields, anchorKeyFields);
    }

    /**
     * The fields of a table equal to the anchor key fields of the next table towards the anchor, or
     * null if the equality columns do not cover them or the types differ.
     */
    private @Nullable int[] anchorKeyFieldsIn(
            int neighbour, int[] neighbourFields, int[] anchorFields, int[] keyFieldsOfNext) {
        RowType anchorType = leafTypes[anchorLeaf];
        RowType neighbourType = leafTypes[neighbour];
        int[] fields = new int[anchorKeyFields.length];
        for (int k = 0; k < anchorKeyFields.length; k++) {
            int found = -1;
            for (int i = 0; i < anchorFields.length && found < 0; i++) {
                if (anchorFields[i] == keyFieldsOfNext[k]) {
                    found = neighbourFields[i];
                }
            }
            if (found < 0
                    || !anchorType
                            .getTypeAt(anchorKeyFields[k])
                            .copy(true)
                            .equals(neighbourType.getTypeAt(found).copy(true))) {
                return null;
            }
            fields[k] = found;
        }
        return fields;
    }

    /**
     * The lookup that returns rows of table {@code to} for a row of the adjacent table {@code
     * from}.
     */
    public int getLookupBetween(int from, int to) {
        for (Edge edge : edges) {
            if (edge.leftLeaf == from && edge.rightLeaf == to) {
                return edge.lookupRight;
            }
            if (edge.rightLeaf == from && edge.leftLeaf == to) {
                return edge.lookupLeft;
            }
        }
        throw new IllegalStateException("No edge between tables " + from + " and " + to);
    }

    /** Maps the output fields of a node to (table, field) pairs of a line. */
    public static final class FieldMap implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int[] leaves;
        private final int[] fields;

        public FieldMap(int[] leaves, int[] fields) {
            Preconditions.checkArgument(leaves.length == fields.length);
            this.leaves = leaves;
            this.fields = fields;
        }

        public static FieldMap ofLeaf(int leaf, int arity) {
            int[] leaves = new int[arity];
            int[] fields = new int[arity];
            Arrays.fill(leaves, leaf);
            for (int i = 0; i < arity; i++) {
                fields[i] = i;
            }
            return new FieldMap(leaves, fields);
        }

        public FieldMap concat(FieldMap other) {
            int[] newLeaves = Arrays.copyOf(leaves, leaves.length + other.leaves.length);
            int[] newFields = Arrays.copyOf(fields, fields.length + other.fields.length);
            System.arraycopy(other.leaves, 0, newLeaves, leaves.length, other.leaves.length);
            System.arraycopy(other.fields, 0, newFields, fields.length, other.fields.length);
            return new FieldMap(newLeaves, newFields);
        }

        public FieldMap project(int[] projection) {
            int[] newLeaves = new int[projection.length];
            int[] newFields = new int[projection.length];
            for (int i = 0; i < projection.length; i++) {
                newLeaves[i] = leaves[projection[i]];
                newFields[i] = fields[projection[i]];
            }
            return new FieldMap(newLeaves, newFields);
        }

        public int size() {
            return leaves.length;
        }

        public int leafOf(int field) {
            return leaves[field];
        }

        public int fieldOf(int field) {
            return fields[field];
        }

        public int[] getLeaves() {
            return leaves;
        }

        public int[] getFields() {
            return fields;
        }

        /** A view of the fields of a line through this map. */
        public LineRowData view() {
            return new LineRowData(leaves, fields);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < leaves.length; i++) {
                sb.append(i == 0 ? "" : ", ").append(leaves[i]).append('.').append(fields[i]);
            }
            return sb.append(']').toString();
        }
    }

    /**
     * A node of the tree: a table or a join. A join may be followed by filters (from calcs between
     * joins); each filter reads the node's fields through its own {@link FieldMap}.
     */
    public static final class Node implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int leaf;
        private final int left;
        private final int right;
        private final FlinkJoinType joinType;
        private final int edge;
        private final FieldMap output;
        private final List<FieldMap> filterInputs;
        private final boolean[] containsLeaf;

        private Node(
                int leaf,
                int left,
                int right,
                FlinkJoinType joinType,
                int edge,
                FieldMap output,
                List<FieldMap> filterInputs,
                boolean[] containsLeaf) {
            this.leaf = leaf;
            this.left = left;
            this.right = right;
            this.joinType = joinType;
            this.edge = edge;
            this.output = output;
            this.filterInputs = filterInputs;
            this.containsLeaf = containsLeaf;
        }

        public static Node leaf(int leaf, int numLeaves, int arity) {
            boolean[] contains = new boolean[numLeaves];
            contains[leaf] = true;
            return new Node(
                    leaf,
                    -1,
                    -1,
                    null,
                    -1,
                    FieldMap.ofLeaf(leaf, arity),
                    Collections.emptyList(),
                    contains);
        }

        /**
         * A join of two nodes. {@code output} describes the node's fields after the filters'
         * projections; {@code filterInputs} describes the fields each filter reads.
         */
        public static Node join(
                int left,
                int right,
                Node leftNode,
                Node rightNode,
                FlinkJoinType joinType,
                int edge,
                FieldMap output,
                List<FieldMap> filterInputs) {
            Preconditions.checkArgument(
                    joinType == FlinkJoinType.INNER
                            || joinType == FlinkJoinType.LEFT
                            || joinType == FlinkJoinType.RIGHT,
                    "Unsupported join type %s",
                    joinType);
            boolean[] contains = new boolean[leftNode.containsLeaf.length];
            for (int i = 0; i < contains.length; i++) {
                contains[i] = leftNode.containsLeaf[i] || rightNode.containsLeaf[i];
            }
            return new Node(-1, left, right, joinType, edge, output, filterInputs, contains);
        }

        public boolean isLeaf() {
            return leaf >= 0;
        }

        public int getLeaf() {
            return leaf;
        }

        public int getLeft() {
            return left;
        }

        public int getRight() {
            return right;
        }

        public FlinkJoinType getJoinType() {
            return joinType;
        }

        public int getEdge() {
            return edge;
        }

        public FieldMap getOutput() {
            return output;
        }

        public List<FieldMap> getFilterInputs() {
            return filterInputs;
        }

        public boolean containsLeaf(int leaf) {
            return containsLeaf[leaf];
        }
    }

    /**
     * The equality columns of one join: a table of the join's left subtree and a table of its right
     * subtree, with a lookup in each direction.
     */
    public static final class Edge implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int leftLeaf;
        private final int[] leftFields;
        private final int rightLeaf;
        private final int[] rightFields;
        private final int lookupRight;
        private final int lookupLeft;

        /**
         * @param lookupRight the lookup that returns rows of the right table for a row of the left
         *     table
         * @param lookupLeft the lookup that returns rows of the left table for a row of the right
         *     table
         */
        public Edge(
                int leftLeaf,
                int[] leftFields,
                int rightLeaf,
                int[] rightFields,
                int lookupRight,
                int lookupLeft) {
            Preconditions.checkArgument(leftFields.length == rightFields.length);
            this.leftLeaf = leftLeaf;
            this.leftFields = leftFields;
            this.rightLeaf = rightLeaf;
            this.rightFields = rightFields;
            this.lookupRight = lookupRight;
            this.lookupLeft = lookupLeft;
        }

        public int getLeftLeaf() {
            return leftLeaf;
        }

        public int[] getLeftFields() {
            return leftFields;
        }

        public int getRightLeaf() {
            return rightLeaf;
        }

        public int[] getRightFields() {
            return rightFields;
        }

        public int getLookupRight() {
            return lookupRight;
        }

        public int getLookupLeft() {
            return lookupLeft;
        }
    }
}
