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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupRefreshDeltaJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The join tree of a {@link StreamExecGroupRefreshDeltaJoin}.
 *
 * <p>Tables (leaves) are numbered from left to right and are the inputs of the exec node in that
 * order. Nodes are stored in post order, so the last node is the top join. Every join has exactly
 * one edge: the equality columns between one table of its left subtree and one table of its right
 * subtree, with a lookup in each direction. Field references of conditions and filters are resolved
 * against a line, which holds one row per table, through (table, field) pairs.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupRefreshDeltaJoinSpec {

    public static final String FIELD_NAME_LEAF_TYPES = "leafTypes";
    public static final String FIELD_NAME_NODES = "nodes";
    public static final String FIELD_NAME_EDGES = "edges";
    public static final String FIELD_NAME_ANCHOR_LEAF = "anchorLeaf";
    public static final String FIELD_NAME_ANCHOR_KEY = "anchorKey";
    public static final String FIELD_NAME_ANCHOR_LOOKUP = "anchorLookup";
    public static final String FIELD_NAME_LEAF_KEYS = "leafKeys";
    public static final String FIELD_NAME_GROUPING = "grouping";

    /** Which output rows form a group that is refreshed as a whole. */
    public enum Grouping {
        /** The output rows of one row of the anchor table, which is in every output row. */
        ANCHOR,
        /** The output rows of one join key; only for a join of two tables. */
        JOIN_KEY
    }

    /** The row type of every table as it enters the join (after its calc). */
    @JsonProperty(FIELD_NAME_LEAF_TYPES)
    private final List<RowType> leafTypes;

    @JsonProperty(FIELD_NAME_NODES)
    private final List<Node> nodes;

    @JsonProperty(FIELD_NAME_EDGES)
    private final List<Edge> edges;

    @JsonProperty(FIELD_NAME_ANCHOR_LEAF)
    private final int anchorLeaf;

    /** The fields of the anchor table's rows that identify an anchor row. */
    @JsonProperty(FIELD_NAME_ANCHOR_KEY)
    private final int[] anchorKey;

    /** Looks up anchor rows by a row that holds the anchor key; null for join-key groups. */
    @JsonProperty(FIELD_NAME_ANCHOR_LOOKUP)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable DeltaJoinSpec anchorLookup;

    @JsonProperty(FIELD_NAME_GROUPING)
    private final Grouping grouping;

    /** A unique key of the rows of every table, as its fields; empty if it has none. */
    @JsonProperty(FIELD_NAME_LEAF_KEYS)
    private final List<int[]> leafKeys;

    @JsonCreator
    public GroupRefreshDeltaJoinSpec(
            @JsonProperty(FIELD_NAME_LEAF_TYPES) List<RowType> leafTypes,
            @JsonProperty(FIELD_NAME_NODES) List<Node> nodes,
            @JsonProperty(FIELD_NAME_EDGES) List<Edge> edges,
            @JsonProperty(FIELD_NAME_ANCHOR_LEAF) int anchorLeaf,
            @JsonProperty(FIELD_NAME_ANCHOR_KEY) int[] anchorKey,
            @JsonProperty(FIELD_NAME_ANCHOR_LOOKUP) @Nullable DeltaJoinSpec anchorLookup,
            @JsonProperty(FIELD_NAME_LEAF_KEYS) List<int[]> leafKeys,
            @JsonProperty(FIELD_NAME_GROUPING) Grouping grouping) {
        this.leafTypes = leafTypes;
        this.nodes = nodes;
        this.edges = edges;
        this.anchorLeaf = anchorLeaf;
        this.anchorKey = anchorKey;
        this.anchorLookup = anchorLookup;
        this.leafKeys = checkNotNull(leafKeys);
        this.grouping = checkNotNull(grouping);
    }

    @JsonIgnore
    public Grouping getGrouping() {
        return grouping;
    }

    @JsonIgnore
    public List<RowType> getLeafTypes() {
        return leafTypes;
    }

    @JsonIgnore
    public List<Node> getNodes() {
        return nodes;
    }

    @JsonIgnore
    public List<Edge> getEdges() {
        return edges;
    }

    @JsonIgnore
    public int getAnchorLeaf() {
        return anchorLeaf;
    }

    @JsonIgnore
    public int[] getAnchorKey() {
        return anchorKey;
    }

    /** A unique key of every table, empty if it has none. */
    @JsonIgnore
    public List<int[]> getLeafKeys() {
        return leafKeys;
    }

    public @Nullable DeltaJoinSpec getAnchorLookup() {
        return anchorLookup;
    }

    /** A table or a join. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Node {

        public static final String FIELD_NAME_LEAF = "leaf";
        public static final String FIELD_NAME_LEFT = "left";
        public static final String FIELD_NAME_RIGHT = "right";
        public static final String FIELD_NAME_JOIN_TYPE = "joinType";
        public static final String FIELD_NAME_EDGE = "edge";
        public static final String FIELD_NAME_CONDITION = "condition";
        public static final String FIELD_NAME_LEFT_TYPE = "leftType";
        public static final String FIELD_NAME_RIGHT_TYPE = "rightType";
        public static final String FIELD_NAME_OUTPUT_LEAVES = "outputLeaves";
        public static final String FIELD_NAME_OUTPUT_FIELDS = "outputFields";
        public static final String FIELD_NAME_FILTERS = "filters";

        /** The table of a leaf node, or -1 for a join. */
        @JsonProperty(FIELD_NAME_LEAF)
        private final int leaf;

        @JsonProperty(FIELD_NAME_LEFT)
        private final int left;

        @JsonProperty(FIELD_NAME_RIGHT)
        private final int right;

        @JsonProperty(FIELD_NAME_JOIN_TYPE)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final @Nullable FlinkJoinType joinType;

        @JsonProperty(FIELD_NAME_EDGE)
        private final int edge;

        /** The complete join condition over the fields of the left and the right input. */
        @JsonProperty(FIELD_NAME_CONDITION)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final @Nullable RexNode condition;

        @JsonProperty(FIELD_NAME_LEFT_TYPE)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final @Nullable RowType leftType;

        @JsonProperty(FIELD_NAME_RIGHT_TYPE)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final @Nullable RowType rightType;

        /** For every output field of the node (after its filters' projections), its table. */
        @JsonProperty(FIELD_NAME_OUTPUT_LEAVES)
        private final int[] outputLeaves;

        /** For every output field of the node, the field of its table. */
        @JsonProperty(FIELD_NAME_OUTPUT_FIELDS)
        private final int[] outputFields;

        /** Filters of calcs between this join and its parent, in the order they are applied. */
        @JsonProperty(FIELD_NAME_FILTERS)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final List<Filter> filters;

        @JsonCreator
        public Node(
                @JsonProperty(FIELD_NAME_LEAF) int leaf,
                @JsonProperty(FIELD_NAME_LEFT) int left,
                @JsonProperty(FIELD_NAME_RIGHT) int right,
                @JsonProperty(FIELD_NAME_JOIN_TYPE) @Nullable FlinkJoinType joinType,
                @JsonProperty(FIELD_NAME_EDGE) int edge,
                @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
                @JsonProperty(FIELD_NAME_LEFT_TYPE) @Nullable RowType leftType,
                @JsonProperty(FIELD_NAME_RIGHT_TYPE) @Nullable RowType rightType,
                @JsonProperty(FIELD_NAME_OUTPUT_LEAVES) int[] outputLeaves,
                @JsonProperty(FIELD_NAME_OUTPUT_FIELDS) int[] outputFields,
                @JsonProperty(FIELD_NAME_FILTERS) @Nullable List<Filter> filters) {
            this.leaf = leaf;
            this.left = left;
            this.right = right;
            this.joinType = joinType;
            this.edge = edge;
            this.condition = condition;
            this.leftType = leftType;
            this.rightType = rightType;
            this.outputLeaves = outputLeaves;
            this.outputFields = outputFields;
            this.filters = filters == null ? List.of() : filters;
        }

        @JsonIgnore
        public boolean isLeaf() {
            return leaf >= 0;
        }

        @JsonIgnore
        public int getLeaf() {
            return leaf;
        }

        @JsonIgnore
        public int getLeft() {
            return left;
        }

        @JsonIgnore
        public int getRight() {
            return right;
        }

        @JsonIgnore
        public @Nullable FlinkJoinType getJoinType() {
            return joinType;
        }

        @JsonIgnore
        public int getEdge() {
            return edge;
        }

        @JsonIgnore
        public @Nullable RexNode getCondition() {
            return condition;
        }

        @JsonIgnore
        public @Nullable RowType getLeftType() {
            return leftType;
        }

        @JsonIgnore
        public @Nullable RowType getRightType() {
            return rightType;
        }

        @JsonIgnore
        public int[] getOutputLeaves() {
            return outputLeaves;
        }

        @JsonIgnore
        public int[] getOutputFields() {
            return outputFields;
        }

        @JsonIgnore
        public List<Filter> getFilters() {
            return filters;
        }
    }

    /** A filter of a calc between two joins. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Filter {

        public static final String FIELD_NAME_CONDITION = "condition";
        public static final String FIELD_NAME_INPUT_TYPE = "inputType";
        public static final String FIELD_NAME_INPUT_LEAVES = "inputLeaves";
        public static final String FIELD_NAME_INPUT_FIELDS = "inputFields";

        @JsonProperty(FIELD_NAME_CONDITION)
        private final RexNode condition;

        @JsonProperty(FIELD_NAME_INPUT_TYPE)
        private final RowType inputType;

        @JsonProperty(FIELD_NAME_INPUT_LEAVES)
        private final int[] inputLeaves;

        @JsonProperty(FIELD_NAME_INPUT_FIELDS)
        private final int[] inputFields;

        @JsonCreator
        public Filter(
                @JsonProperty(FIELD_NAME_CONDITION) RexNode condition,
                @JsonProperty(FIELD_NAME_INPUT_TYPE) RowType inputType,
                @JsonProperty(FIELD_NAME_INPUT_LEAVES) int[] inputLeaves,
                @JsonProperty(FIELD_NAME_INPUT_FIELDS) int[] inputFields) {
            this.condition = condition;
            this.inputType = inputType;
            this.inputLeaves = inputLeaves;
            this.inputFields = inputFields;
        }

        @JsonIgnore
        public RexNode getCondition() {
            return condition;
        }

        @JsonIgnore
        public RowType getInputType() {
            return inputType;
        }

        @JsonIgnore
        public int[] getInputLeaves() {
            return inputLeaves;
        }

        @JsonIgnore
        public int[] getInputFields() {
            return inputFields;
        }
    }

    /** The equality columns of one join, with a lookup in each direction. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Edge {

        public static final String FIELD_NAME_LEFT_LEAF = "leftLeaf";
        public static final String FIELD_NAME_LEFT_FIELDS = "leftFields";
        public static final String FIELD_NAME_RIGHT_LEAF = "rightLeaf";
        public static final String FIELD_NAME_RIGHT_FIELDS = "rightFields";
        public static final String FIELD_NAME_LOOKUP_RIGHT = "lookupRight";
        public static final String FIELD_NAME_LOOKUP_LEFT = "lookupLeft";

        @JsonProperty(FIELD_NAME_LEFT_LEAF)
        private final int leftLeaf;

        @JsonProperty(FIELD_NAME_LEFT_FIELDS)
        private final int[] leftFields;

        @JsonProperty(FIELD_NAME_RIGHT_LEAF)
        private final int rightLeaf;

        @JsonProperty(FIELD_NAME_RIGHT_FIELDS)
        private final int[] rightFields;

        /** Looks up rows of the right table by a row of the left table. */
        @JsonProperty(FIELD_NAME_LOOKUP_RIGHT)
        private final DeltaJoinSpec lookupRight;

        /** Looks up rows of the left table by a row of the right table. */
        @JsonProperty(FIELD_NAME_LOOKUP_LEFT)
        private final DeltaJoinSpec lookupLeft;

        @JsonCreator
        public Edge(
                @JsonProperty(FIELD_NAME_LEFT_LEAF) int leftLeaf,
                @JsonProperty(FIELD_NAME_LEFT_FIELDS) int[] leftFields,
                @JsonProperty(FIELD_NAME_RIGHT_LEAF) int rightLeaf,
                @JsonProperty(FIELD_NAME_RIGHT_FIELDS) int[] rightFields,
                @JsonProperty(FIELD_NAME_LOOKUP_RIGHT) DeltaJoinSpec lookupRight,
                @JsonProperty(FIELD_NAME_LOOKUP_LEFT) DeltaJoinSpec lookupLeft) {
            this.leftLeaf = leftLeaf;
            this.leftFields = leftFields;
            this.rightLeaf = rightLeaf;
            this.rightFields = rightFields;
            this.lookupRight = lookupRight;
            this.lookupLeft = lookupLeft;
        }

        @JsonIgnore
        public int getLeftLeaf() {
            return leftLeaf;
        }

        @JsonIgnore
        public int[] getLeftFields() {
            return leftFields;
        }

        @JsonIgnore
        public int getRightLeaf() {
            return rightLeaf;
        }

        @JsonIgnore
        public int[] getRightFields() {
            return rightFields;
        }

        @JsonIgnore
        public DeltaJoinSpec getLookupRight() {
            return lookupRight;
        }

        @JsonIgnore
        public DeltaJoinSpec getLookupLeft() {
            return lookupLeft;
        }
    }
}
