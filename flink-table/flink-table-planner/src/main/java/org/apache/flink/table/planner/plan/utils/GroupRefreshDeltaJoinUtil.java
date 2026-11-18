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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.reuse.ScanReuserUtils;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorJoinTree.FieldMap;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Analysis of a tree of joins for the group refresh delta join.
 *
 * <p>The tree may consist of INNER, LEFT and RIGHT equi-joins, calcs between joins that only
 * project fields and filter deterministically, and table source scans with an optional
 * deterministic calc as leaves. Every join's equality columns must come from one table on each
 * side, so that the tables and joins form a tree of edges. Every table must support async lookups
 * on its edge columns. The anchor is a table that is present in every output row and has a primary
 * key it can be looked up by.
 */
public final class GroupRefreshDeltaJoinUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GroupRefreshDeltaJoinUtil.class);

    private GroupRefreshDeltaJoinUtil() {}

    /** The result of analysing a join tree. */
    public static final class JoinTree {

        public final List<RelNode> leafInputs;
        public final List<StreamPhysicalTableSourceScan> leafScans;
        public final List<RexProgram> leafCalcs;

        /** Whether a leaf's input had a {@code ChangelogNormalize} that the join does not need. */
        public final List<Boolean> leafNormalizerRemoved;

        public final List<PlannerNode> nodes;
        public final List<PlannerEdge> edges;

        /** Whether a group is the rows of an anchor row or of a join key. */
        public GroupRefreshDeltaJoinSpec.Grouping grouping =
                GroupRefreshDeltaJoinSpec.Grouping.ANCHOR;

        /**
         * Why the join cannot read its output back from its sink, for messages; null if unknown.
         */
        public @Nullable String readBackRefusal;

        /** Whether the top join may be a FULL join: only with join-key groups. */
        private boolean fullJoinAllowed;

        /** For anchor groups: the anchor table and its key; -1 and empty for join-key groups. */
        public int anchorLeaf = -1;

        public int[] anchorKey = new int[0];

        /** Whether every anchor row has at most one output row. */
        public boolean singleRowGroups;

        /** Inputs that must provide UPDATE_BEFORE and full deletes. */
        public boolean[] needsOldImages;

        /** The output field of every anchor key field, or null if one is not in the output. */
        public @Nullable int[] outputAnchorKeyPositions;

        /** Per table, the fields holding the anchor key if it is read per group; null otherwise. */
        public @Nullable int[][] groupReadFields;

        private JoinTree() {
            this.leafInputs = new ArrayList<>();
            this.leafScans = new ArrayList<>();
            this.leafCalcs = new ArrayList<>();
            this.leafNormalizerRemoved = new ArrayList<>();
            this.nodes = new ArrayList<>();
            this.edges = new ArrayList<>();
        }

        public int numLeaves() {
            return leafInputs.size();
        }

        public PlannerNode root() {
            return nodes.get(nodes.size() - 1);
        }

        /** A short description of the tree for EXPLAIN, e.g. {@code (#0 LEFT #1) INNER #2}. */
        public String describe() {
            return describe(nodes.size() - 1);
        }

        private String describe(int node) {
            PlannerNode n = nodes.get(node);
            if (n.isLeaf()) {
                return "#" + n.leaf;
            }
            String left = describe(n.left);
            String right = describe(n.right);
            if (!nodes.get(n.left).isLeaf()) {
                left = "(" + left + ")";
            }
            if (!nodes.get(n.right).isLeaf()) {
                right = "(" + right + ")";
            }
            return left + " " + n.joinType + " " + right;
        }
    }

    /** A table or a join of the analysed tree. */
    public static final class PlannerNode {
        public final int leaf;
        public final int left;
        public final int right;
        public final @Nullable FlinkJoinType joinType;
        public final int edge;
        public final @Nullable RexNode condition;
        public final @Nullable RowType leftType;
        public final @Nullable RowType rightType;
        public FieldMap output;
        public RelNodeRowType outputType;
        public final List<GroupRefreshDeltaJoinSpec.Filter> filters = new ArrayList<>();

        private PlannerNode(
                int leaf,
                int left,
                int right,
                @Nullable FlinkJoinType joinType,
                int edge,
                @Nullable RexNode condition,
                @Nullable RowType leftType,
                @Nullable RowType rightType,
                FieldMap output,
                RelNodeRowType outputType) {
            this.leaf = leaf;
            this.left = left;
            this.right = right;
            this.joinType = joinType;
            this.edge = edge;
            this.condition = condition;
            this.leftType = leftType;
            this.rightType = rightType;
            this.output = output;
            this.outputType = outputType;
        }

        public boolean isLeaf() {
            return leaf >= 0;
        }
    }

    /** The row type of a node, both as a Calcite and as a logical type. */
    public static final class RelNodeRowType {
        public final org.apache.calcite.rel.type.RelDataType relType;

        RelNodeRowType(org.apache.calcite.rel.type.RelDataType relType) {
            this.relType = relType;
        }

        public RowType logical() {
            return FlinkTypeFactory.toLogicalRowType(relType);
        }
    }

    /** The equality columns of a join. */
    public static final class PlannerEdge {
        public final int leftLeaf;
        public final int[] leftFields;
        public final int rightLeaf;
        public final int[] rightFields;

        PlannerEdge(int leftLeaf, int[] leftFields, int rightLeaf, int[] rightFields) {
            this.leftLeaf = leftLeaf;
            this.leftFields = leftFields;
            this.rightLeaf = rightLeaf;
            this.rightFields = rightFields;
        }

        int[] fieldsOf(int leaf) {
            return leaf == leftLeaf ? leftFields : rightFields;
        }

        int other(int leaf) {
            return leaf == leftLeaf ? rightLeaf : leftLeaf;
        }
    }

    /** Thrown internally when a tree cannot be converted. */
    private static final class Unsupported extends Exception {
        Unsupported(String reason) {
            super(reason, null, false, false);
        }
    }

    /**
     * Analyses the tree of joins rooted at the given join. Returns empty if the tree cannot be
     * converted into a delta join that refreshes groups, or if the incremental delta join can
     * handle it.
     *
     * @param joinKeyGroups whether a join of two tables refreshes the groups of its join key; a
     *     tree of more tables, or a join of two tables otherwise, refreshes the groups of the rows
     *     of an anchor table
     */
    public static Optional<JoinTree> analyze(StreamPhysicalJoin top, boolean joinKeyGroups) {
        try {
            JoinTree tree = new JoinTree();
            boolean twoTables = !reachesJoin(top.getLeft()) && !reachesJoin(top.getRight());
            tree.fullJoinAllowed = twoTables && joinKeyGroups;
            buildJoin(tree, top);
            validateLeaves(tree);
            if (!needsGroupRefresh(tree)) {
                return Optional.empty();
            }
            if (tree.fullJoinAllowed) {
                tree.grouping = GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY;
                computeJoinKeyGroups(tree);
                return Optional.of(tree);
            }
            chooseAnchor(tree);
            computeOldImages(tree);
            computeOutputAnchorKey(tree);
            computeGroupReads(tree);
            return Optional.of(tree);
        } catch (Unsupported e) {
            LOG.debug(
                    "The join cannot be converted into a group refresh delta join: {}",
                    e.getMessage());
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Building the tree
    // --------------------------------------------------------------------------------------------

    private static RelNode unwrap(RelNode node) {
        return DeltaJoinUtil.unwrapNode(node, false);
    }

    private static RelNode stripExchanges(RelNode node) {
        node = unwrap(node);
        while (node instanceof StreamPhysicalExchange) {
            node = unwrap(node.getInput(0));
        }
        return node;
    }

    /** Whether a join is reached through calcs and exchanges only. */
    private static boolean reachesJoin(RelNode node) {
        node = stripExchanges(node);
        while (node instanceof StreamPhysicalCalc) {
            node = stripExchanges(node.getInput(0));
        }
        return node instanceof StreamPhysicalJoin;
    }

    private static int build(JoinTree tree, RelNode input) throws Unsupported {
        RelNode node = stripExchanges(input);
        if (node instanceof StreamPhysicalJoin) {
            return buildJoin(tree, (StreamPhysicalJoin) node);
        }
        if (node instanceof StreamPhysicalCalc && reachesJoin(node)) {
            return buildCalcOverJoin(tree, (StreamPhysicalCalc) node);
        }
        return buildLeaf(tree, node);
    }

    private static int buildJoin(JoinTree tree, StreamPhysicalJoin join) throws Unsupported {
        FlinkJoinType joinType = JoinTypeUtil.getFlinkJoinType(join.getJoinType());
        boolean supported =
                joinType == FlinkJoinType.INNER
                        || joinType == FlinkJoinType.LEFT
                        || joinType == FlinkJoinType.RIGHT
                        || (joinType == FlinkJoinType.FULL && tree.fullJoinAllowed);
        if (!supported) {
            throw new Unsupported("join type " + joinType);
        }
        int left = build(tree, join.getLeft());
        int right = build(tree, join.getRight());

        List<Boolean> filterNulls = new ArrayList<>();
        JoinInfo joinInfo =
                JoinUtil.createJoinInfo(
                        join.getLeft(), join.getRight(), join.getCondition(), filterNulls);
        if (joinInfo.pairs().isEmpty()) {
            throw new Unsupported("no equality condition");
        }
        if (filterNulls.contains(false)) {
            throw new Unsupported("null-safe equality");
        }
        if (!RexUtil.isDeterministic(join.getCondition())) {
            throw new Unsupported("non-deterministic join condition");
        }
        FieldMap leftMap = tree.nodes.get(left).output;
        FieldMap rightMap = tree.nodes.get(right).output;
        int leftLeaf = singleLeaf(leftMap, joinInfo.leftKeys);
        int rightLeaf = singleLeaf(rightMap, joinInfo.rightKeys);
        int[] leftFields = joinInfo.leftKeys.stream().mapToInt(leftMap::fieldOf).toArray();
        int[] rightFields = joinInfo.rightKeys.stream().mapToInt(rightMap::fieldOf).toArray();
        tree.edges.add(new PlannerEdge(leftLeaf, leftFields, rightLeaf, rightFields));

        tree.nodes.add(
                new PlannerNode(
                        -1,
                        left,
                        right,
                        joinType,
                        tree.edges.size() - 1,
                        join.getCondition(),
                        FlinkTypeFactory.toLogicalRowType(join.getLeft().getRowType()),
                        FlinkTypeFactory.toLogicalRowType(join.getRight().getRowType()),
                        leftMap.concat(rightMap),
                        new RelNodeRowType(join.getRowType())));
        return tree.nodes.size() - 1;
    }

    private static int singleLeaf(FieldMap map, ImmutableIntList keys) throws Unsupported {
        int leaf = -1;
        for (int key : keys) {
            int keyLeaf = map.leafOf(key);
            if (leaf != -1 && keyLeaf != leaf) {
                throw new Unsupported("join keys of one side come from several tables");
            }
            leaf = keyLeaf;
        }
        return leaf;
    }

    private static int buildCalcOverJoin(JoinTree tree, StreamPhysicalCalc calc)
            throws Unsupported {
        int child = build(tree, calc.getInput());
        PlannerNode node = tree.nodes.get(child);
        if (node.isLeaf()) {
            throw new Unsupported("unexpected calc");
        }
        RexProgram program = calc.getProgram();
        if (!program.getExprList().stream().allMatch(RexUtil::isDeterministic)) {
            throw new Unsupported("non-deterministic calc between joins");
        }
        List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        int[] projection = new int[projects.size()];
        for (int i = 0; i < projects.size(); i++) {
            if (!(projects.get(i) instanceof RexInputRef)) {
                throw new Unsupported("computed projection between joins");
            }
            projection[i] = ((RexInputRef) projects.get(i)).getIndex();
        }
        if (program.getCondition() != null) {
            RexNode condition = program.expandLocalRef(program.getCondition());
            node.filters.add(
                    new GroupRefreshDeltaJoinSpec.Filter(
                            condition,
                            node.outputType.logical(),
                            node.output.getLeaves(),
                            node.output.getFields()));
        }
        node.output = node.output.project(projection);
        node.outputType = new RelNodeRowType(calc.getRowType());
        return child;
    }

    private static int buildLeaf(JoinTree tree, RelNode joinInput) throws Unsupported {
        boolean[] normalizerRemoved = new boolean[1];
        RelNode input = withoutNormalizer(joinInput, normalizerRemoved);
        if (input == null || !isScanWithCalcs(input)) {
            throw new Unsupported("unsupported node before a table source scan");
        }
        if (!DeltaJoinUtil.isCalcOnTableScanSupported(input)) {
            throw new Unsupported("unsupported calc on a table source scan");
        }
        StreamPhysicalTableSourceScan scan = DeltaJoinUtil.getTableScan(input);
        RexProgram calc = DeltaJoinUtil.getRexProgramBetweenJoinAndTableScan(input).orElse(null);
        int leaf = tree.leafInputs.size();
        tree.leafInputs.add(input);
        tree.leafScans.add(scan);
        tree.leafCalcs.add(calc);
        tree.leafNormalizerRemoved.add(normalizerRemoved[0]);
        int arity = input.getRowType().getFieldCount();
        tree.nodes.add(
                new PlannerNode(
                        leaf,
                        -1,
                        -1,
                        null,
                        -1,
                        null,
                        null,
                        null,
                        FieldMap.ofLeaf(leaf, arity),
                        new RelNodeRowType(input.getRowType())));
        return tree.nodes.size() - 1;
    }

    /**
     * The input without the {@code ChangelogNormalize} that the planner adds after upsert sources.
     * The group refresh delta join reads the inputs' state by lookups, so it needs a normalizer
     * only to obtain old images; inputs that need them are rejected later. Returns null for a
     * normalizer with a pushed-down filter.
     */
    private static @Nullable RelNode withoutNormalizer(RelNode input, boolean[] removed) {
        RelNode node = unwrap(input);
        if (node instanceof StreamPhysicalExchange) {
            return withoutNormalizer(node.getInput(0), removed);
        }
        if (node instanceof StreamPhysicalChangelogNormalize) {
            if (((StreamPhysicalChangelogNormalize) node).filterCondition() != null) {
                return null;
            }
            removed[0] = true;
            return withoutNormalizer(node.getInput(0), removed);
        }
        if (node instanceof StreamPhysicalCalc) {
            RelNode calcInput = withoutNormalizer(node.getInput(0), removed);
            if (calcInput == null) {
                return null;
            }
            return calcInput == unwrap(node.getInput(0))
                    ? node
                    : node.copy(node.getTraitSet(), Collections.singletonList(calcInput));
        }
        return node;
    }

    /**
     * Whether the input is a table source scan below exchanges and calcs only. Other nodes,
     * including intermediate tables of other query blocks, are not looked through.
     */
    private static boolean isScanWithCalcs(RelNode input) {
        RelNode node = unwrap(input);
        while (node instanceof StreamPhysicalExchange || node instanceof StreamPhysicalCalc) {
            node = unwrap(node.getInput(0));
        }
        return node instanceof StreamPhysicalTableSourceScan;
    }

    // --------------------------------------------------------------------------------------------
    // Validation and choices
    // --------------------------------------------------------------------------------------------

    private static void validateLeaves(JoinTree tree) throws Unsupported {
        for (StreamPhysicalTableSourceScan scan : tree.leafScans) {
            // lookups must return the rows the scan emits; pushed filters are checked as for the
            // delta join below, but pushed partitions leave the plan, and lookups do not prune them
            for (SourceAbilitySpec spec : scan.tableSourceTable().abilitySpecs()) {
                if (spec instanceof PartitionPushDownSpec) {
                    throw new Unsupported("partitions pushed into a table source");
                }
            }
        }
        for (PlannerEdge edge : tree.edges) {
            if (toSourceFields(tree, edge.leftLeaf, edge.leftFields) == null
                    || toSourceFields(tree, edge.rightLeaf, edge.rightFields) == null) {
                throw new Unsupported("join key is not a column of the table");
            }
            if (!DeltaJoinUtil.isTableScanSupported(
                            tree.leafScans.get(edge.leftLeaf),
                            toSourceFields(tree, edge.leftLeaf, edge.leftFields))
                    || !DeltaJoinUtil.isTableScanSupported(
                            tree.leafScans.get(edge.rightLeaf),
                            toSourceFields(tree, edge.rightLeaf, edge.rightFields))) {
                throw new Unsupported("table does not support async lookups on its join key");
            }
        }
    }

    /** The anchor strategy is used where the incremental delta join is not sound. */
    private static boolean needsGroupRefresh(JoinTree tree) {
        boolean hasOuterJoin =
                tree.nodes.stream().anyMatch(n -> !n.isLeaf() && n.joinType != FlinkJoinType.INNER);
        boolean hasDeletes =
                tree.leafScans.stream()
                        .anyMatch(
                                scan ->
                                        DeltaJoinUtil.declaredChangelogMode(scan)
                                                .contains(RowKind.DELETE));
        return hasOuterJoin || hasDeletes;
    }

    private static void chooseAnchor(JoinTree tree) throws Unsupported {
        List<Integer> candidates = new ArrayList<>();
        collectAnchorCandidates(tree, tree.nodes.size() - 1, candidates);
        int bestLeaf = -1;
        int[] bestKey = null;
        boolean bestSingle = false;
        for (int leaf : candidates) {
            Optional<int[]> key = lookupableKey(tree, leaf);
            if (key.isEmpty()) {
                continue;
            }
            boolean single = hasSingleRowGroups(tree, leaf);
            if (bestLeaf == -1 || (single && !bestSingle)) {
                bestLeaf = leaf;
                bestKey = key.get();
                bestSingle = single;
            }
        }
        if (bestLeaf == -1) {
            // projection push-down may have removed the key because the query does not use it;
            // read it back for the first candidate that allows it, preferring one-row groups
            List<Integer> ordered = new ArrayList<>();
            candidates.stream().filter(l -> hasSingleRowGroups(tree, l)).forEach(ordered::add);
            candidates.stream().filter(l -> !ordered.contains(l)).forEach(ordered::add);
            for (int leaf : ordered) {
                Optional<int[]> key = widenWithPrimaryKey(tree, leaf);
                if (key.isPresent()) {
                    bestLeaf = leaf;
                    bestKey = key.get();
                    bestSingle = hasSingleRowGroups(tree, leaf);
                    break;
                }
            }
        }
        if (bestLeaf == -1) {
            throw new Unsupported("no table can be the anchor");
        }
        tree.anchorLeaf = bestLeaf;
        tree.anchorKey = bestKey;
        tree.singleRowGroups = bestSingle;
    }

    /**
     * Adds the primary key of a table to its scan's projection and calc when projection push-down
     * removed it, and returns its fields in the widened rows. The key columns are appended, so
     * every field of the original input keeps its position and the tree's field mappings, which
     * cover only the original fields, stay valid. Returns empty and changes nothing if the table
     * has no primary key, the projection is nested, or the source cannot look the key up.
     */
    private static Optional<int[]> widenWithPrimaryKey(JoinTree tree, int leaf) {
        StreamPhysicalTableSourceScan scan = tree.leafScans.get(leaf);
        TableSourceTable table = scan.tableSourceTable();
        ResolvedSchema schema = table.contextResolvedTable().getResolvedSchema();
        if (schema.getPrimaryKey().isEmpty()) {
            return Optional.empty();
        }
        List<String> physical = DataType.getFieldNames(schema.toPhysicalRowDataType());
        int[] keyColumns =
                schema.getPrimaryKey().get().getColumns().stream()
                        .mapToInt(physical::indexOf)
                        .toArray();
        if (Arrays.stream(keyColumns).anyMatch(i -> i < 0)) {
            return Optional.empty();
        }

        // 1. the scan reads the key columns, appended to its projection
        RelNode newScan = scan;
        int[] keyInScan = keyColumns.clone();
        ProjectPushDownSpec projection =
                Arrays.stream(table.abilitySpecs())
                        .filter(spec -> spec instanceof ProjectPushDownSpec)
                        .map(spec -> (ProjectPushDownSpec) spec)
                        .findFirst()
                        .orElse(null);
        if (projection != null) {
            int[][] fields = projection.getProjectedFields();
            if (Arrays.stream(fields).anyMatch(f -> f.length != 1)) {
                return Optional.empty();
            }
            List<int[]> newFields = new ArrayList<>(Arrays.asList(fields));
            for (int k = 0; k < keyColumns.length; k++) {
                int column = keyColumns[k];
                int position = -1;
                for (int i = 0; i < newFields.size(); i++) {
                    if (newFields.get(i)[0] == column) {
                        position = i;
                    }
                }
                if (position < 0) {
                    newFields.add(new int[] {column});
                    position = newFields.size() - 1;
                }
                keyInScan[k] = position;
            }
            if (newFields.size() > fields.length) {
                FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(scan);
                List<SourceAbilitySpec> specs = ScanReuserUtils.abilitySpecsWithoutEscaped(table);
                RowType originType =
                        DynamicSourceUtils.createProducedType(schema, table.tableSource());
                int[][] projected = newFields.toArray(new int[0][]);
                RowType producedType = (RowType) Projection.of(projected).project(originType);
                specs.add(new ProjectPushDownSpec(projected, producedType));
                ScanTableSource source =
                        new DynamicTableSourceSpec(table.contextResolvedTable(), specs)
                                .getScanTableSource(ShortcutUtils.unwrapContext(scan), typeFactory);
                newScan =
                        scan.copy(
                                table.replace(
                                        source,
                                        typeFactory.buildRelNodeRowType(producedType),
                                        specs.toArray(new SourceAbilitySpec[0])));
            }
        }

        // 2. the calc passes the key columns through, appended to its projection
        RexProgram calc = tree.leafCalcs.get(leaf);
        RelNode newInput = newScan;
        RexProgram newCalc = null;
        int[] key = keyInScan;
        if (calc != null) {
            RexProgramBuilder builder =
                    new RexProgramBuilder(newScan.getRowType(), scan.getCluster().getRexBuilder());
            List<String> names = calc.getOutputRowType().getFieldNames();
            List<RexLocalRef> projects = calc.getProjectList();
            for (int i = 0; i < projects.size(); i++) {
                builder.addProject(calc.expandLocalRef(projects.get(i)), names.get(i));
            }
            key = new int[keyInScan.length];
            for (int k = 0; k < keyInScan.length; k++) {
                key[k] = projects.size() + k;
                builder.addProject(
                        new RexInputRef(
                                keyInScan[k],
                                newScan.getRowType().getFieldList().get(keyInScan[k]).getType()),
                        "$anchor_key" + k);
            }
            if (calc.getCondition() != null) {
                builder.addCondition(calc.expandLocalRef(calc.getCondition()));
            }
            newCalc = builder.getProgram();
            newInput = ScanReuserUtils.createCalcForScan(newScan, newCalc);
        }

        RelNode oldInput = tree.leafInputs.get(leaf);
        RexProgram oldCalc = tree.leafCalcs.get(leaf);
        tree.leafInputs.set(leaf, newInput);
        tree.leafScans.set(leaf, (StreamPhysicalTableSourceScan) newScan);
        tree.leafCalcs.set(leaf, newCalc);
        int[] sourceKey = toSourceFields(tree, leaf, key);
        if (sourceKey == null
                || !DeltaJoinUtil.isTableScanSupported(tree.leafScans.get(leaf), sourceKey)) {
            tree.leafInputs.set(leaf, oldInput);
            tree.leafScans.set(leaf, scan);
            tree.leafCalcs.set(leaf, oldCalc);
            return Optional.empty();
        }
        return Optional.of(key);
    }

    /** Tables present in every output row: the preserved side of outer joins, both of INNER. */
    private static void collectAnchorCandidates(JoinTree tree, int node, List<Integer> out) {
        PlannerNode n = tree.nodes.get(node);
        if (n.isLeaf()) {
            out.add(n.leaf);
            return;
        }
        if (n.joinType != FlinkJoinType.RIGHT) {
            collectAnchorCandidates(tree, n.left, out);
        }
        if (n.joinType != FlinkJoinType.LEFT) {
            collectAnchorCandidates(tree, n.right, out);
        }
    }

    /**
     * Tables two or more joins from the anchor whose rows hold the anchor key and are looked up
     * by it: a refresh reads them once per group instead of once per parent row.
     */
    private static void computeGroupReads(JoinTree tree) {
        int numLeaves = tree.numLeaves();
        int[] next = new int[numLeaves];
        Arrays.fill(next, -2);
        next[tree.anchorLeaf] = -1;
        int[][] keyFields = new int[numLeaves][];
        keyFields[tree.anchorLeaf] = tree.anchorKey;
        List<Integer> order = new ArrayList<>();
        Deque<Integer> queue = new ArrayDeque<>();
        queue.add(tree.anchorLeaf);
        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (PlannerEdge edge : tree.edges) {
                int other =
                        edge.leftLeaf == current
                                ? edge.rightLeaf
                                : edge.rightLeaf == current ? edge.leftLeaf : -1;
                if (other >= 0 && next[other] == -2) {
                    next[other] = current;
                    order.add(other);
                    queue.add(other);
                }
            }
        }
        RowType anchorType =
                FlinkTypeFactory.toLogicalRowType(
                        tree.leafInputs.get(tree.anchorLeaf).getRowType());
        tree.groupReadFields = new int[numLeaves][];
        for (int leaf : order) {
            int towards = next[leaf];
            int node = joinOfEdgeBetween(tree, leaf, towards);
            if (keyFields[towards] == null
                    || (towards != tree.anchorLeaf && !keptOnlyWhereMatched(tree, node, leaf))) {
                continue;
            }
            PlannerEdge edge = tree.edges.get(tree.nodes.get(node).edge);
            int[] leafSide = edge.fieldsOf(leaf);
            int[] towardsSide = edge.fieldsOf(towards);
            RowType leafType =
                    FlinkTypeFactory.toLogicalRowType(tree.leafInputs.get(leaf).getRowType());
            int[] fields = new int[tree.anchorKey.length];
            boolean covered = true;
            for (int k = 0; k < fields.length && covered; k++) {
                int found = -1;
                for (int i = 0; i < towardsSide.length && found < 0; i++) {
                    if (towardsSide[i] == keyFields[towards][k]) {
                        found = leafSide[i];
                    }
                }
                covered =
                        found >= 0
                                && leafType.getTypeAt(found)
                                        .copy(true)
                                        .equals(anchorType.getTypeAt(tree.anchorKey[k]).copy(true));
                fields[k] = found;
            }
            if (!covered) {
                continue;
            }
            keyFields[leaf] = fields;
            if (towards == tree.anchorLeaf || leafSide.length <= fields.length) {
                // next to the anchor, or looked up by the anchor key anyway: nothing to save
                continue;
            }
            int[] sourceFields = toSourceFields(tree, leaf, fields);
            if (sourceFields != null
                    && DeltaJoinUtil.isTableScanSupported(
                            tree.leafScans.get(leaf), sourceFields)) {
                tree.groupReadFields[leaf] = fields;
            }
        }
    }

    /** The join whose equality columns connect two adjacent tables. */
    private static int joinOfEdgeBetween(JoinTree tree, int a, int b) {
        for (int n = 0; n < tree.nodes.size(); n++) {
            PlannerNode node = tree.nodes.get(n);
            if (node.isLeaf()) {
                continue;
            }
            PlannerEdge edge = tree.edges.get(node.edge);
            if ((edge.leftLeaf == a && edge.rightLeaf == b)
                    || (edge.leftLeaf == b && edge.rightLeaf == a)) {
                return n;
            }
        }
        throw new IllegalStateException("No join connects tables " + a + " and " + b);
    }

    /** Whether the join keeps rows of the table only where its equality columns matched. */
    private static boolean keptOnlyWhereMatched(JoinTree tree, int node, int leaf) {
        PlannerNode join = tree.nodes.get(node);
        boolean inLeft = containsLeaf(tree, join.left, leaf);
        switch (join.joinType) {
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

    private static boolean containsLeaf(JoinTree tree, int node, int leaf) {
        PlannerNode n = tree.nodes.get(node);
        return n.isLeaf()
                ? n.leaf == leaf
                : containsLeaf(tree, n.left, leaf) || containsLeaf(tree, n.right, leaf);
    }

    /** The smallest unique key of the table that it supports lookups on. */
    private static Optional<int[]> lookupableKey(JoinTree tree, int leaf) {
        return upsertKeys(tree, leaf).stream()
                .sorted(Comparator.comparingInt(ImmutableBitSet::cardinality))
                .map(ImmutableBitSet::toArray)
                .filter(
                        key -> {
                            int[] sourceKey = toSourceFields(tree, leaf, key);
                            return sourceKey != null
                                    && DeltaJoinUtil.isTableScanSupported(
                                            tree.leafScans.get(leaf), sourceKey);
                        })
                .findFirst();
    }

    /** Whether every edge leaving the anchor reaches at most one row per row it starts from. */
    private static boolean hasSingleRowGroups(JoinTree tree, int anchor) {
        boolean[] visited = new boolean[tree.numLeaves()];
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        queue.add(anchor);
        visited[anchor] = true;
        while (!queue.isEmpty()) {
            int from = queue.poll();
            for (PlannerEdge edge : tree.edges) {
                if (edge.leftLeaf != from && edge.rightLeaf != from) {
                    continue;
                }
                int to = edge.other(from);
                if (visited[to]) {
                    continue;
                }
                visited[to] = true;
                queue.add(to);
                ImmutableBitSet lookupColumns = ImmutableBitSet.of(edge.fieldsOf(to));
                if (upsertKeys(tree, to).stream().noneMatch(lookupColumns::contains)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Join-key groups: both tables need a unique key, which tells the rows of a group apart, and a
     * table whose rows can move to another join key must provide old images.
     */
    private static void computeJoinKeyGroups(JoinTree tree) throws Unsupported {
        tree.needsOldImages = new boolean[tree.numLeaves()];
        PlannerEdge edge = tree.edges.get(0);
        for (int leaf = 0; leaf < tree.numLeaves(); leaf++) {
            Set<ImmutableBitSet> keys = upsertKeys(tree, leaf);
            if (keys.isEmpty()) {
                throw new Unsupported("table #" + leaf + " has no unique key");
            }
            // a row whose unique key holds its join key never moves to another group
            ImmutableBitSet joinKey = ImmutableBitSet.of(edge.fieldsOf(leaf));
            boolean moves = keys.stream().noneMatch(k -> k.contains(joinKey));
            setOldImages(tree, leaf, moves);
        }
    }

    /**
     * Whether a join of two tables grouped by the join key has a parent: a table with at most one
     * row per join key, as its unique key is in the join key. Changes of the other table, the
     * children, then refresh only their own rows.
     */
    public static boolean hasParent(JoinTree tree) {
        if (tree.grouping != GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY) {
            return false;
        }
        int[][] joinKeys = {tree.edges.get(0).leftFields, tree.edges.get(0).rightFields};
        int[][] uniqueKeys = new int[2][];
        for (int t = 0; t < 2; t++) {
            RelNode input = tree.leafInputs.get(t);
            FlinkRelMetadataQuery mq =
                    FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
            uniqueKeys[t] = UpsertKeyUtil.smallestKey(mq.getUniqueKeys(input)).orElse(new int[0]);
        }
        for (int t = 0; t < 2; t++) {
            int[] joinKey = joinKeys[t];
            boolean parent =
                    uniqueKeys[t].length > 0
                            && Arrays.stream(uniqueKeys[t])
                                    .allMatch(
                                            field ->
                                                    Arrays.stream(joinKey)
                                                            .anyMatch(k -> k == field));
            if (parent && uniqueKeys[1 - t].length > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * The keys of the output of join-key groups. A key holds a column with the join key of the
     * group, from a table that is in every row with that key: the preserved table, either table of
     * an inner join, or both tables of a full join. So rows with the same key values come from one
     * group, in order. It also holds the unique key of each table that can have several rows per
     * join key, which tells the rows of a group apart; a table with one row per join key needs
     * none, since a row of the other table is either joined with that row or padded. A key whose
     * columns are not all in the output is left out.
     */
    public static Set<ImmutableBitSet> joinKeyGroupOutputKeys(JoinTree tree) {
        PlannerEdge edge = tree.edges.get(0);
        FlinkJoinType joinType = tree.root().joinType;
        ImmutableBitSet.Builder rows = ImmutableBitSet.builder();
        for (int leaf = 0; leaf < 2; leaf++) {
            Optional<int[]> uniqueKey = UpsertKeyUtil.smallestKey(upsertKeys(tree, leaf));
            if (uniqueKey.isEmpty()) {
                return Collections.emptySet();
            }
            ImmutableBitSet joinKey = ImmutableBitSet.of(edge.fieldsOf(leaf));
            if (!joinKey.contains(ImmutableBitSet.of(uniqueKey.get()))) {
                for (int field : uniqueKey.get()) {
                    int position = outputPosition(tree, leaf, field);
                    if (position < 0) {
                        return Collections.emptySet();
                    }
                    rows.set(position);
                }
            }
        }
        List<int[]> groupColumns = new ArrayList<>();
        if (joinType == FlinkJoinType.FULL) {
            groupColumns.add(
                    IntStream.concat(
                                    Arrays.stream(edge.leftFields)
                                            .map(f -> outputPosition(tree, 0, f)),
                                    Arrays.stream(edge.rightFields)
                                            .map(f -> outputPosition(tree, 1, f)))
                            .toArray());
        }
        if (joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.LEFT) {
            groupColumns.add(
                    Arrays.stream(edge.leftFields).map(f -> outputPosition(tree, 0, f)).toArray());
        }
        if (joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.RIGHT) {
            groupColumns.add(
                    Arrays.stream(edge.rightFields).map(f -> outputPosition(tree, 1, f)).toArray());
        }
        ImmutableBitSet rowKey = rows.build();
        Set<ImmutableBitSet> keys = new LinkedHashSet<>();
        for (int[] columns : groupColumns) {
            if (Arrays.stream(columns).allMatch(p -> p >= 0)) {
                keys.add(rowKey.union(ImmutableBitSet.of(columns)));
            }
        }
        return keys;
    }

    /** The output field of the given field of a table, or -1. */
    private static int outputPosition(JoinTree tree, int leaf, int field) {
        FieldMap output = tree.root().output;
        for (int i = 0; i < output.size(); i++) {
            if (output.leafOf(i) == leaf && output.fieldOf(i) == field) {
                return i;
            }
        }
        return -1;
    }

    private static void computeOldImages(JoinTree tree) throws Unsupported {
        tree.needsOldImages = new boolean[tree.numLeaves()];
        for (int leaf = 0; leaf < tree.numLeaves(); leaf++) {
            Set<ImmutableBitSet> keys = upsertKeys(tree, leaf);
            boolean needs = false;
            if (leaf != tree.anchorLeaf) {
                // the columns that connect a row to its anchors must not change in an update
                ImmutableBitSet towardsAnchor = ImmutableBitSet.of(edgeTowardsAnchor(tree, leaf));
                needs = keys.stream().noneMatch(k -> k.contains(towardsAnchor));
            }
            setOldImages(tree, leaf, needs);
        }
    }

    /**
     * Records whether the table must provide old images: because its rows can move, or because a
     * filter on it can start to reject a row.
     */
    private static void setOldImages(JoinTree tree, int leaf, boolean rowsMove) throws Unsupported {
        boolean needs = rowsMove;
        RexProgram calc = tree.leafCalcs.get(leaf);
        if (calc != null && calc.getCondition() != null) {
            // a row that stops passing the filter is only seen through its old image; the filter
            // reads fields of the scan, so compare with the scan's keys
            ImmutableBitSet filterColumns =
                    RelOptUtil.InputFinder.bits(calc.expandLocalRef(calc.getCondition()));
            StreamPhysicalTableSourceScan scan = tree.leafScans.get(leaf);
            Set<ImmutableBitSet> scanKeys =
                    FlinkRelMetadataQuery.reuseOrCreate(scan.getCluster().getMetadataQuery())
                            .getUpsertKeys(scan);
            needs |=
                    scanKeys == null || scanKeys.stream().noneMatch(k -> k.contains(filterColumns));
        }
        tree.needsOldImages[leaf] = needs;
        if (needs && tree.leafNormalizerRemoved.get(leaf)) {
            throw new Unsupported("input needs old images, but its source is an upsert source");
        }
        ChangelogMode mode = DeltaJoinUtil.declaredChangelogMode(tree.leafScans.get(leaf));
        if (needs && mode.contains(RowKind.UPDATE_AFTER) && !mode.contains(RowKind.UPDATE_BEFORE)) {
            throw new Unsupported("input cannot provide UPDATE_BEFORE");
        }
        if (needs && mode.contains(RowKind.DELETE) && mode.keyOnlyDeletes()) {
            throw new Unsupported("input cannot provide full deletes");
        }
    }

    /** The fields of the given table on the first edge of its path to the anchor. */
    private static int[] edgeTowardsAnchor(JoinTree tree, int leaf) {
        int[] next = new int[tree.numLeaves()];
        Arrays.fill(next, -2);
        next[tree.anchorLeaf] = -1;
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        queue.add(tree.anchorLeaf);
        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (PlannerEdge edge : tree.edges) {
                if (edge.leftLeaf != current && edge.rightLeaf != current) {
                    continue;
                }
                int other = edge.other(current);
                if (next[other] == -2) {
                    next[other] = current;
                    queue.add(other);
                }
            }
        }
        int towards = next[leaf];
        for (PlannerEdge edge : tree.edges) {
            if ((edge.leftLeaf == leaf && edge.rightLeaf == towards)
                    || (edge.rightLeaf == leaf && edge.leftLeaf == towards)) {
                return edge.fieldsOf(leaf);
            }
        }
        throw new IllegalStateException("Table " + leaf + " is not connected to the anchor.");
    }

    private static void computeOutputAnchorKey(JoinTree tree) {
        FieldMap output = tree.root().output;
        int[] positions = new int[tree.anchorKey.length];
        for (int k = 0; k < tree.anchorKey.length; k++) {
            positions[k] = -1;
            for (int i = 0; i < output.size(); i++) {
                if (output.leafOf(i) == tree.anchorLeaf && output.fieldOf(i) == tree.anchorKey[k]) {
                    positions[k] = i;
                    break;
                }
            }
            if (positions[k] < 0) {
                tree.outputAnchorKeyPositions = null;
                return;
            }
        }
        tree.outputAnchorKeyPositions = positions;
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    public static Set<ImmutableBitSet> upsertKeys(JoinTree tree, int leaf) {
        RelNode input = tree.leafInputs.get(leaf);
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> keys = mq.getUpsertKeys(input);
        return keys == null ? Collections.emptySet() : keys;
    }

    /**
     * Maps fields of a table's rows (after its calc) to fields of the table source, or returns null
     * if a field is not a plain column of the source.
     */
    private static @Nullable int[] toSourceFields(JoinTree tree, int leaf, int[] fields) {
        IntPair[] pairs = new IntPair[fields.length];
        for (int i = 0; i < fields.length; i++) {
            pairs[i] = IntPair.of(i, fields[i]);
        }
        IntPair[] mapped =
                TemporalJoinUtil.getTemporalTableJoinKeyPairs(
                        pairs,
                        JavaScalaConversionUtil.toScala(
                                Optional.ofNullable(tree.leafCalcs.get(leaf))));
        if (mapped.length != fields.length) {
            return null;
        }
        return Arrays.stream(mapped).mapToInt(p -> p.target).toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Exec spec
    // --------------------------------------------------------------------------------------------

    /** Creates the exec spec, including the lookups of every edge and of the anchor. */
    public static GroupRefreshDeltaJoinSpec toSpec(JoinTree tree, RelOptCluster cluster) {
        List<RowType> leafTypes =
                tree.leafInputs.stream()
                        .map(input -> FlinkTypeFactory.toLogicalRowType(input.getRowType()))
                        .collect(Collectors.toList());
        List<GroupRefreshDeltaJoinSpec.Node> nodes = new ArrayList<>();
        for (PlannerNode n : tree.nodes) {
            nodes.add(
                    new GroupRefreshDeltaJoinSpec.Node(
                            n.leaf,
                            n.left,
                            n.right,
                            n.joinType,
                            n.edge,
                            n.condition,
                            n.leftType,
                            n.rightType,
                            n.output.getLeaves(),
                            n.output.getFields(),
                            n.filters));
        }
        List<GroupRefreshDeltaJoinSpec.Edge> edges = new ArrayList<>();
        for (PlannerEdge e : tree.edges) {
            JoinInfo info =
                    JoinInfo.of(
                            ImmutableIntList.of(e.leftFields), ImmutableIntList.of(e.rightFields));
            edges.add(
                    new GroupRefreshDeltaJoinSpec.Edge(
                            e.leftLeaf,
                            e.leftFields,
                            e.rightLeaf,
                            e.rightFields,
                            lookupSpec(tree, info, e.rightLeaf, cluster, true),
                            lookupSpec(tree, info, e.leftLeaf, cluster, false)));
        }
        DeltaJoinSpec anchorLookup = null;
        if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR) {
            int[] keyRowFields = new int[tree.anchorKey.length];
            for (int i = 0; i < keyRowFields.length; i++) {
                keyRowFields[i] = i;
            }
            JoinInfo anchorInfo =
                    JoinInfo.of(
                            ImmutableIntList.of(keyRowFields), ImmutableIntList.of(tree.anchorKey));
            anchorLookup = lookupSpec(tree, anchorInfo, tree.anchorLeaf, cluster, true);
        }
        List<GroupRefreshDeltaJoinSpec.GroupRead> groupReads = new ArrayList<>();
        if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR
                && tree.groupReadFields != null) {
            int[] keyRowFields = new int[tree.anchorKey.length];
            for (int i = 0; i < keyRowFields.length; i++) {
                keyRowFields[i] = i;
            }
            for (int leaf = 0; leaf < tree.numLeaves(); leaf++) {
                int[] fields = tree.groupReadFields[leaf];
                if (fields != null) {
                    JoinInfo info =
                            JoinInfo.of(ImmutableIntList.of(keyRowFields), ImmutableIntList.of(fields));
                    groupReads.add(
                            new GroupRefreshDeltaJoinSpec.GroupRead(
                                    leaf, fields, lookupSpec(tree, info, leaf, cluster, true)));
                }
            }
        }
        // the rows a table returns to lookups are its current rows, which its unique keys identify
        List<int[]> leafKeys = new ArrayList<>();
        for (RelNode input : tree.leafInputs) {
            FlinkRelMetadataQuery mq =
                    FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
            leafKeys.add(UpsertKeyUtil.smallestKey(mq.getUniqueKeys(input)).orElse(new int[0]));
        }
        return new GroupRefreshDeltaJoinSpec(
                leafTypes,
                nodes,
                edges,
                tree.anchorLeaf,
                tree.anchorKey,
                anchorLookup,
                leafKeys,
                tree.grouping,
                groupReads);
    }

    private static DeltaJoinSpec lookupSpec(
            JoinTree tree,
            JoinInfo info,
            int lookupLeaf,
            RelOptCluster cluster,
            boolean rightIsLookupSide) {
        TableSourceTable table = tree.leafScans.get(lookupLeaf).tableSourceTable();
        return DeltaJoinUtil.getDeltaJoinSpec(
                info, table, tree.leafCalcs.get(lookupLeaf), cluster, rightIsLookupSide);
    }
}
