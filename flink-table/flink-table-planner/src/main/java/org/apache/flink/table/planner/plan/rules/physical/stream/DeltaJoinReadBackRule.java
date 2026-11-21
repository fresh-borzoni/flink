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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsReadBackAfterCheckpoint;
import org.apache.flink.table.connector.sink.abilities.SupportsReadYourWrites;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.sink.ReadYourWritesSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupRefreshDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.GroupRefreshDeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.UpsertKeyUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorJoinTree.FieldMap;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Lets a group refresh delta join read the rows it emitted back from the table of its sink, instead
 * of keeping them in state. The calc between the join and the sink moves into the join, which then
 * emits the rows of the table.
 *
 * <p>It applies if the join can read what it wrote ({@link SupportsReadYourWrites} in the task of
 * the join, or {@link SupportsReadBackAfterCheckpoint} with aligned checkpoints), and the primary
 * key of the table holds the group key and tells the rows of a group apart. The join is assumed to
 * be the only writer of the rows of its groups.
 */
@Value.Enclosing
public class DeltaJoinReadBackRule extends RelRule<DeltaJoinReadBackRule.Config> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaJoinReadBackRule.class);

    public static final DeltaJoinReadBackRule INSTANCE =
            DeltaJoinReadBackRule.Config.DEFAULT.toRule();

    private DeltaJoinReadBackRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // the join exists only if the option or the hint enabled it; a join forced to read its
        // tables as of epochs never reads its output back
        TableConfig config = unwrapTableConfig(call);
        return config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK)
                || config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_VERSIONED)
                        != OptimizerConfigOptions.DeltaJoinVersioned.NONE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalSink sink = call.rel(0);
        RelNode input = unwrap(sink.getInput());
        StreamPhysicalCalc calc = null;
        if (input instanceof StreamPhysicalCalc) {
            calc = (StreamPhysicalCalc) input;
            input = unwrap(calc.getInput());
        }
        if (!(input instanceof StreamPhysicalGroupRefreshDeltaJoin)) {
            return;
        }
        StreamPhysicalGroupRefreshDeltaJoin join = (StreamPhysicalGroupRefreshDeltaJoin) input;
        if (join.getReadBack() != null) {
            return;
        }
        RexProgram projection =
                calc != null
                        ? calc.getProgram()
                        : RexProgram.createIdentity(join.getRowType(), join.getRowType());
        String table = sink.contextResolvedTable().getIdentifier().asSummaryString();
        TableConfig config = unwrapTableConfig(sink);
        OptimizerConfigOptions.DeltaJoinVersioned versioned =
                config.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_VERSIONED);
        boolean readBackEnabled =
                config.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK);
        if (versioned != OptimizerConfigOptions.DeltaJoinVersioned.NONE) {
            Analysis readYourWrites =
                    readBackEnabled ? new Analysis(join, projection, sink, false) : null;
            boolean preferReadYourWrites =
                    versioned == OptimizerConfigOptions.DeltaJoinVersioned.AUTO
                            && readYourWrites != null
                            && readYourWrites.readBack != null
                            && readYourWrites.readYourWritesSink != null;
            if (!preferReadYourWrites && emitsVersioned(call, sink, join, projection, versioned)) {
                return;
            }
            if (!readBackEnabled || versioned == OptimizerConfigOptions.DeltaJoinVersioned.FORCE) {
                // the join reads its tables as of epochs without an output table, if it can
                return;
            }
        }
        Analysis analysis = new Analysis(join, projection, sink, false);
        // the join may be grouped otherwise: by the join key if it has a parent, so that a child
        // refreshes only its own rows and a parent reads its children at once; otherwise by what
        // the output table is keyed by
        StreamPhysicalGroupRefreshDeltaJoin alternative = join.getAlternative();
        if (alternative != null
                && (GroupRefreshDeltaJoinUtil.hasParent(alternative.getTree())
                        || !analysis.keyedByGroups())) {
            Analysis other = new Analysis(alternative, projection, sink, false);
            if (other.readBack != null) {
                LOG.info(
                        "The delta join that writes {} groups by the join key, by which its rows "
                                + "can be read back from it.",
                        table);
                join = alternative;
                analysis = other;
            }
        }
        if (analysis.readBack == null) {
            LOG.info(
                    "The delta join that writes {} cannot read its rows back: {}.",
                    table,
                    analysis.reason);
            join.getTree().readBackRefusal = table + ": " + analysis.reason;
            return;
        }
        LOG.info(
                "The delta join that writes {} reads the rows of a group back from it instead of "
                        + "keeping them in state, {}.",
                table,
                analysis.readYourWritesSink != null
                        ? "at any time, since the sink lets the join read what it wrote"
                        : "once a checkpoint completed after the group was written");
        RelNode readingBack = join.withReadBack(analysis.readBack);
        if (analysis.readYourWritesSink == null) {
            call.transformTo(sink.copy(sink.getTraitSet(), Collections.singletonList(readingBack)));
            return;
        }
        SinkAbilitySpec[] abilities =
                Arrays.copyOf(sink.abilitySpecs(), sink.abilitySpecs().length + 1);
        abilities[abilities.length - 1] = new ReadYourWritesSpec();
        call.transformTo(
                new StreamPhysicalSink(
                        sink.getCluster(),
                        sink.getTraitSet(),
                        readingBack,
                        sink.hints(),
                        sink.contextResolvedTable(),
                        analysis.readYourWritesSink,
                        sink.targetColumns(),
                        abilities,
                        sink.upsertMaterialize(),
                        sink.conflictStrategy()));
    }

    /**
     * Lets the join read its tables as of epochs and emit the rows of the output table, keyed by
     * its primary key, so that the sink needs no materialization. Returns false if it cannot.
     */
    private static boolean emitsVersioned(
            RelOptRuleCall call,
            StreamPhysicalSink sink,
            StreamPhysicalGroupRefreshDeltaJoin join,
            RexProgram projection,
            OptimizerConfigOptions.DeltaJoinVersioned versioned) {
        StreamPhysicalGroupRefreshDeltaJoin anchored = join;
        if (join.getTree().grouping != GroupRefreshDeltaJoinSpec.Grouping.ANCHOR
                && join.getAlternative() != null
                && join.getAlternative().getTree().grouping
                        == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR) {
            anchored = join.getAlternative();
        }
        String domain =
                DeltaJoinVersionedRule.domainOf(
                        anchored, versioned == OptimizerConfigOptions.DeltaJoinVersioned.FORCE);
        if (domain == null) {
            return false;
        }
        Analysis analysis = new Analysis(anchored, projection, sink, true);
        String table = sink.contextResolvedTable().getIdentifier().asSummaryString();
        if (analysis.readBack == null) {
            LOG.info(
                    "The delta join that writes {} reads its tables as of epochs, but emits its "
                            + "joined rows: {}.",
                    table,
                    analysis.reason);
            return false;
        }
        LOG.info(
                "The delta join that writes {} reads its tables as of epochs and emits the rows of "
                        + "the table, keyed by its primary key.",
                table);
        RelNode versionedJoin =
                anchored.withVersioned(
                        domain,
                        DeltaJoinVersionedRule.inputsWithEpochs(anchored, domain),
                        analysis.readBack);
        call.transformTo(sink.copy(sink.getTraitSet(), Collections.singletonList(versionedJoin)));
        return true;
    }

    /**
     * A copy of the sink that makes the rows it receives visible to the lookups of its task, if it
     * can and runs in the task of the join; null otherwise.
     */
    private static @Nullable DynamicTableSink readYourWritesSink(
            StreamPhysicalSink sink, Configuration configuration) {
        if (!(sink.tableSink() instanceof SupportsReadYourWrites)
                || !configuration.get(PipelineOptions.OPERATOR_CHAINING)
                // both shuffle by the primary key before the sink
                || configuration.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE)
                        == ExecutionConfigOptions.SinkKeyedShuffle.FORCE
                || configuration.get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE)
                        == ExecutionConfigOptions.UpsertMaterialize.FORCE) {
            return null;
        }
        DynamicTableSink applied = sink.tableSink().copy();
        if (!((SupportsReadYourWrites) applied).applyReadYourWrites()) {
            return null;
        }
        // a parallelism of its own would redistribute the rows before the sink
        DynamicTableSink.SinkRuntimeProvider provider =
                applied.getSinkRuntimeProvider(
                        new SinkRuntimeProviderContext(false, sink.targetColumns()));
        return provider instanceof ParallelismProvider
                        && ((ParallelismProvider) provider).getParallelism().isPresent()
                ? null
                : applied;
    }

    private static RelNode unwrap(RelNode node) {
        return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
    }

    /** Whether, and how, a join can read its rows back from the table of a sink. */
    private static final class Analysis {

        private final StreamPhysicalGroupRefreshDeltaJoin join;
        private final GroupRefreshDeltaJoinUtil.JoinTree tree;
        private final RowType joinedType;
        private final FieldMap output;
        private final List<RexNode> projection = new ArrayList<>();
        private final List<Integer> primaryKey = new ArrayList<>();

        /** The primary key of the output table, whatever the analysis decides. */
        private final List<Integer> primaryKeyColumns = new ArrayList<>();

        private @Nullable String reason;
        private @Nullable StreamPhysicalGroupRefreshDeltaJoin.ReadBack readBack;

        /** The sink, made to let the join read what it wrote, if it can. */
        private @Nullable DynamicTableSink readYourWritesSink;

        /**
         * Whether the join reads its tables as of epochs instead of reading its output back: then
         * the output table need not be readable, only keyed by the groups.
         */
        private final boolean versioned;

        private Analysis(
                StreamPhysicalGroupRefreshDeltaJoin join,
                RexProgram program,
                StreamPhysicalSink sink,
                boolean versioned) {
            this.versioned = versioned;
            this.join = join;
            this.tree = join.getTree();
            this.joinedType = FlinkTypeFactory.toLogicalRowType(join.getRowType());
            this.output = tree.root().output;
            for (RexLocalRef ref : program.getProjectList()) {
                projection.add(program.expandLocalRef(ref));
            }
            ResolvedSchema schema = sink.contextResolvedTable().getResolvedSchema();
            if (projection.size() == schema.getColumnCount()) {
                schema.getPrimaryKey()
                        .ifPresent(
                                key ->
                                        key.getColumns()
                                                .forEach(
                                                        column ->
                                                                primaryKeyColumns.add(
                                                                        schema.getColumnNames()
                                                                                .indexOf(column))));
            }
            this.reason = analyze(program, sink);
        }

        private @Nullable String analyze(RexProgram program, StreamPhysicalSink sink) {
            if (!versioned && takesUpsertsByAnchorKey(sink)) {
                return "every group has at most one row, which the join upserts by its key";
            }
            if (sink.targetColumns() != null) {
                return "the sink writes only some columns, so it reads back rows it did not write";
            }
            TableConfig tableConfig = unwrapTableConfig(sink);
            Configuration configuration = Configuration.fromMap(tableConfig.toMap());
            readYourWritesSink = versioned ? null : readYourWritesSink(sink, configuration);
            if (!versioned && readYourWritesSink == null) {
                if (!configuration
                        .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL)
                        .isPresent()) {
                    return "checkpoints are disabled, so the rows it writes never become readable";
                }
                if (CheckpointingOptions.isUnalignedCheckpointEnabled(configuration)) {
                    return "unaligned checkpoints do not prove that the sink received the rows";
                }
                if (!(sink.tableSink() instanceof SupportsReadBackAfterCheckpoint)) {
                    return "the sink does not declare that its rows can be read back";
                }
            }
            // a primary key is written by one group, in order: there are no conflicts to resolve,
            // and the latest row of a key is its current row
            InsertConflictStrategy conflicts = sink.conflictStrategy();
            if (conflicts != null
                    && conflicts.getBehavior()
                            != InsertConflictStrategy.ConflictBehavior.DEDUPLICATE) {
                return "the sink resolves conflicts other than by keeping the latest row";
            }
            if (!program.getExprList().stream().allMatch(RexUtil::isDeterministic)) {
                return "the projection is not deterministic";
            }

            ContextResolvedTable table = sink.contextResolvedTable();
            ResolvedSchema schema = table.getResolvedSchema();
            if (!schema.getColumns().stream().allMatch(Column::isPhysical)) {
                return "the output table has computed or metadata columns";
            }
            if (schema.getPrimaryKey().isEmpty()) {
                return "the output table has no primary key";
            }
            if (projection.size() != schema.getColumnCount()) {
                return "the projection does not produce the columns of the output table";
            }
            for (String column : schema.getPrimaryKey().map(UniqueConstraint::getColumns).get()) {
                primaryKey.add(schema.getColumnNames().indexOf(column));
            }

            // the columns that hold the group key
            int arity = groupKeyArity();
            if (arity == 0) {
                return "the output does not hold the anchor key";
            }
            int[] groupColumns = new int[arity];
            for (int i = 0; i < arity; i++) {
                LogicalType keyType = groupKeyType(i);
                groupColumns[i] = -1;
                for (int column : primaryKey) {
                    LogicalType columnType =
                            schema.getColumnDataTypes().get(column).getLogicalType();
                    if (holdsGroupKey(strip(projection.get(column)), i)
                            && columnType.copy(true).equals(keyType.copy(true))) {
                        groupColumns[i] = column;
                        break;
                    }
                }
                if (groupColumns[i] < 0) {
                    return "no primary key column holds the group key of every row";
                }
            }
            List<String> groupColumnNames =
                    Arrays.stream(groupColumns)
                            .mapToObj(schema.getColumnNames()::get)
                            .collect(Collectors.toList());
            boolean indexed =
                    versioned
                            || schema.getIndexes().stream()
                                    .map(Index::getColumns)
                                    .anyMatch(groupColumnNames::containsAll);
            if (!indexed) {
                return String.format(
                        "the output table has no index on the columns %s that hold the group key",
                        groupColumnNames);
            }

            String apart =
                    tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY
                            ? joinKeyRowsApart()
                            : anchorRowsApart();
            if (apart != null) {
                return apart;
            }

            try {
                if (!versioned) {
                    new DynamicTableSourceSpec(table, Collections.emptyList())
                            .getLookupTableSource(unwrapContext(sink), unwrapTypeFactory(sink));
                }
            } catch (Throwable t) {
                return "the output table cannot be looked up: " + t.getMessage();
            }
            readBack =
                    new StreamPhysicalGroupRefreshDeltaJoin.ReadBack(
                            program,
                            table,
                            groupColumns,
                            primaryKey.stream().mapToInt(Integer::intValue).toArray(),
                            readYourWritesSink != null);
            return null;
        }

        /**
         * Whether the output table is keyed by the groups of the join: it can be read back by them,
         * or every group has at most one row and the primary key of the table is the anchor key, so
         * that the join upserts by it.
         */
        private boolean keyedByGroups() {
            return readBack != null || primaryKeyIsAnchorKey();
        }

        /**
         * Whether the join can upsert by the anchor key instead of reading back: single-row groups
         * keyed by it and a sink taking key deletes. Asked from the sink, before changelog
         * inference.
         */
        private boolean takesUpsertsByAnchorKey(StreamPhysicalSink sink) {
            if (!primaryKeyIsAnchorKey() || sink.conflictStrategy() != null) {
                return false;
            }
            ChangelogMode mode;
            try {
                mode = sink.tableSink().getChangelogMode(ChangelogMode.all());
            } catch (Throwable t) {
                return false;
            }
            return !mode.contains(RowKind.UPDATE_BEFORE) && mode.keyOnlyDeletes();
        }

        /** Every group has at most one row, and the primary key of the table is the anchor key. */
        private boolean primaryKeyIsAnchorKey() {
            if (tree.grouping != GroupRefreshDeltaJoinSpec.Grouping.ANCHOR
                    || !tree.singleRowGroups
                    || tree.outputAnchorKeyPositions == null
                    || primaryKeyColumns.size() != tree.outputAnchorKeyPositions.length) {
                return false;
            }
            for (int i = 0; i < tree.outputAnchorKeyPositions.length; i++) {
                int field = i;
                if (primaryKeyColumns.stream()
                        .noneMatch(column -> holdsGroupKey(strip(projection.get(column)), field))) {
                    return false;
                }
            }
            return true;
        }

        // ---------------------------------------------------------------------------- group key

        private int groupKeyArity() {
            if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY) {
                return tree.edges.get(0).leftFields.length;
            }
            return tree.outputAnchorKeyPositions == null ? 0 : tree.outputAnchorKeyPositions.length;
        }

        private LogicalType groupKeyType(int i) {
            if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY) {
                RowType leftType =
                        FlinkTypeFactory.toLogicalRowType(tree.leafInputs.get(0).getRowType());
                return leftType.getTypeAt(tree.edges.get(0).leftFields[i]);
            }
            return joinedType.getTypeAt(tree.outputAnchorKeyPositions[i]);
        }

        /**
         * Whether the expression holds field {@code i} of the group key in every row. The anchor is
         * in every row. A join key is held by the preserved input, by either input of an inner
         * join, or by both coalesced; rows without a key may hold a constant.
         */
        private boolean holdsGroupKey(RexNode expr, int i) {
            List<RexNode> operands = withoutTrailingConstant(expr);
            if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR) {
                return operands.size() == 1
                        && isRef(operands.get(0), tree.outputAnchorKeyPositions[i]);
            }
            FlinkJoinType joinType = tree.root().joinType;
            int left = positionOf(0, tree.edges.get(0).leftFields[i]);
            int right = positionOf(1, tree.edges.get(0).rightFields[i]);
            if (operands.size() == 1 && isRef(operands.get(0), left)) {
                return joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.LEFT;
            }
            if (operands.size() == 1 && isRef(operands.get(0), right)) {
                return joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.RIGHT;
            }
            return operands.size() == 2
                    && ((isRef(operands.get(0), left) && isRef(operands.get(1), right))
                            || (isRef(operands.get(0), right) && isRef(operands.get(1), left)));
        }

        // ---------------------------------------------------------------------------- rows apart

        /** The unique keys of both tables, except their join keys, tell the rows apart. */
        private @Nullable String joinKeyRowsApart() {
            boolean[] uniquePerKey = new boolean[2];
            for (int leaf = 0; leaf < 2; leaf++) {
                Optional<int[]> key =
                        UpsertKeyUtil.smallestKey(GroupRefreshDeltaJoinUtil.upsertKeys(tree, leaf));
                if (key.isEmpty()) {
                    return String.format("table #%s has no unique key", leaf);
                }
                int[] joinKey =
                        leaf == 0 ? tree.edges.get(0).leftFields : tree.edges.get(0).rightFields;
                uniquePerKey[leaf] = true;
                for (int field : key.get()) {
                    if (contains(joinKey, field)) {
                        continue;
                    }
                    uniquePerKey[leaf] = false;
                    if (!keyHolds(positionOf(leaf, field))) {
                        return String.format(
                                "the primary key does not hold the unique key of table #%s", leaf);
                    }
                }
            }
            if (tree.root().joinType == FlinkJoinType.FULL && uniquePerKey[0] && uniquePerKey[1]) {
                return "a full join of rows that are unique per join key can pad both inputs in "
                        + "one group, and no primary key column tells those rows apart";
            }
            return null;
        }

        /**
         * A unique key declared for the joined rows tells them apart, or the unique keys of all
         * other tables do, see {@link #heldFields()}.
         */
        private @Nullable String anchorRowsApart() {
            int[] anchorPositions = tree.outputAnchorKeyPositions;
            Set<ImmutableBitSet> declared = new HashSet<>(join.getOutputUpsertKeys());
            if (join.getOutputUniqueKeys() != null) {
                declared.addAll(join.getOutputUniqueKeys());
            }
            for (ImmutableBitSet key : declared) {
                if (key.asList().stream()
                        .allMatch(field -> contains(anchorPositions, field) || keyHolds(field))) {
                    return null;
                }
            }
            Set<List<Integer>> held = heldFields();
            for (int leaf = 0; leaf < tree.numLeaves(); leaf++) {
                if (leaf == tree.anchorLeaf) {
                    continue;
                }
                Optional<int[]> key =
                        UpsertKeyUtil.smallestKey(GroupRefreshDeltaJoinUtil.upsertKeys(tree, leaf));
                if (key.isEmpty()) {
                    return String.format("table #%s has no unique key", leaf);
                }
                for (int field : key.get()) {
                    if (!held.contains(Arrays.asList(leaf, field))) {
                        return String.format(
                                "the primary key does not hold the unique key of table #%s", leaf);
                    }
                }
            }
            return null;
        }

        /**
         * Key fields that need no column of their own: held by a primary key column, tied to the
         * anchor key, or equal to such a field through an equi-join that drops their unmatched
         * rows.
         */
        private Set<List<Integer>> heldFields() {
            Set<List<Integer>> held = fieldsTiedToAnchorKey();
            for (int i = 0; i < output.size(); i++) {
                if (keyHolds(i)) {
                    held.add(Arrays.asList(output.leafOf(i), output.fieldOf(i)));
                }
            }
            boolean grown = true;
            while (grown) {
                grown = false;
                for (GroupRefreshDeltaJoinUtil.PlannerNode node : tree.nodes) {
                    if (node.isLeaf() || node.joinType == FlinkJoinType.FULL) {
                        continue;
                    }
                    GroupRefreshDeltaJoinUtil.PlannerEdge edge = tree.edges.get(node.edge);
                    boolean rightFollows = node.joinType != FlinkJoinType.RIGHT;
                    boolean leftFollows = node.joinType != FlinkJoinType.LEFT;
                    for (int i = 0; i < edge.leftFields.length; i++) {
                        List<Integer> left = Arrays.asList(edge.leftLeaf, edge.leftFields[i]);
                        List<Integer> right = Arrays.asList(edge.rightLeaf, edge.rightFields[i]);
                        if (rightFollows && held.contains(left) && held.add(right)) {
                            grown = true;
                        }
                        if (leftFollows && held.contains(right) && held.add(left)) {
                            grown = true;
                        }
                    }
                }
            }
            return held;
        }

        /** The (table, field) pairs that equality columns tie to the anchor key, transitively. */
        private Set<List<Integer>> fieldsTiedToAnchorKey() {
            Set<List<Integer>> tied = new HashSet<>();
            for (int field : tree.anchorKey) {
                tied.add(Arrays.asList(tree.anchorLeaf, field));
            }
            boolean grown = true;
            while (grown) {
                grown = false;
                for (GroupRefreshDeltaJoinUtil.PlannerEdge edge : tree.edges) {
                    for (int i = 0; i < edge.leftFields.length; i++) {
                        List<Integer> left = Arrays.asList(edge.leftLeaf, edge.leftFields[i]);
                        List<Integer> right = Arrays.asList(edge.rightLeaf, edge.rightFields[i]);
                        if (tied.contains(left) != tied.contains(right)) {
                            tied.add(left);
                            tied.add(right);
                            grown = true;
                        }
                    }
                }
            }
            return tied;
        }

        /** Whether a primary key column holds the joined field, or a constant if it is null. */
        private boolean keyHolds(int field) {
            if (field < 0) {
                return false;
            }
            for (int column : primaryKey) {
                RexNode expr = strip(projection.get(column));
                if (isRef(expr, field)) {
                    return true;
                }
                List<RexNode> operands = coalesced(expr);
                if (operands.size() == 2
                        && isRef(operands.get(0), field)
                        && operands.get(1) instanceof RexLiteral) {
                    return true;
                }
            }
            return false;
        }

        /** The position of a table's field in the joined row, or -1. */
        private int positionOf(int leaf, int field) {
            for (int i = 0; i < output.size(); i++) {
                if (output.leafOf(i) == leaf && output.fieldOf(i) == field) {
                    return i;
                }
            }
            return -1;
        }
    }

    // -------------------------------------------------------------------------------- expressions

    /** The operands of a coalesce without a trailing constant, or the expression itself. */
    private static List<RexNode> withoutTrailingConstant(RexNode expr) {
        List<RexNode> operands = new ArrayList<>(coalesced(expr));
        if (operands.isEmpty()) {
            operands.add(expr);
        } else if (operands.get(operands.size() - 1) instanceof RexLiteral) {
            operands.remove(operands.size() - 1);
        }
        return operands;
    }

    /** Removes casts that only change the nullability. */
    private static RexNode strip(RexNode expr) {
        while (expr.isA(SqlKind.CAST)) {
            RexNode operand = ((RexCall) expr).getOperands().get(0);
            LogicalType from = FlinkTypeFactory.toLogicalType(operand.getType());
            LogicalType to = FlinkTypeFactory.toLogicalType(expr.getType());
            if (!from.copy(true).equals(to.copy(true))) {
                break;
            }
            expr = operand;
        }
        return expr;
    }

    /**
     * The operands of {@code COALESCE(a, b, ...)}, also when written as a CASE or nested, in order;
     * empty if the expression is no coalesce.
     */
    private static List<RexNode> coalesced(RexNode expr) {
        if (!(expr instanceof RexCall)) {
            return Collections.emptyList();
        }
        RexCall call = (RexCall) expr;
        List<RexNode> operands;
        if (call.isA(SqlKind.COALESCE)
                || call.getOperator().getName().equalsIgnoreCase("COALESCE")) {
            operands = call.getOperands();
        } else if (call.isA(SqlKind.CASE)
                && call.getOperands().size() == 3
                && call.getOperands().get(0).isA(SqlKind.IS_NOT_NULL)
                && strip(((RexCall) call.getOperands().get(0)).getOperands().get(0))
                        .equals(strip(call.getOperands().get(1)))) {
            // CASE WHEN a IS NOT NULL THEN a ELSE b END
            operands = Arrays.asList(call.getOperands().get(1), call.getOperands().get(2));
        } else {
            return Collections.emptyList();
        }
        List<RexNode> flat = new ArrayList<>();
        for (RexNode operand : operands) {
            RexNode stripped = strip(operand);
            List<RexNode> nested = coalesced(stripped);
            if (nested.isEmpty()) {
                flat.add(stripped);
            } else {
                flat.addAll(nested);
            }
        }
        return flat;
    }

    private static boolean isRef(RexNode expr, int index) {
        return expr instanceof RexInputRef && ((RexInputRef) expr).getIndex() == index;
    }

    private static boolean contains(int[] fields, int field) {
        return Arrays.stream(fields).anyMatch(f -> f == field);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        DeltaJoinReadBackRule.Config DEFAULT =
                ImmutableDeltaJoinReadBackRule.Config.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalSink.class).anyInputs())
                        .withDescription("DeltaJoinReadBackRule");

        @Override
        default DeltaJoinReadBackRule toRule() {
            return new DeltaJoinReadBackRule(this);
        }
    }
}
