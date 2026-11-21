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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinReadBackSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupRefreshDeltaJoin;
import org.apache.flink.table.planner.plan.trait.DeleteKind;
import org.apache.flink.table.planner.plan.trait.DeleteKindTrait;
import org.apache.flink.table.planner.plan.trait.DeleteKindTraitDef;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTrait;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTraitDef;
import org.apache.flink.table.planner.plan.trait.UpdateKind;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.trait.UpdateKindTraitDef;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.GroupRefreshDeltaJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOutputMode;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Stream physical node of a delta join that refreshes groups: a join without join state that marks,
 * for every change, the group of output rows it may affect, and replaces the group with its current
 * rows read by lookups. A join of two tables (INNER, LEFT, RIGHT or FULL) is grouped by its join
 * key; a tree of INNER, LEFT and RIGHT joins is grouped by the key of an anchor table whose row is
 * in every output row.
 *
 * <p>The inputs are the tables of the tree from left to right. The node is created before
 * changelog-mode inference, which requests old images from the inputs that need them and decides
 * from the consumer's requirements whether the operator keeps the emitted rows ({@link
 * GroupRefreshOutputMode#STATE}) or upserts by group key ({@link GroupRefreshOutputMode#UPSERT}). A
 * rule may let it read its emitted rows back from the output table instead ({@link
 * GroupRefreshOutputMode#READ_BACK}).
 */
public class StreamPhysicalGroupRefreshDeltaJoin extends AbstractRelNode
        implements StreamPhysicalRel {

    private List<RelNode> inputs;
    private final GroupRefreshDeltaJoinUtil.JoinTree tree;
    private final Set<ImmutableBitSet> outputUpsertKeys;
    private final @Nullable Set<ImmutableBitSet> outputUniqueKeys;
    private final @Nullable ReadBack readBack;

    /**
     * The same join grouped otherwise, which {@link
     * org.apache.flink.table.planner.plan.rules.physical.stream.DeltaJoinReadBackRule} may choose
     * when the output table can be read back by its groups only.
     */
    private final @Nullable StreamPhysicalGroupRefreshDeltaJoin alternative;

    /**
     * The epoch domain if the join reads its tables as of epochs of its sources, see {@link
     * GroupRefreshOutputMode#VERSIONED}; null otherwise.
     */
    private final @Nullable String versionedDomain;

    public StreamPhysicalGroupRefreshDeltaJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RelDataType rowType,
            GroupRefreshDeltaJoinUtil.JoinTree tree,
            Set<ImmutableBitSet> outputUpsertKeys,
            @Nullable Set<ImmutableBitSet> outputUniqueKeys) {
        this(
                cluster,
                traitSet,
                inputs,
                rowType,
                tree,
                outputUpsertKeys,
                outputUniqueKeys,
                null,
                null,
                null);
    }

    private StreamPhysicalGroupRefreshDeltaJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RelDataType rowType,
            GroupRefreshDeltaJoinUtil.JoinTree tree,
            Set<ImmutableBitSet> outputUpsertKeys,
            @Nullable Set<ImmutableBitSet> outputUniqueKeys,
            @Nullable ReadBack readBack,
            @Nullable StreamPhysicalGroupRefreshDeltaJoin alternative,
            @Nullable String versionedDomain) {
        super(cluster, traitSet);
        this.inputs = inputs;
        this.rowType = rowType;
        this.tree = tree;
        this.outputUpsertKeys = outputUpsertKeys;
        this.outputUniqueKeys = outputUniqueKeys;
        this.readBack = readBack;
        this.alternative = alternative;
        this.versionedDomain = versionedDomain;
    }

    /** This join, with the same join grouped otherwise as an alternative. */
    public StreamPhysicalGroupRefreshDeltaJoin withAlternative(
            StreamPhysicalGroupRefreshDeltaJoin alternative) {
        return new StreamPhysicalGroupRefreshDeltaJoin(
                getCluster(),
                getTraitSet(),
                inputs,
                getRowType(),
                tree,
                outputUpsertKeys,
                outputUniqueKeys,
                readBack,
                alternative,
                versionedDomain);
    }

    /** The same join grouped otherwise, on the current inputs of this join; null if none. */
    public @Nullable StreamPhysicalGroupRefreshDeltaJoin getAlternative() {
        return alternative == null
                ? null
                : (StreamPhysicalGroupRefreshDeltaJoin)
                        alternative.copy(alternative.getTraitSet(), inputs);
    }

    /**
     * How the join writes the rows of an output table and reads them back instead of keeping them
     * in state: a projection from the joined rows to the rows of the table, which the join emits.
     */
    public static final class ReadBack {
        /** From the joined row to a row of the output table, with an optional filter. */
        public final RexProgram projection;

        public final ContextResolvedTable outputTable;

        /** The columns of the output table that hold the group key. */
        public final int[] groupColumns;

        /** The primary key of the output table. */
        public final int[] primaryKey;

        /**
         * Whether a lookup of the output table sees every row the join emitted before it, or only
         * the rows emitted before a completed checkpoint.
         */
        public final boolean readYourWrites;

        public ReadBack(
                RexProgram projection,
                ContextResolvedTable outputTable,
                int[] groupColumns,
                int[] primaryKey,
                boolean readYourWrites) {
            this.projection = projection;
            this.outputTable = outputTable;
            this.groupColumns = groupColumns;
            this.primaryKey = primaryKey;
            this.readYourWrites = readYourWrites;
        }
    }

    /**
     * This join, emitting the rows of the output table of the given read-back. Its rows are unique
     * on the primary key of the table, which every change of a row comes from one group in order.
     */
    public StreamPhysicalGroupRefreshDeltaJoin withReadBack(ReadBack readBack) {
        Set<ImmutableBitSet> key = Collections.singleton(ImmutableBitSet.of(readBack.primaryKey));
        return new StreamPhysicalGroupRefreshDeltaJoin(
                getCluster(),
                getTraitSet(),
                inputs,
                readBack.projection.getOutputRowType(),
                tree,
                key,
                key,
                readBack,
                null,
                null);
    }

    /**
     * This join on the given inputs, whose scans cut their changes into the epochs of the domain,
     * reading its tables as of these epochs. With an output, it emits the rows of the output table,
     * keyed by its primary key, as with read-back, but never reads them back.
     */
    public StreamPhysicalGroupRefreshDeltaJoin withVersioned(
            String domain, List<RelNode> inputs, @Nullable ReadBack output) {
        if (output == null) {
            return new StreamPhysicalGroupRefreshDeltaJoin(
                    getCluster(),
                    getTraitSet(),
                    inputs,
                    getRowType(),
                    tree,
                    outputUpsertKeys,
                    outputUniqueKeys,
                    null,
                    null,
                    domain);
        }
        Set<ImmutableBitSet> key = Collections.singleton(ImmutableBitSet.of(output.primaryKey));
        return new StreamPhysicalGroupRefreshDeltaJoin(
                getCluster(),
                getTraitSet(),
                inputs,
                output.projection.getOutputRowType(),
                tree,
                key,
                key,
                output,
                null,
                domain);
    }

    public @Nullable String getVersionedDomain() {
        return versionedDomain;
    }

    public @Nullable ReadBack getReadBack() {
        return readBack;
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        List<RelNode> newInputs = new ArrayList<>(inputs);
        newInputs.set(ordinalInParent, p);
        this.inputs = List.copyOf(newInputs);
        recomputeDigest();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalGroupRefreshDeltaJoin(
                getCluster(),
                traitSet,
                inputs,
                getRowType(),
                tree,
                outputUpsertKeys,
                outputUniqueKeys,
                readBack,
                alternative,
                versionedDomain);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = 100.0d * inputs.size();
        return planner.getCostFactory().makeCost(rows, rows, 0);
    }

    public GroupRefreshDeltaJoinUtil.JoinTree getTree() {
        return tree;
    }

    /** Whether the given input must provide UPDATE_BEFORE and full deletes. */
    public boolean needsOldImages(int input) {
        // read as of epochs, every change needs its row as of the epoch before
        return tree.needsOldImages[input] || versionedDomain != null;
    }

    /** Upsert keys of the output: unique keys of the join that contain the anchor key. */
    public Set<ImmutableBitSet> getOutputUpsertKeys() {
        return outputUpsertKeys;
    }

    public @Nullable Set<ImmutableBitSet> getOutputUniqueKeys() {
        return outputUniqueKeys;
    }

    /**
     * The output mode follows from the changelog the consumer requires. Remembering only whether it
     * emitted a row per anchor key, the operator can upsert one row per anchor key and delete it by
     * key, possibly repeating an upsert.
     */
    public GroupRefreshOutputMode getOutputMode() {
        if (versionedDomain != null) {
            return GroupRefreshOutputMode.VERSIONED;
        }
        if (readBack != null) {
            return GroupRefreshOutputMode.READ_BACK;
        }
        UpdateKindTrait updateKind = getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
        DeleteKindTrait deleteKind = getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
        DuplicateChangesTrait duplicates =
                getTraitSet().getTrait(DuplicateChangesTraitDef.INSTANCE);
        boolean upsert =
                tree.singleRowGroups
                        && tree.outputAnchorKeyPositions != null
                        && updateKind != null
                        && updateKind.updateKind() == UpdateKind.ONLY_UPDATE_AFTER
                        && deleteKind != null
                        && deleteKind.deleteKind() == DeleteKind.DELETE_BY_KEY
                        && duplicates != null
                        && duplicates.getDuplicateChanges() == DuplicateChanges.ALLOW;
        return upsert ? GroupRefreshOutputMode.UPSERT : GroupRefreshOutputMode.STATE;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        for (int i = 0; i < inputs.size(); i++) {
            pw.input("input#" + i, inputs.get(i));
        }
        boolean anchorGroups = tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR;
        String anchorKey = "";
        if (anchorGroups) {
            List<String> anchorTableFields =
                    tree.leafInputs.get(tree.anchorLeaf).getRowType().getFieldNames();
            anchorKey =
                    Arrays.stream(tree.anchorKey)
                            .mapToObj(anchorTableFields::get)
                            .collect(Collectors.joining(", "));
        }
        boolean traitsInferred =
                getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE()) != null
                        && getTraitSet().getTrait(DuplicateChangesTraitDef.INSTANCE) != null
                        && getTraitSet()
                                        .getTrait(DuplicateChangesTraitDef.INSTANCE)
                                        .getDuplicateChanges()
                                != DuplicateChanges.NONE;
        List<String> conditions = new ArrayList<>();
        List<String> filters = new ArrayList<>();
        for (GroupRefreshDeltaJoinUtil.PlannerNode node : tree.nodes) {
            if (node.isLeaf()) {
                continue;
            }
            List<String> joinFields = new ArrayList<>(node.leftType.getFieldNames());
            joinFields.addAll(node.rightType.getFieldNames());
            conditions.add(
                    FlinkRexUtil.getExpressionString(
                            node.condition, JavaScalaConversionUtil.toScala(joinFields)));
            for (GroupRefreshDeltaJoinSpec.Filter filter : node.filters) {
                filters.add(
                        FlinkRexUtil.getExpressionString(
                                filter.getCondition(),
                                JavaScalaConversionUtil.toScala(
                                        filter.getInputType().getFieldNames())));
            }
        }
        return pw.item("joinTree", tree.describe())
                .item("joinConditions", String.join("; ", conditions))
                .itemIf("filtersBetweenJoins", String.join("; ", filters), !filters.isEmpty())
                .item(
                        "groups",
                        anchorGroups
                                ? "anchor #" + tree.anchorLeaf + "(" + anchorKey + ")"
                                : "join key")
                .itemIf("readsGroupsOf", groupReadsString(), groupReadsString() != null)
                .itemIf("mode", traitsInferred ? getOutputMode() : null, traitsInferred)
                .itemIf(
                        "readBack",
                        readBack == null
                                ? null
                                : readBack.outputTable.getIdentifier().asSummaryString(),
                        readBack != null && versionedDomain == null)
                .itemIf(
                        "readableAfter",
                        readBack == null ? null : readBack.readYourWrites ? "write" : "checkpoint",
                        readBack != null && versionedDomain == null)
                .itemIf(
                        "output",
                        readBack == null
                                ? null
                                : readBack.outputTable.getIdentifier().asSummaryString(),
                        readBack != null && versionedDomain != null)
                .itemIf("projection", projectionString(), readBack != null)
                .itemIf("epochs", versionedDomain, versionedDomain != null)
                .item("select", String.join(", ", getRowType().getFieldNames()));
    }

    /** The tables whose rows of a group a refresh reads at once, e.g. {@code #2, #3}; or null. */
    private @Nullable String groupReadsString() {
        if (tree.grouping != GroupRefreshDeltaJoinSpec.Grouping.ANCHOR
                || tree.groupReadFields == null) {
            return null;
        }
        List<String> leaves = new ArrayList<>();
        for (int leaf = 0; leaf < tree.groupReadFields.length; leaf++) {
            if (tree.groupReadFields[leaf] != null) {
                leaves.add("#" + leaf);
            }
        }
        return leaves.isEmpty() ? null : String.join(", ", leaves);
    }

    private @Nullable String projectionString() {
        if (readBack == null) {
            return null;
        }
        RexProgram program = readBack.projection;
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> expressions = new ArrayList<>();
        for (RexLocalRef ref : program.getProjectList()) {
            expressions.add(
                    FlinkRexUtil.getExpressionString(
                            program.expandLocalRef(ref),
                            JavaScalaConversionUtil.toScala(inputFields)));
        }
        String projection = String.join(", ", expressions);
        if (program.getCondition() != null) {
            projection +=
                    " WHERE "
                            + FlinkRexUtil.getExpressionString(
                                    program.expandLocalRef(program.getCondition()),
                                    JavaScalaConversionUtil.toScala(inputFields));
        }
        return projection;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        GroupRefreshOutputMode mode = getOutputMode();
        if (mode == GroupRefreshOutputMode.STATE
                && !unwrapTableConfig(this)
                        .getOptional(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK)
                        .map(readBack -> !readBack)
                        .orElse(false)) {
            // after a failure the state forgets rows emitted after the checkpoint, which a sink
            // that is not rolled back keeps
            throw new ValidationException(
                    String.format(
                            "A group refresh delta join cannot read the rows it emitted back (%s), "
                                    + "so it would keep them in state. That is correct only if "
                                    + "every sink discards the writes of a failed attempt, as "
                                    + "exactly-once sinks do. Set '%s' to false to keep the rows "
                                    + "in state.",
                            tree.readBackRefusal != null
                                    ? tree.readBackRefusal
                                    : "its rows do not go to a sink directly",
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK
                                    .key()));
        }
        List<InputProperty> inputProperties =
                Collections.nCopies(inputs.size(), InputProperty.DEFAULT);
        // every key the planner declared for the output must hold in every emitted group
        Set<ImmutableBitSet> declared = new java.util.LinkedHashSet<>(outputUpsertKeys);
        if (outputUniqueKeys != null) {
            declared.addAll(outputUniqueKeys);
        }
        // with read-back, the keys hold for the rows of the output table, and the operator keeps
        // its groups unique on the primary key of the table instead
        int[][] declaredKeys =
                readBack == null
                        ? declared.stream().map(ImmutableBitSet::toArray).toArray(int[][]::new)
                        : versionedDomain != null
                                ? new int[][] {readBack.primaryKey}
                                : new int[0][];
        return new StreamExecGroupRefreshDeltaJoin(
                unwrapTableConfig(this),
                GroupRefreshDeltaJoinUtil.toSpec(tree, getCluster()),
                mode,
                mode == GroupRefreshOutputMode.UPSERT ? tree.outputAnchorKeyPositions : null,
                declaredKeys,
                inputProperties,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription(),
                readBack == null ? null : toReadBackSpec(readBack),
                dropsUpdateBefore(),
                versionedDomain);
    }

    /** Whether the consumer requires only the new row of an update. */
    private boolean dropsUpdateBefore() {
        UpdateKindTrait updateKind = getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
        return updateKind != null && updateKind.updateKind() == UpdateKind.ONLY_UPDATE_AFTER;
    }

    private DeltaJoinReadBackSpec toReadBackSpec(ReadBack readBack) {
        RexProgram program = readBack.projection;
        List<RexNode> projection = new ArrayList<>();
        for (RexLocalRef ref : program.getProjectList()) {
            projection.add(program.expandLocalRef(ref));
        }
        RexNode condition =
                program.getCondition() == null
                        ? null
                        : program.expandLocalRef(program.getCondition());
        FlinkTypeFactory typeFactory = unwrapTypeFactory(this);
        RowType tableType =
                (RowType)
                        readBack.outputTable
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();
        return new DeltaJoinReadBackSpec(
                FlinkTypeFactory.toLogicalRowType(program.getInputRowType()),
                projection,
                condition,
                new TemporalTableSourceSpec(
                        new DynamicTableSourceSpec(readBack.outputTable, Collections.emptyList()),
                        typeFactory.buildRelNodeRowType(tableType)),
                readBack.groupColumns,
                readBack.primaryKey,
                readBack.readYourWrites);
    }
}
