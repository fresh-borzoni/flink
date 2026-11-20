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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinReadBackSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.DeltaJoinReadBackUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorGroupRefreshOperatorFactory;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorJoinTree;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorLocateOperatorFactory;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorLookupSpec;
import org.apache.flink.table.runtime.operators.join.deltajoin.anchor.AnchorNotes;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.DropUnchangedUpdatesOperator;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOutputMode;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.RowTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapDataTypeFactory;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream exec node of a delta join that refreshes groups: every change marks the group of output
 * rows it may affect, and a refresh reads the group's current rows from the inputs with lookups and
 * replaces what it emitted before.
 *
 * <p>Groups of the join key of a join of two tables are refreshed by one two-input operator that
 * receives both inputs partitioned by the join key. Groups of the anchor key of a tree of joins are
 * refreshed by one operator that receives the anchor keys that locate operators, one per input,
 * found for every change, partitioned by the anchor key. Both operators share the same engine.
 */
@ExecNodeMetadata(
        name = "stream-exec-group-refresh-delta-join",
        version = 1,
        producedTransformations = {
            StreamExecGroupRefreshDeltaJoin.UNCHANGED_TRANSFORMATION,
            StreamExecGroupRefreshDeltaJoin.LOCATE_TRANSFORMATION,
            StreamExecGroupRefreshDeltaJoin.REFRESH_TRANSFORMATION
        },
        consumedOptions = {
            "table.exec.async-lookup.buffer-capacity",
            "table.exec.async-lookup.timeout",
            "table.exec.delta-join.cache-enabled",
            "table.exec.delta-join.left.cache-size",
            "table.exec.delta-join.right.cache-size",
            "table.exec.delta-join.group-refresh.output-cache-size",
            "table.exec.delta-join.group-refresh.max-pinned-rows",
            "table.exec.delta-join.group-refresh.max-group-size",
            "table.exec.delta-join.group-refresh.page-size"
        },
        minPlanVersion = FlinkVersion.v2_5,
        minStateVersion = FlinkVersion.v2_5)
public class StreamExecGroupRefreshDeltaJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    public static final String UNCHANGED_TRANSFORMATION =
            "group-refresh-delta-join-unchanged-updates";
    public static final String LOCATE_TRANSFORMATION = "group-refresh-delta-join-locate";
    public static final String REFRESH_TRANSFORMATION = "group-refresh-delta-join-refresh";

    public static final String FIELD_NAME_SPEC = "spec";
    public static final String FIELD_NAME_OUTPUT_MODE = "outputMode";
    public static final String FIELD_NAME_OUTPUT_GROUP_KEY = "outputGroupKey";
    public static final String FIELD_NAME_DECLARED_KEYS = "declaredKeys";
    public static final String FIELD_NAME_READ_BACK = "readBack";
    public static final String FIELD_NAME_DROP_UPDATE_BEFORE = "dropUpdateBefore";

    @JsonProperty(FIELD_NAME_SPEC)
    private final GroupRefreshDeltaJoinSpec spec;

    @JsonProperty(FIELD_NAME_OUTPUT_MODE)
    private final GroupRefreshOutputMode outputMode;

    /** The output field of every group key field; only set for the UPSERT mode. */
    @JsonProperty(FIELD_NAME_OUTPUT_GROUP_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable int[] outputGroupKey;

    /** The unique and upsert keys declared for the output; every emitted group keeps them. */
    @JsonProperty(FIELD_NAME_DECLARED_KEYS)
    private final int[][] declaredKeys;

    /** For the READ_BACK mode, the output table and how the joined rows become its rows. */
    @JsonProperty(FIELD_NAME_READ_BACK)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable DeltaJoinReadBackSpec readBack;

    /** Whether the consumer upserts by key, so that updates emit only the new row. */
    @JsonProperty(FIELD_NAME_DROP_UPDATE_BEFORE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean dropUpdateBefore;

    public StreamExecGroupRefreshDeltaJoin(
            ReadableConfig tableConfig,
            GroupRefreshDeltaJoinSpec spec,
            GroupRefreshOutputMode outputMode,
            @Nullable int[] outputGroupKey,
            int[][] declaredKeys,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            @Nullable DeltaJoinReadBackSpec readBack,
            boolean dropUpdateBefore) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupRefreshDeltaJoin.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecGroupRefreshDeltaJoin.class, tableConfig),
                spec,
                outputMode,
                outputGroupKey,
                declaredKeys,
                inputProperties,
                outputType,
                description,
                readBack,
                dropUpdateBefore);
    }

    @JsonCreator
    public StreamExecGroupRefreshDeltaJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SPEC) GroupRefreshDeltaJoinSpec spec,
            @JsonProperty(FIELD_NAME_OUTPUT_MODE) GroupRefreshOutputMode outputMode,
            @Nullable @JsonProperty(FIELD_NAME_OUTPUT_GROUP_KEY) int[] outputGroupKey,
            @JsonProperty(FIELD_NAME_DECLARED_KEYS) int[][] declaredKeys,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @Nullable @JsonProperty(FIELD_NAME_READ_BACK) DeltaJoinReadBackSpec readBack,
            @JsonProperty(FIELD_NAME_DROP_UPDATE_BEFORE) boolean dropUpdateBefore) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        Preconditions.checkArgument(
                (outputMode == GroupRefreshOutputMode.READ_BACK) == (readBack != null),
                "The READ_BACK mode, and only it, needs the output table.");
        Preconditions.checkArgument(
                inputProperties.size() == spec.getLeafTypes().size(),
                "Expected one input per table.");
        this.spec = checkNotNull(spec);
        this.outputMode = checkNotNull(outputMode);
        this.outputGroupKey = outputGroupKey;
        this.declaredKeys = checkNotNull(declaredKeys);
        this.readBack = readBack;
        this.dropUpdateBefore = dropUpdateBefore;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (spec.getGrouping() == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY) {
            RowType leftType = spec.getLeafTypes().get(0);
            RowType rightType = spec.getLeafTypes().get(1);
            int[] joinedTables = new int[leftType.getFieldCount() + rightType.getFieldCount()];
            int[] joinedFields = new int[joinedTables.length];
            for (int i = 0; i < joinedTables.length; i++) {
                boolean left = i < leftType.getFieldCount();
                joinedTables[i] = left ? 0 : 1;
                joinedFields[i] = left ? i : i - leftType.getFieldCount();
            }
            GroupRefreshDeltaJoinSpec.Edge edge = spec.getEdges().get(0);
            RowType joinKeyType = RowTypeUtils.projectRowType(leftType, edge.getLeftFields());
            // the fields that make up row keys: the unique keys and the join keys of both inputs
            ImmutableBitSet.Builder keyFields = ImmutableBitSet.builder();
            for (int field : edge.getLeftFields()) {
                keyFields.set(field);
            }
            for (int field : edge.getRightFields()) {
                keyFields.set(leftType.getFieldCount() + field);
            }
            for (int field : spec.getLeafKeys().get(0)) {
                keyFields.set(field);
            }
            for (int field : spec.getLeafKeys().get(1)) {
                keyFields.set(leftType.getFieldCount() + field);
            }
            return new JoinKeyGroupRefreshTranslator(
                            planner,
                            config,
                            spec,
                            refreshSpec(
                                    planner,
                                    config,
                                    joinedTables,
                                    joinedFields,
                                    joinKeyType,
                                    keyFields.build(),
                                    false),
                            (RowType) getOutputType())
                    .translate(
                            withoutUnchangedUpdates(
                                    planner, config, 0, edge.getLeftFields(), uniqueKeyOf(0)),
                            withoutUnchangedUpdates(
                                    planner, config, 1, edge.getRightFields(), uniqueKeyOf(1)),
                            createTransformationMeta(
                                    REFRESH_TRANSFORMATION,
                                    "GroupRefreshDeltaJoin",
                                    "GroupRefreshDeltaJoin(groups=[join key], mode=["
                                            + outputMode
                                            + "])",
                                    config));
        }
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        FlinkTypeFactory typeFactory = planner.getTypeFactory();

        // runtime lookups: 0 is the anchor lookup, then two per edge (right, left)
        List<AnchorLookupSpec> lookups = new ArrayList<>();
        RowType anchorType = spec.getLeafTypes().get(spec.getAnchorLeaf());
        RowType anchorKeyType = RowTypeUtils.projectRowType(anchorType, spec.getAnchorKey());
        lookups.add(
                lookup(
                        planner,
                        config,
                        typeFactory,
                        spec.getAnchorLookup(),
                        anchorKeyType,
                        anchorType,
                        "anchor #" + spec.getAnchorLeaf()));
        AnchorJoinTree.Edge[] edges = new AnchorJoinTree.Edge[spec.getEdges().size()];
        for (int e = 0; e < edges.length; e++) {
            GroupRefreshDeltaJoinSpec.Edge edge = spec.getEdges().get(e);
            RowType leftType = spec.getLeafTypes().get(edge.getLeftLeaf());
            RowType rightType = spec.getLeafTypes().get(edge.getRightLeaf());
            int lookupRight = lookups.size();
            lookups.add(
                    lookup(
                            planner,
                            config,
                            typeFactory,
                            edge.getLookupRight(),
                            leftType,
                            rightType,
                            "#" + edge.getLeftLeaf() + " -> #" + edge.getRightLeaf()));
            int lookupLeft = lookups.size();
            lookups.add(
                    lookup(
                            planner,
                            config,
                            typeFactory,
                            edge.getLookupLeft(),
                            rightType,
                            leftType,
                            "#" + edge.getRightLeaf() + " -> #" + edge.getLeftLeaf()));
            edges[e] =
                    new AnchorJoinTree.Edge(
                            edge.getLeftLeaf(),
                            edge.getLeftFields(),
                            edge.getRightLeaf(),
                            edge.getRightFields(),
                            lookupRight,
                            lookupLeft);
        }

        int numLeaves = spec.getLeafTypes().size();
        // tables whose rows of a whole group are read at once by the anchor key
        int[] groupLookups = new int[numLeaves];
        Arrays.fill(groupLookups, -1);
        for (GroupRefreshDeltaJoinSpec.GroupRead read : spec.getGroupReads()) {
            groupLookups[read.getLeaf()] = lookups.size();
            lookups.add(
                    lookup(
                            planner,
                            config,
                            typeFactory,
                            read.getLookup(),
                            anchorKeyType,
                            spec.getLeafTypes().get(read.getLeaf()),
                            "group of #" + read.getLeaf()));
        }
        List<GroupRefreshDeltaJoinSpec.Node> specNodes = spec.getNodes();
        AnchorJoinTree.Node[] nodes = new AnchorJoinTree.Node[specNodes.size()];
        GeneratedJoinCondition[] conditions = new GeneratedJoinCondition[specNodes.size()];
        GeneratedFilterCondition[][] filters = new GeneratedFilterCondition[specNodes.size()][];
        for (int n = 0; n < nodes.length; n++) {
            GroupRefreshDeltaJoinSpec.Node node = specNodes.get(n);
            List<GroupRefreshDeltaJoinSpec.Filter> nodeFilters = node.getFilters();
            filters[n] = new GeneratedFilterCondition[nodeFilters.size()];
            List<AnchorJoinTree.FieldMap> filterInputs = new ArrayList<>();
            for (int f = 0; f < nodeFilters.size(); f++) {
                GroupRefreshDeltaJoinSpec.Filter filter = nodeFilters.get(f);
                filters[n][f] =
                        FilterCodeGenerator.generateFilterCondition(
                                config, classLoader, filter.getCondition(), filter.getInputType());
                filterInputs.add(
                        new AnchorJoinTree.FieldMap(
                                filter.getInputLeaves(), filter.getInputFields()));
            }
            if (node.isLeaf()) {
                nodes[n] =
                        AnchorJoinTree.Node.leaf(
                                node.getLeaf(),
                                numLeaves,
                                spec.getLeafTypes().get(node.getLeaf()).getFieldCount());
                continue;
            }
            conditions[n] =
                    JoinUtil.generateConditionFunction(
                            config,
                            classLoader,
                            checkNotNull(node.getCondition()),
                            checkNotNull(node.getLeftType()),
                            checkNotNull(node.getRightType()));
            nodes[n] =
                    AnchorJoinTree.Node.join(
                            node.getLeft(),
                            node.getRight(),
                            nodes[node.getLeft()],
                            nodes[node.getRight()],
                            checkNotNull(node.getJoinType()),
                            node.getEdge(),
                            new AnchorJoinTree.FieldMap(
                                    node.getOutputLeaves(), node.getOutputFields()),
                            filterInputs);
        }

        AnchorJoinTree tree =
                new AnchorJoinTree(
                        spec.getLeafTypes().toArray(new RowType[0]),
                        nodes,
                        edges,
                        spec.getAnchorLeaf(),
                        spec.getAnchorKey(),
                        0,
                        groupLookups);
        AnchorLookupSpec[] lookupSpecs = lookups.toArray(new AnchorLookupSpec[0]);
        long timeoutMs =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT).toMillis();
        int capacity = config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY);

        // tables whose changes refresh only their own lines send the changed rows along: that
        // needs single rows of the output, read by row keys that every line knows by itself
        boolean rowsReadable =
                outputMode == GroupRefreshOutputMode.STATE
                        || (outputMode == GroupRefreshOutputMode.READ_BACK
                                && checkNotNull(readBack).isReadYourWrites());
        boolean rowKeysOfLines = outputMode == GroupRefreshOutputMode.READ_BACK;
        if (outputMode == GroupRefreshOutputMode.STATE) {
            rowKeysOfLines = true;
            for (int i = 0; i < numLeaves; i++) {
                rowKeysOfLines &= spec.getLeafKeys().get(i).length > 0;
            }
        }
        boolean[] withRows = new boolean[numLeaves];
        boolean anyWithRows = false;
        for (int i = 0; i < numLeaves; i++) {
            withRows[i] =
                    rowsReadable
                            && rowKeysOfLines
                            && tree.canRefreshRowsOf(i)
                            && spec.getLeafKeys().get(i).length > 0;
            anyWithRows |= withRows[i];
        }
        AnchorNotes notes = AnchorNotes.of(tree, withRows);

        // 1. locate the anchors of every change of every input
        List<Transformation<RowData>> located = new ArrayList<>();
        for (int i = 0; i < numLeaves; i++) {
            ExecEdge inputEdge = getInputEdges().get(i);
            Transformation<RowData> input = withoutUnchangedUpdates(planner, config, i);
            OneInputTransformation<RowData, RowData> locate =
                    ExecNodeUtil.createOneInputTransformation(
                            input,
                            createTransformationMeta(
                                    LOCATE_TRANSFORMATION + "-" + inputLetters(i),
                                    "GroupRefreshDeltaJoinLocate",
                                    "GroupRefreshDeltaJoinLocate(input=[#" + i + "])",
                                    config),
                            new AnchorLocateOperatorFactory(
                                    tree,
                                    lookupSpecs,
                                    i,
                                    (RowType) inputEdge.getOutputType(),
                                    timeoutMs,
                                    capacity,
                                    notes,
                                    withRows[i] ? spec.getLeafKeys().get(i) : new int[0],
                                    withRows[i] && joinsAnchorByEqualities(tree, i),
                                    pageSize(config)),
                            InternalTypeInfo.of(notes.getType()),
                            input.getParallelism(),
                            0,
                            false);
            located.add(locate);
        }

        // 2. bring all notes of an anchor key to one subtask
        RowDataKeySelector keySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, notes.getKeyFields(), InternalTypeInfo.of(notes.getType()));
        Transformation<RowData> union = new UnionTransformation<>(located);
        PartitionTransformation<RowData> exchange =
                new PartitionTransformation<>(
                        union,
                        new KeyGroupStreamPartitioner<>(
                                keySelector, DEFAULT_LOWER_BOUND_MAX_PARALLELISM));
        exchange.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

        // 3. refresh the groups of the anchor keys
        AnchorJoinTree.FieldMap joined = tree.getNode(tree.getRootNode()).getOutput();
        OneInputTransformation<RowData, RowData> refresh =
                ExecNodeUtil.createOneInputTransformation(
                        exchange,
                        createTransformationMeta(
                                REFRESH_TRANSFORMATION,
                                "GroupRefreshDeltaJoin",
                                "GroupRefreshDeltaJoin(groups=[anchor], mode=[" + outputMode + "])",
                                config),
                        new AnchorGroupRefreshOperatorFactory(
                                refreshSpec(
                                        planner,
                                        config,
                                        joined.getLeaves(),
                                        joined.getFields(),
                                        anchorKeyType,
                                        null,
                                        anyWithRows),
                                tree,
                                lookupSpecs,
                                conditions,
                                filters,
                                notes),
                        InternalTypeInfo.of(getOutputType()),
                        ExecutionConfig.PARALLELISM_DEFAULT,
                        0,
                        false);
        refresh.setStateKeySelector(keySelector);
        refresh.setStateKeyType(keySelector.getProducedType());
        return refresh;
    }

    /**
     * The common part of both groupings: how the joined rows, read through the given field map,
     * become emitted rows, and how the operator knows what it emitted before.
     */
    private GroupRefreshSpec refreshSpec(
            PlannerBase planner,
            ExecNodeConfig config,
            int[] joinedTables,
            int[] joinedFields,
            RowType groupKeyType,
            @Nullable ImmutableBitSet keyFields,
            boolean readRowsOfCompleteLines) {
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        RowType outputType = (RowType) getOutputType();
        RowType[] tableTypes = spec.getLeafTypes().toArray(new RowType[0]);
        int[][] tableKeys = new int[tableTypes.length][];
        for (int t = 0; t < tableKeys.length; t++) {
            tableKeys[t] = spec.getLeafKeys().get(t);
        }
        int capacity = config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY);
        GroupRefreshSpec.Builder builder =
                GroupRefreshSpec.builder(
                                tableTypes,
                                tableKeys,
                                joinedTables,
                                joinedFields,
                                outputType,
                                outputMode)
                        .declaredKeys(declaredKeys)
                        .groupKeyType(groupKeyType)
                        .capacity(capacity, 10 * capacity)
                        .maxCachedRows(
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE))
                        .maxPinnedRows(
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_MAX_PINNED_ROWS))
                        .lookupTimeoutMs(
                                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT)
                                        .toMillis())
                        .maxGroupSize(
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_MAX_GROUP_SIZE))
                        .pageSize(pageSize(config))
                        .emitUpdateBefore(!dropUpdateBefore);
        if (readBack != null) {
            builder.projection(
                            DeltaJoinReadBackUtil.generateOutputCalc(
                                    config,
                                    classLoader,
                                    readBack,
                                    outputType,
                                    planner.getTypeFactory()))
                    .readBack(
                            KeySelectorUtil.getRowDataSelector(
                                    classLoader,
                                    readBack.getOutputKey(),
                                    InternalTypeInfo.of(outputType)),
                            DeltaJoinReadBackUtil.generateOutputLookup(
                                    planner, config, readBack, groupKeyType))
                    .readYourWrites(readBack.isReadYourWrites());
            // single rows can be read back by row keys of complete lines, or of partial lines if
            // the primary key of the table is made of keys only
            List<RexNode> keyExpressions = DeltaJoinReadBackUtil.outputKeyExpressions(readBack);
            RowType outputKeyType =
                    RowTypeUtils.projectRowType(outputType, readBack.getOutputKey());
            if (readBack.isReadYourWrites()
                    && keyFields != null
                    && keyFields.contains(RelOptUtil.InputFinder.bits(keyExpressions, null))) {
                builder.readBackRows(
                        DeltaJoinReadBackUtil.generateOutputRowLookup(
                                planner, config, readBack, outputKeyType),
                        DeltaJoinReadBackUtil.generateOutputKeyCalc(
                                config,
                                classLoader,
                                readBack,
                                outputKeyType,
                                planner.getTypeFactory()));
            } else if (readBack.isReadYourWrites() && readRowsOfCompleteLines) {
                builder.readBackRows(
                        DeltaJoinReadBackUtil.generateOutputRowLookup(
                                planner, config, readBack, outputKeyType),
                        null);
            }
        }
        if (outputMode == GroupRefreshOutputMode.UPSERT) {
            builder.upsertByGroupKey(checkNotNull(outputGroupKey));
        }
        return builder.build();
    }

    /**
     * The input, without updates that change nothing the join reads; chained to the input, where
     * the two rows of an update are adjacent.
     */
    private Transformation<RowData> withoutUnchangedUpdates(
            PlannerBase planner, ExecNodeConfig config, int input) {
        return withoutUnchangedUpdates(planner, config, input, null, null);
    }

    /**
     * As {@link #withoutUnchangedUpdates(PlannerBase, ExecNodeConfig, int)}; with the join key and
     * a unique key of the input, an update that keeps both passes as its new row alone.
     */
    private Transformation<RowData> withoutUnchangedUpdates(
            PlannerBase planner,
            ExecNodeConfig config,
            int input,
            @Nullable int[] joinKey,
            @Nullable int[] uniqueKey) {
        ExecEdge edge = getInputEdges().get(input);
        Transformation<RowData> transformation =
                (Transformation<RowData>) edge.translateToPlan(planner);
        RowType type = (RowType) edge.getOutputType();
        return ExecNodeUtil.createOneInputTransformation(
                transformation,
                createTransformationMeta(
                        UNCHANGED_TRANSFORMATION + "-" + inputLetters(input),
                        "DropUnchangedUpdates",
                        "DropUnchangedUpdates(input=[#" + input + "])",
                        config),
                uniqueKey == null || uniqueKey.length == 0
                        ? new DropUnchangedUpdatesOperator(type)
                        : new DropUnchangedUpdatesOperator(type, joinKey, uniqueKey),
                InternalTypeInfo.of(type),
                transformation.getParallelism(),
                0,
                false);
    }

    /**
     * Whether the given table is joined towards the anchor on equalities of fields only: then every
     * version of a row with the same key has the same lines, and an update after alone is enough to
     * replace them.
     */
    private boolean joinsAnchorByEqualities(AnchorJoinTree tree, int leaf) {
        RexNode condition = spec.getNodes().get(tree.getJoinTowardsAnchor(leaf)).getCondition();
        if (condition == null) {
            return true;
        }
        for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
            if (!conjunct.isA(SqlKind.EQUALS) && !conjunct.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
                return false;
            }
            for (RexNode operand : ((RexCall) conjunct).getOperands()) {
                if (!(operand instanceof RexInputRef)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int pageSize(ExecNodeConfig config) {
        int pageSize =
                config.get(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE);
        if (pageSize <= 0) {
            throw new ValidationException(
                    String.format(
                            "'%s' must be positive, but is %d.",
                            ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE
                                    .key(),
                            pageSize));
        }
        return pageSize;
    }

    /** A unique key of the input, empty if it has none. */
    private int[] uniqueKeyOf(int input) {
        return spec.getLeafKeys().get(input);
    }

    /** Transformation names may only hold letters and dashes: input 0 is "a", 26 is "aa". */
    private static String inputLetters(int input) {
        StringBuilder letters = new StringBuilder();
        for (int i = input; i >= 0; i = i / 26 - 1) {
            letters.insert(0, (char) ('a' + i % 26));
        }
        return letters.toString();
    }

    /** Generates the fetcher and the calc of one lookup. */
    private static AnchorLookupSpec lookup(
            PlannerBase planner,
            ExecNodeConfig config,
            FlinkTypeFactory typeFactory,
            DeltaJoinSpec lookupSpec,
            RowType sourceRowType,
            RowType resultType,
            String description) {
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        TableSourceTable lookupTable =
                (TableSourceTable)
                        lookupSpec
                                .getLookupTable()
                                .getTemporalTable(planner.getFlinkContext(), typeFactory);
        Map<Integer, FunctionCallUtil.FunctionParam> lookupKeyMap = lookupSpec.getLookupKeyMap();
        AsyncTableFunction<?> lookupFunction =
                DeltaJoinUtil.getUnwrappedAsyncLookupFunction(
                        lookupTable, lookupKeyMap.keySet(), classLoader);
        UserDefinedFunctionHelper.prepareInstance(config, lookupFunction);

        RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(lookupTable.getRowType());
        RowType fetcherRowType =
                DeltaJoinUtil.combineOutputRowType(
                        sourceRowType, tableSourceRowType, FlinkJoinType.INNER, typeFactory);
        List<FunctionCallUtil.FunctionParam> keysOnSourceRow =
                Arrays.stream(LookupJoinUtil.getOrderedLookupKeys(lookupKeyMap.keySet()))
                        .mapToObj(lookupKeyMap::get)
                        .collect(Collectors.toList());
        int[] sourceKeyFields =
                keysOnSourceRow.stream()
                        .mapToInt(
                                key -> {
                                    Preconditions.checkState(
                                            key instanceof FunctionCallUtil.FieldRef,
                                            "The group refresh delta join only supports field references"
                                                    + " as lookup keys, but found %s",
                                            key);
                                    return ((FunctionCallUtil.FieldRef) key).index;
                                })
                        .toArray();

        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                fetcher =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                unwrapDataTypeFactory(planner.createRelBuilder()),
                                sourceRowType,
                                tableSourceRowType,
                                fetcherRowType,
                                keysOnSourceRow,
                                lookupFunction,
                                String.join(".", lookupTable.getQualifiedName()));

        GeneratedFunction<FlatMapFunction<RowData, RowData>> calc = null;
        if (lookupSpec.getProjectionOnTemporalTable().isPresent()) {
            List<RexNode> projection = lookupSpec.getProjectionOnTemporalTable().get();
            RexNode filter = lookupSpec.getFilterOnTemporalTable().orElse(null);
            calc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projection),
                            filter,
                            resultType,
                            tableSourceRowType,
                            typeFactory);
        }
        LogicalType[] sourceKeyTypes =
                Arrays.stream(sourceKeyFields)
                        .mapToObj(sourceRowType::getTypeAt)
                        .toArray(LogicalType[]::new);
        return new AnchorLookupSpec(
                fetcher.tableFunc(),
                fetcher.dataType(),
                calc,
                resultType,
                sourceKeyFields,
                sourceKeyTypes,
                description + " on " + String.join(".", lookupTable.getQualifiedName()),
                lookupFunction instanceof SupportsPagedLookup);
    }
}
