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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.DeltaJoinReadBackUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupLookup;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.JoinKeyGroupRefreshOperatorFactory;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.JoinKeyGroupSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapDataTypeFactory;

/**
 * Translates a delta join of two tables that refreshes the groups of its join key: both inputs are
 * partitioned by the join key, and a {@link JoinKeyGroupRefreshOperator} refreshes the rows of a
 * join key on every change of it.
 */
final class JoinKeyGroupRefreshTranslator {

    private final PlannerBase planner;
    private final ExecNodeConfig config;
    private final GroupRefreshDeltaJoinSpec spec;
    private final GroupRefreshSpec refreshSpec;
    private final RowType outputType;
    private final ClassLoader classLoader;
    private final FlinkTypeFactory typeFactory;

    JoinKeyGroupRefreshTranslator(
            PlannerBase planner,
            ExecNodeConfig config,
            GroupRefreshDeltaJoinSpec spec,
            GroupRefreshSpec refreshSpec,
            RowType outputType) {
        Preconditions.checkArgument(
                spec.getGrouping() == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY
                        && spec.getLeafTypes().size() == 2,
                "Join-key groups need a join of two tables.");
        this.planner = planner;
        this.config = config;
        this.spec = spec;
        this.refreshSpec = refreshSpec;
        this.outputType = outputType;
        this.classLoader = planner.getFlinkContext().getClassLoader();
        this.typeFactory = planner.getTypeFactory();
    }

    Transformation<RowData> translate(
            Transformation<RowData> leftInput,
            Transformation<RowData> rightInput,
            TransformationMetadata metadata) {
        RowType leftType = spec.getLeafTypes().get(0);
        RowType rightType = spec.getLeafTypes().get(1);
        GroupRefreshDeltaJoinSpec.Edge edge = spec.getEdges().get(0);
        GroupRefreshDeltaJoinSpec.Node root = spec.getNodes().get(spec.getNodes().size() - 1);
        FlinkJoinType joinType = Preconditions.checkNotNull(root.getJoinType());
        int[] leftJoinKeys = edge.getLeftFields();
        int[] rightJoinKeys = edge.getRightFields();
        int[] leftUniqueKey = uniqueKey(0);
        int[] rightUniqueKey = uniqueKey(1);

        RowDataKeySelector leftJoinKey = selector(leftJoinKeys, leftType);
        RowDataKeySelector rightJoinKey = selector(rightJoinKeys, rightType);

        // the left rows of a key are what the right input looks up, and vice versa
        GroupLookup leftLookup =
                groupLookup(edge.getLookupLeft(), rightJoinKeys, rightType, leftType, leftJoinKey);
        GroupLookup rightLookup =
                groupLookup(edge.getLookupRight(), leftJoinKeys, leftType, rightType, rightJoinKey);

        RowType joinedType =
                DeltaJoinUtil.combineOutputRowType(leftType, rightType, joinType, typeFactory);
        // the whole join condition; its equalities hold within a group anyway
        GeneratedFilterCondition condition =
                FilterCodeGenerator.generateFilterCondition(
                        config,
                        classLoader,
                        Preconditions.checkNotNull(root.getCondition()),
                        joinedType,
                        "JoinCondition");

        // the inputs are cached by their unique keys, so only if both have one
        boolean cache =
                config.get(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED)
                        && leftUniqueKey.length > 0
                        && rightUniqueKey.length > 0;
        // a parent: an input with at most one row per join key, as its unique key is in the join
        // key; changes of the other input, its children, then refresh only their own rows
        int scopedSide = -1;
        if (isParent(rightUniqueKey, rightJoinKeys) && leftUniqueKey.length > 0) {
            scopedSide = 0;
        } else if (isParent(leftUniqueKey, leftJoinKeys) && rightUniqueKey.length > 0) {
            scopedSide = 1;
        }
        JoinKeyGroupSpec groups =
                new JoinKeyGroupSpec(
                        joinType,
                        leftType,
                        rightType,
                        condition,
                        leftLookup,
                        rightLookup,
                        selector(leftUniqueKey, leftType),
                        selector(rightUniqueKey, rightType),
                        cache
                                ? config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_DELTA_JOIN_LEFT_CACHE_SIZE)
                                : 0,
                        cache
                                ? config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_DELTA_JOIN_RIGHT_CACHE_SIZE)
                                : 0,
                        scopedSide,
                        scopedSide == 1 ? rightUniqueKey : leftUniqueKey,
                        scopedSide == 1 ? rightJoinKeys : leftJoinKeys,
                        scopedSide == 1 ? leftJoinKeys : rightJoinKeys);

        TwoInputTransformation<RowData, RowData, RowData> transform =
                ExecNodeUtil.createTwoInputTransformation(
                        partitioned(leftInput, leftJoinKey),
                        partitioned(rightInput, rightJoinKey),
                        metadata,
                        new JoinKeyGroupRefreshOperatorFactory(refreshSpec, groups),
                        InternalTypeInfo.of(outputType),
                        ExecutionConfig.PARALLELISM_DEFAULT,
                        0,
                        false);
        transform.setStateKeySelectors(leftJoinKey, rightJoinKey);
        transform.setStateKeyType(leftJoinKey.getProducedType());
        return transform;
    }

    /** Whether the unique key is made of join key fields: one row per join key at most. */
    private static boolean isParent(int[] uniqueKey, int[] joinKeys) {
        if (uniqueKey.length == 0) {
            return false;
        }
        for (int field : uniqueKey) {
            if (Arrays.stream(joinKeys).noneMatch(joinKey -> joinKey == field)) {
                return false;
            }
        }
        return true;
    }

    private int[] uniqueKey(int table) {
        return spec.getLeafKeys().get(table);
    }

    private static Transformation<RowData> partitioned(
            Transformation<RowData> input, RowDataKeySelector key) {
        PartitionTransformation<RowData> exchange =
                new PartitionTransformation<>(
                        input,
                        new KeyGroupStreamPartitioner<>(key, DEFAULT_LOWER_BOUND_MAX_PARALLELISM));
        exchange.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
        return exchange;
    }

    private RowDataKeySelector selector(int[] fields, RowType type) {
        return KeySelectorUtil.getRowDataSelector(classLoader, fields, InternalTypeInfo.of(type));
    }

    /**
     * A lookup of the given table whose input is the join key, as a row of the join key fields of
     * the other table, instead of a row of the other table.
     */
    private GroupLookup groupLookup(
            DeltaJoinSpec lookup,
            int[] streamJoinKeys,
            RowType streamType,
            RowType resultType,
            RowDataKeySelector resultKey) {
        TableSourceTable lookupTable =
                (TableSourceTable)
                        lookup.getLookupTable()
                                .getTemporalTable(planner.getFlinkContext(), typeFactory);
        Map<Integer, FunctionCallUtil.FunctionParam> lookupKeyMap = lookup.getLookupKeyMap();
        AsyncTableFunction<?> asyncLookupFunction =
                DeltaJoinUtil.getUnwrappedAsyncLookupFunction(
                        lookupTable, lookupKeyMap.keySet(), classLoader);
        UserDefinedFunctionHelper.prepareInstance(config, asyncLookupFunction);

        RowType keyRowType =
                RowType.of(
                        Arrays.stream(streamJoinKeys)
                                .mapToObj(streamType::getTypeAt)
                                .toArray(LogicalType[]::new));
        List<FunctionCallUtil.FunctionParam> lookupKeysOnKeyRow =
                Arrays.stream(LookupJoinUtil.getOrderedLookupKeys(lookupKeyMap.keySet()))
                        .mapToObj(lookupKeyMap::get)
                        .map(key -> toKeyRowParam(key, streamJoinKeys))
                        .collect(Collectors.toList());

        RowType tableType = FlinkTypeFactory.toLogicalRowType(lookupTable.getRowType());
        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                fetcher =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                unwrapDataTypeFactory(planner.createRelBuilder()),
                                keyRowType,
                                tableType,
                                DeltaJoinUtil.combineOutputRowType(
                                        keyRowType, tableType, FlinkJoinType.INNER, typeFactory),
                                lookupKeysOnKeyRow,
                                asyncLookupFunction,
                                String.join(".", lookupTable.getQualifiedName()));

        GeneratedFunction<FlatMapFunction<RowData, RowData>> calc = null;
        if (lookup.getProjectionOnTemporalTable().isPresent()) {
            calc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(
                                    lookup.getProjectionOnTemporalTable().get()),
                            lookup.getFilterOnTemporalTable().orElse(null),
                            resultType,
                            tableType,
                            typeFactory);
        }
        return DeltaJoinReadBackUtil.groupLookup(
                fetcher,
                calc,
                new RowDataSerializer(resultType),
                resultKey,
                asyncLookupFunction,
                lookupKeysOnKeyRow,
                keyRowType);
    }

    /** Maps a lookup key on a row of the other table to the same key on the join key row. */
    private static FunctionCallUtil.FunctionParam toKeyRowParam(
            FunctionCallUtil.FunctionParam param, int[] streamJoinKeys) {
        if (!(param instanceof FunctionCallUtil.FieldRef)) {
            return param;
        }
        int streamIndex = ((FunctionCallUtil.FieldRef) param).index;
        for (int i = 0; i < streamJoinKeys.length; i++) {
            if (streamJoinKeys[i] == streamIndex) {
                return new FunctionCallUtil.FieldRef(i);
            }
        }
        throw new IllegalStateException(
                String.format(
                        "The lookup key on field %d is not a join key %s.",
                        streamIndex, Arrays.toString(streamJoinKeys)));
    }
}
