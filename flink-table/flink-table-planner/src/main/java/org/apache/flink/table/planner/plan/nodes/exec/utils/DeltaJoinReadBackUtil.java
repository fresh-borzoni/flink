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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinReadBackSpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupLookup;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapDataTypeFactory;

/** Generates what a join needs to write an output table and read it back. */
public final class DeltaJoinReadBackUtil {

    private DeltaJoinReadBackUtil() {}

    /** The projection from the joined row to rows of the output table. */
    public static GeneratedFunction<FlatMapFunction<RowData, RowData>> generateOutputCalc(
            ExecNodeConfig config,
            ClassLoader classLoader,
            DeltaJoinReadBackSpec readBack,
            RowType outputType,
            FlinkTypeFactory typeFactory) {
        return LookupJoinCodeGenerator.generateCalcMapFunction(
                config,
                classLoader,
                JavaScalaConversionUtil.toScala(readBack.getProjection()),
                readBack.getCondition(),
                outputType,
                readBack.getProjectionInput(),
                typeFactory);
    }

    /**
     * A lookup of the output table by its columns that hold the group key. Its input is the group
     * key row, and it returns the rows of the output table.
     */
    public static GroupLookup generateOutputLookup(
            PlannerBase planner,
            ExecNodeConfig config,
            DeltaJoinReadBackSpec readBack,
            RowType keyRowType) {
        return generateLookup(planner, config, readBack, readBack.getGroupColumns(), keyRowType);
    }

    /**
     * A lookup of the output table by its primary key. Its input is the key row, and it returns the
     * row of the output table with that key, if any.
     */
    public static GroupLookup generateOutputRowLookup(
            PlannerBase planner,
            ExecNodeConfig config,
            DeltaJoinReadBackSpec readBack,
            RowType keyRowType) {
        return generateLookup(planner, config, readBack, readBack.getOutputKey(), keyRowType);
    }

    /**
     * The primary key of the output table from a joined row: the projection of only its key
     * columns, which must not read other fields of the joined row.
     */
    public static GeneratedFunction<FlatMapFunction<RowData, RowData>> generateOutputKeyCalc(
            ExecNodeConfig config,
            ClassLoader classLoader,
            DeltaJoinReadBackSpec readBack,
            RowType keyRowType,
            FlinkTypeFactory typeFactory) {
        return LookupJoinCodeGenerator.generateCalcMapFunction(
                config,
                classLoader,
                JavaScalaConversionUtil.toScala(outputKeyExpressions(readBack)),
                null,
                keyRowType,
                readBack.getProjectionInput(),
                typeFactory);
    }

    /** The expressions of the primary key columns of the output table, on the joined row. */
    public static List<RexNode> outputKeyExpressions(DeltaJoinReadBackSpec readBack) {
        List<RexNode> keys = new ArrayList<>();
        for (int column : readBack.getOutputKey()) {
            keys.add(readBack.getProjection().get(column));
        }
        return keys;
    }

    private static GroupLookup generateLookup(
            PlannerBase planner,
            ExecNodeConfig config,
            DeltaJoinReadBackSpec readBack,
            int[] groupColumns,
            RowType keyRowType) {
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        FlinkTypeFactory typeFactory = planner.getTypeFactory();
        TableSourceTable outputTable =
                (TableSourceTable)
                        readBack.getOutputTable()
                                .getTemporalTable(planner.getFlinkContext(), typeFactory);
        Set<Integer> lookupColumns =
                Arrays.stream(groupColumns).boxed().collect(Collectors.toSet());
        AsyncTableFunction<?> asyncLookupFunction =
                DeltaJoinUtil.getUnwrappedAsyncLookupFunction(
                        outputTable, lookupColumns, classLoader, readBack.isReadYourWrites());
        UserDefinedFunctionHelper.prepareInstance(config, asyncLookupFunction);

        List<FunctionCallUtil.FunctionParam> lookupKeysOnKeyRow = new ArrayList<>();
        for (int column : LookupJoinUtil.getOrderedLookupKeys(lookupColumns)) {
            int position = 0;
            while (groupColumns[position] != column) {
                position++;
            }
            lookupKeysOnKeyRow.add(new FunctionCallUtil.FieldRef(position));
        }

        RowType outputTableRowType = FlinkTypeFactory.toLogicalRowType(outputTable.getRowType());
        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                fetcher =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                unwrapDataTypeFactory(planner.createRelBuilder()),
                                keyRowType,
                                outputTableRowType,
                                DeltaJoinUtil.combineOutputRowType(
                                        keyRowType,
                                        outputTableRowType,
                                        FlinkJoinType.INNER,
                                        typeFactory),
                                lookupKeysOnKeyRow,
                                asyncLookupFunction,
                                String.join(".", outputTable.getQualifiedName()));
        return groupLookup(
                fetcher,
                null,
                new RowDataSerializer(outputTableRowType),
                KeySelectorUtil.getRowDataSelector(
                        classLoader, groupColumns, InternalTypeInfo.of(outputTableRowType)),
                asyncLookupFunction,
                lookupKeysOnKeyRow,
                keyRowType);
    }

    /**
     * A lookup of the rows of a group key; it reads in pages if the lookup function supports it.
     *
     * @param lookupKeysOnKeyRow the lookup keys, as fields of the key row, in the order of the
     *     lookup function
     */
    public static GroupLookup groupLookup(
            FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<
                            AsyncFunction<RowData, Object>>
                    fetcher,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> calc,
            RowDataSerializer resultSerializer,
            RowDataKeySelector resultKey,
            AsyncTableFunction<?> lookupFunction,
            List<FunctionCallUtil.FunctionParam> lookupKeysOnKeyRow,
            RowType keyRowType) {
        if (!(lookupFunction instanceof SupportsPagedLookup)) {
            return new GroupLookup(
                    fetcher.tableFunc(), fetcher.dataType(), calc, resultSerializer, resultKey);
        }
        int[] keyFields = new int[lookupKeysOnKeyRow.size()];
        LogicalType[] keyTypes = new LogicalType[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            keyFields[i] = ((FunctionCallUtil.FieldRef) lookupKeysOnKeyRow.get(i)).index;
            keyTypes[i] = keyRowType.getTypeAt(keyFields[i]);
        }
        return new GroupLookup(
                fetcher.tableFunc(),
                fetcher.dataType(),
                calc,
                resultSerializer,
                resultKey,
                keyFields,
                keyTypes);
    }
}
