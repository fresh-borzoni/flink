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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.util.Map;

/** {@link TableTestProgram} definitions for testing {@link StreamExecGroupRefreshDeltaJoin}. */
public class GroupRefreshDeltaJoinTestPrograms {

    private static final Map<String, String> OPTIONS =
            Map.of("async", "true", "sink-insert-only", "false");

    /**
     * Orders left-joined with customers, grouped by the anchor key, the order id. Order 2 is
     * emitted with a padded customer before the restore and replaced when its customer arrives
     * after it.
     */
    public static final TableTestProgram ANCHOR_GROUPS_LEFT_JOIN =
            TableTestProgram.of(
                            "group-refresh-delta-join-anchor-groups",
                            "validates a group refresh delta join of a LEFT join grouped by an anchor")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                            true)
                    // these programs keep the rows in state
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                            false)
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                            OptimizerConfigOptions.DeltaJoinGrouping.ANCHOR)
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "o_id int not null",
                                            "o_cid int",
                                            "o_amount int",
                                            "primary key (o_id) not enforced")
                                    .addOptions(OPTIONS)
                                    .addIndex("o_id")
                                    .addIndex("o_cid")
                                    .producedBeforeRestore(Row.of(1, 10, 100), Row.of(2, 20, 200))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(Row.of(3, 10, 300))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("customers")
                                    .addSchema(
                                            "c_id int not null",
                                            "c_name string",
                                            "primary key (c_id) not enforced")
                                    .addOptions(OPTIONS)
                                    .addIndex("c_id")
                                    .producedBeforeRestore(Row.of(10, "a"))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(Row.of(20, "b"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema(
                                            "o_id int not null",
                                            "c_name string",
                                            "o_amount int",
                                            "primary key (o_id) not enforced")
                                    .addOptions(OPTIONS)
                                    .testMaterializedData()
                                    // the rows after the restore replace earlier ones by key
                                    .deduplicatedFieldIndices(new int[] {0})
                                    .consumedBeforeRestore(
                                            Row.of(1, "a", 100), Row.of(2, null, 200))
                                    .consumedAfterRestore(
                                            Row.of(1, "a", 100),
                                            Row.of(2, "b", 200),
                                            Row.of(3, "a", 300))
                                    .build())
                    .runSql(
                            "INSERT INTO snk SELECT o_id, c_name, o_amount FROM orders"
                                    + " LEFT JOIN customers ON o_cid = c_id")
                    .build();

    /**
     * Orders left-joined with customers, grouped by the join key and kept in state. The group of
     * customer 20 holds order 2 with a padded customer before the restore; when the customer
     * arrives after it, the restored group is replaced, which retracts the padded row.
     */
    public static final TableTestProgram JOIN_KEY_GROUPS_LEFT_JOIN =
            TableTestProgram.of(
                            "group-refresh-delta-join-join-key-groups",
                            "validates a group refresh delta join of a LEFT join grouped by the join"
                                    + " key, with groups restored from state")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                            true)
                    // these programs keep the rows in state
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                            false)
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                            OptimizerConfigOptions.DeltaJoinGrouping.JOIN_KEY)
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "o_id int not null",
                                            "o_cid int",
                                            "o_amount int",
                                            "primary key (o_id) not enforced")
                                    .addOptions(OPTIONS)
                                    .addIndex("o_id")
                                    .addIndex("o_cid")
                                    .producedBeforeRestore(Row.of(1, 10, 100), Row.of(2, 20, 200))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(Row.of(3, 10, 300))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("customers")
                                    .addSchema(
                                            "c_id int not null",
                                            "c_name string",
                                            "primary key (c_id) not enforced")
                                    .addOptions(OPTIONS)
                                    .addIndex("c_id")
                                    .producedBeforeRestore(Row.of(10, "a"))
                                    .treatDataBeforeRestoreAsConsumedData()
                                    .producedAfterRestore(Row.of(20, "b"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("snk")
                                    .addSchema("o_id int", "c_name string", "o_amount int")
                                    .addOptions(OPTIONS)
                                    .testMaterializedData()
                                    // the sink is restored with its rows from before the restore;
                                    // the expected rows are merged by order id
                                    .deduplicatedFieldIndices(new int[] {0})
                                    .consumedBeforeRestore(
                                            Row.of(1, "a", 100), Row.of(2, null, 200))
                                    .consumedAfterRestore(
                                            Row.of(1, "a", 100),
                                            Row.of(2, "b", 200),
                                            Row.of(3, "a", 300))
                                    .build())
                    .runSql(
                            "INSERT INTO snk SELECT o_id, c_name, o_amount FROM orders"
                                    + " LEFT JOIN customers ON o_cid = c_id")
                    .build();
}
