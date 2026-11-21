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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.SharedStoreTableFactory;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan tests for delta joins that refresh groups: joins of two tables refresh the groups of their
 * join key, and joins that write a table they can read back keep no state for their output.
 */
class GroupRefreshDeltaJoinTest extends TableTestBase {

    private static final String ALL_CHANGES = "I,UB,UA,D";

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util =
                streamTestUtil(
                        TableConfig.getDefault()
                                .set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                                        true)
                                .set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                                        OptimizerConfigOptions.DeltaJoinGrouping.JOIN_KEY)
                                // the tests that read back turn it on
                                .set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                                        false));
        util.tableEnv().getConfig().set("execution.checkpointing.interval", "10 s");

        // orders reference customers; items are keyed by order and item, like a child table
        source(
                "orders",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("customer_id", DataTypes.INT())
                        .column("country", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .primaryKey("order_id")
                        .index("order_id")
                        .index("customer_id")
                        .index("country")
                        .build(),
                ALL_CHANGES,
                Map.of());
        source(
                "customers",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .primaryKey("customer_id")
                        .index("customer_id")
                        .build(),
                ALL_CHANGES,
                Map.of());
        source(
                "shipments",
                Schema.newBuilder()
                        .column("shipment_id", DataTypes.INT().notNull())
                        .column("country", DataTypes.STRING())
                        .primaryKey("shipment_id")
                        .index("shipment_id")
                        .index("country")
                        .build(),
                ALL_CHANGES,
                Map.of());
        // upsert sources: only UPDATE_AFTER and key-only deletes
        source(
                "items_upsert",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("item_id", DataTypes.INT().notNull())
                        .column("qty", DataTypes.INT())
                        .primaryKey("order_id", "item_id")
                        .index("order_id")
                        .build(),
                "I,UA,D",
                Map.of("source.produces-delete-by-key", "true"));
        source(
                "orders_upsert",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("amount", DataTypes.INT())
                        .primaryKey("order_id")
                        .index("order_id")
                        .build(),
                "I,UA,D",
                Map.of("source.produces-delete-by-key", "true"));

        // sinks that cannot be read back
        util.tableEnv()
                .createTable(
                        "order_customers",
                        TableDescriptor.forConnector("values")
                                .schema(
                                        Schema.newBuilder()
                                                .column("order_id", DataTypes.INT().notNull())
                                                .column("customer_id", DataTypes.INT())
                                                .column("name", DataTypes.STRING())
                                                .build())
                                .option("sink-insert-only", "false")
                                .build());
        util.tableEnv()
                .createTable(
                        "order_shipments",
                        TableDescriptor.forConnector("values")
                                .schema(
                                        Schema.newBuilder()
                                                .column("country", DataTypes.STRING())
                                                .column("order_id", DataTypes.INT())
                                                .column("shipment_id", DataTypes.INT())
                                                .build())
                                .option("sink-insert-only", "false")
                                .build());

        // sinks that can be read back: keyed by the join key and the keys of the joined rows
        readBackSink(
                "customer_orders",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .primaryKey("customer_id", "order_id")
                        .index("customer_id")
                        .build());
        readBackSink(
                "order_items",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("item_id", DataTypes.INT().notNull())
                        .column("amount", DataTypes.INT())
                        .column("qty", DataTypes.INT())
                        .primaryKey("order_id", "item_id")
                        .index("order_id")
                        .build());
        // the same table, but its sink makes rows readable only once a checkpoint completed
        readBackSink(
                "customer_orders_checkpointed",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .primaryKey("customer_id", "order_id")
                        .index("customer_id")
                        .build(),
                false);
        readBackSink(
                "country_shipments",
                Schema.newBuilder()
                        .column("country", DataTypes.STRING().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("shipment_id", DataTypes.INT().notNull())
                        .column("amount", DataTypes.INT())
                        .primaryKey("country", "order_id", "shipment_id")
                        .index("country")
                        .build());
    }

    private void source(
            String name, Schema schema, String changelogMode, Map<String, String> options) {
        TableDescriptor.Builder descriptor =
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("changelog-mode", changelogMode)
                        .option("async", "true")
                        .option("bounded", "false");
        options.forEach(descriptor::option);
        util.tableEnv().createTable(name, descriptor.build());
    }

    private void readBackSink(String name, Schema schema) {
        readBackSink(name, schema, true);
    }

    private void readBackSink(String name, Schema schema, boolean readYourWrites) {
        util.tableEnv()
                .createTable(
                        name,
                        TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                                .schema(schema)
                                .option(SharedStoreTableFactory.STORE, "plan-test")
                                .option(
                                        SharedStoreTableFactory.SINK_READ_YOUR_WRITES,
                                        readYourWrites)
                                .build());
    }

    private void verifyPlan(String insert) {
        util.verifyRelPlanInsert(
                insert,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    private String explain(String insert) {
        return util.tableEnv().explainSql(insert);
    }

    private void readBack() {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        true);
    }

    /** Without read-back, a join that would keep its rows in state fails, naming the reason. */
    private void assertReadBackRefused(String insert, String reason) {
        assertThatThrownBy(() -> explain(insert))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "cannot read the rows it emitted back (" + reason));
    }

    @Test
    void testLeftJoinRefreshesJoinKeyGroups() {
        String sql =
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explain(sql))
                .contains("GroupRefreshDeltaJoin(")
                .contains("groups=[join key]")
                .doesNotContain("readBack=[");
        verifyPlan(sql);
    }

    @Test
    void testFullJoinRefreshesJoinKeyGroups() {
        String sql =
                "INSERT INTO order_shipments SELECT COALESCE(o.country, s.country), o.order_id,"
                        + " s.shipment_id FROM orders o FULL JOIN shipments s"
                        + " ON o.country = s.country";
        assertThat(explain(sql)).contains("GroupRefreshDeltaJoin(").contains("groups=[join key]");
        verifyPlan(sql);
    }

    /** Rows joined on their own key never move to another group: no old images are needed. */
    @Test
    void testJoinOnKeysOfUpsertSources() {
        readBack();
        String sql =
                "INSERT INTO order_items SELECT i.order_id, i.item_id, o.amount, i.qty"
                        + " FROM items_upsert i LEFT JOIN orders_upsert o"
                        + " ON i.order_id = o.order_id";
        assertThat(explain(sql))
                .contains("groups=[join key]")
                .contains("readBack=[")
                .doesNotContain("ChangelogNormalize");
        verifyPlan(sql);
    }

    /** The projection moves into the join, and the sink keeps no state. */
    @Test
    void testReadBackFromOutputTable() {
        readBack();
        String sql =
                "INSERT INTO customer_orders SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id";
        String plan = explain(sql);
        assertThat(plan)
                .contains("groups=[join key]")
                .contains("readBack=[default_catalog.default_database.customer_orders]")
                .contains("readableAfter=[write]")
                .doesNotContain("upsertMaterialize=[true]");
        verifyPlan(sql);
    }

    /**
     * A sink that makes rows readable only once a checkpoint completed: the join keeps the groups
     * it wrote in memory until then.
     */
    @Test
    void testReadBackAfterCheckpoint() {
        readBack();
        String sql =
                "INSERT INTO customer_orders_checkpointed SELECT c.customer_id, o.order_id, c.name,"
                        + " o.amount FROM customers c JOIN orders o"
                        + " ON c.customer_id = o.customer_id";
        assertThat(explain(sql))
                .contains(
                        "readBack=[default_catalog.default_database.customer_orders_checkpointed]")
                .contains("readableAfter=[checkpoint]");
    }

    /** NULL join keys and padded keys are written under constants. */
    @Test
    void testReadBackOfFullJoinWithConstantsForNulls() {
        readBack();
        String sql =
                "INSERT INTO country_shipments SELECT COALESCE(o.country, s.country, '?'),"
                        + " COALESCE(o.order_id, -1), COALESCE(s.shipment_id, -1), o.amount"
                        + " FROM orders o FULL JOIN shipments s ON o.country = s.country";
        assertThat(explain(sql))
                .contains("readBack=[default_catalog.default_database.country_shipments]")
                .doesNotContain("upsertMaterialize=[true]");
        verifyPlan(sql);
    }

    @Test
    void testNoReadBackForSinkThatDoesNotDeclareIt() {
        readBack();
        String sql =
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertReadBackRefused(
                sql,
                "default_catalog.default_database.order_customers: the sink does not declare "
                        + "that its rows can be read back");
    }

    /** Unaligned checkpoints prove nothing about the sink: only reading your writes is left. */
    @Test
    void testUnalignedCheckpointsNeedReadYourWrites() {
        readBack();
        util.tableEnv().getConfig().set("execution.checkpointing.unaligned.enabled", "true");
        String query =
                " SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id";
        assertReadBackRefused(
                "INSERT INTO customer_orders_checkpointed" + query,
                "default_catalog.default_database.customer_orders_checkpointed: unaligned "
                        + "checkpoints do not prove that the sink received the rows");
        assertThat(explain("INSERT INTO customer_orders" + query))
                .contains("readableAfter=[write]");
    }

    /** Reading your writes needs no checkpoints at all. */
    @Test
    void testReadYourWritesWithoutCheckpoints() {
        readBack();
        util.tableEnv()
                .getConfig()
                .getConfiguration()
                .removeConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL);
        String query =
                " SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id";
        assertReadBackRefused(
                "INSERT INTO customer_orders_checkpointed" + query,
                "default_catalog.default_database.customer_orders_checkpointed: checkpoints are "
                        + "disabled, so the rows it writes never become readable");
        assertThat(explain("INSERT INTO customer_orders" + query))
                .contains("readableAfter=[write]");
    }

    /** A forced keyed shuffle or upsert materializer would move the sink out of the join's task. */
    @Test
    void testNoReadYourWritesWhenSinkInputIsShuffled() {
        readBack();
        String sql =
                "INSERT INTO customer_orders SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id";
        TableConfig config = util.tableEnv().getConfig();
        config.set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
                ExecutionConfigOptions.SinkKeyedShuffle.FORCE);
        assertThat(explain(sql)).doesNotContain("readableAfter=[write]");

        config.set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
                ExecutionConfigOptions.SinkKeyedShuffle.AUTO);
        config.set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                ExecutionConfigOptions.UpsertMaterialize.FORCE);
        assertThat(explain(sql)).doesNotContain("readableAfter=[write]");
    }

    @Test
    void testNoReadBackWhenDisabled() {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        false);
        String sql =
                "INSERT INTO customer_orders SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id";
        assertThat(explain(sql)).contains("groups=[join key]").doesNotContain("readBack=[");
    }

    /** Without group refreshes, the joins stay regular joins. */
    @Test
    void testDisabledKeepsRegularJoin() {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                        false);
        String sql =
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explain(sql)).doesNotContain("GroupRefreshDeltaJoin(").contains("Join(");
    }

    /** The hint enables group refreshes for the joins of its query block only. */
    @Test
    void testHintEnablesGroupRefreshForItsQueryBlock() {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                        false);
        String hinted =
                "INSERT INTO order_customers SELECT /*+ GROUP_REFRESH */"
                        + " o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explain(hinted)).contains("GroupRefreshDeltaJoin(");
        String other =
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name FROM"
                        + " (SELECT /*+ GROUP_REFRESH */ order_id, customer_id FROM orders) o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explain(other))
                .as("the hint of another query block")
                .doesNotContain("GroupRefreshDeltaJoin(");
    }

    @Test
    void testHintTakesNoOptions() {
        String sql =
                "INSERT INTO order_customers SELECT /*+ GROUP_REFRESH('o') */"
                        + " o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThatThrownBy(() -> explain(sql)).hasMessageContaining("takes no options");
    }

    @Test
    void testJsonPlanOfJoinKeyGroups() {
        util.verifyJsonPlan(
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id");
    }

    /** The plan carries the output table, the projection and the key to read the groups back. */
    @Test
    void testJsonPlanOfReadBack() {
        readBack();
        util.verifyJsonPlan(
                "INSERT INTO customer_orders SELECT c.customer_id, o.order_id, c.name, o.amount"
                        + " FROM customers c JOIN orders o ON c.customer_id = o.customer_id");
    }

    // ------------------------------------------------------------------------------- grouping

    private String explainAuto(String sql) {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.AUTO);
        return explain(sql);
    }

    /** A change of an order re-reads its own row, not every order of its customer. */
    @Test
    void testAutoGroupsByAnchor() {
        String sql =
                "INSERT INTO order_customers SELECT o.order_id, o.customer_id, c.name"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explainAuto(sql)).contains("groups=[anchor #0(order_id)]");
    }

    @Test
    void testAutoGroupsFullJoinByJoinKey() {
        String sql =
                "INSERT INTO order_shipments SELECT COALESCE(o.country, s.country), o.order_id,"
                        + " s.shipment_id FROM orders o FULL JOIN shipments s"
                        + " ON o.country = s.country";
        assertThat(explainAuto(sql)).contains("groups=[join key]");
    }

    /**
     * Orders are the parents of items. Grouped by the join key, a changed item refreshes only its
     * own row, and a changed order reads its items at once instead of refreshing them one by one.
     */
    @Test
    void testAutoGroupsParentJoinsByJoinKey() {
        readBack();
        String sql =
                "INSERT INTO order_items SELECT i.order_id, i.item_id, o.amount, i.qty"
                        + " FROM items_upsert i LEFT JOIN orders_upsert o"
                        + " ON i.order_id = o.order_id";
        assertThat(explainAuto(sql))
                .contains("groups=[join key]")
                .contains("readBack=[default_catalog.default_database.order_items]");
    }

    /**
     * The output table cannot be read back, and is keyed by the anchor key: every group has one
     * row, which the join upserts by its key without memory.
     */
    @Test
    void testAutoKeepsAnchorThatKeysTheOutput() {
        util.tableEnv()
                .createTable(
                        "order_items_upserted",
                        TableDescriptor.forConnector("values")
                                .schema(
                                        Schema.newBuilder()
                                                .column("order_id", DataTypes.INT().notNull())
                                                .column("item_id", DataTypes.INT().notNull())
                                                .column("amount", DataTypes.INT())
                                                .column("qty", DataTypes.INT())
                                                .primaryKey("order_id", "item_id")
                                                .build())
                                .option("sink-insert-only", "false")
                                .build());
        String sql =
                "INSERT INTO order_items_upserted SELECT i.order_id, i.item_id, o.amount, i.qty"
                        + " FROM items_upsert i LEFT JOIN orders_upsert o"
                        + " ON i.order_id = o.order_id";
        assertThat(explainAuto(sql)).contains("groups=[anchor #0(").doesNotContain("readBack=[");
    }

    /**
     * Shipments are keyed by the country and the order, and can be read back by the country only:
     * the join groups by the join key, by which it can read its rows back.
     */
    @Test
    void testAutoGroupsByJoinKeyThatKeysTheOutput() {
        readBack();
        String sql =
                "INSERT INTO country_shipments SELECT o.country, o.order_id,"
                        + " COALESCE(s.shipment_id, -1), o.amount"
                        + " FROM orders o LEFT JOIN shipments s ON o.country = s.country";
        assertThat(explainAuto(sql))
                .contains("groups=[join key]")
                .contains("readBack=[default_catalog.default_database.country_shipments]");
    }

    /**
     * Every table of a chain is keyed by the keys of its parent and its own id, and joins its
     * parent on the parent's key. The output key holds the id of every table once: a row of a child
     * exists only where its parent ids equal the parent's, which the output key holds.
     */
    @Test
    void testReadBackOfChainKeyedByParentKeys() {
        readBack();
        chainTables();
        readBackSink(
                "customer_lines",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("line_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("qty", DataTypes.INT())
                        .primaryKey("customer_id", "order_id", "line_id")
                        .index("customer_id")
                        .build());
        String sql =
                "INSERT INTO customer_lines SELECT c.customer_id, COALESCE(o.order_id, -1),"
                        + " COALESCE(l.line_id, -1), c.name, o.amount, l.qty"
                        + " FROM customers c"
                        + " LEFT JOIN chain_orders o ON o.customer_id = c.customer_id"
                        + " LEFT JOIN chain_lines l"
                        + " ON l.customer_id = o.customer_id AND l.order_id = o.order_id";
        assertThat(explainAuto(sql))
                .contains("groups=[anchor #0(customer_id)]")
                .contains("readBack=[default_catalog.default_database.customer_lines]")
                // lines hold the customer id and can be looked up by it alone: read per group
                .contains("readsGroupsOf=[#2]");
    }

    /** Without the order id in the output key, lines of two orders are not told apart. */
    @Test
    void testNoReadBackOfChainWithoutParentKey() {
        readBack();
        chainTables();
        readBackSink(
                "customer_lines_by_line",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("line_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("qty", DataTypes.INT())
                        .primaryKey("customer_id", "line_id")
                        .index("customer_id")
                        .build());
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.AUTO);
        assertReadBackRefused(
                "INSERT INTO customer_lines_by_line SELECT c.customer_id,"
                        + " COALESCE(l.line_id, -1), c.name, l.qty"
                        + " FROM customers c"
                        + " LEFT JOIN chain_orders o ON o.customer_id = c.customer_id"
                        + " LEFT JOIN chain_lines l"
                        + " ON l.customer_id = o.customer_id AND l.order_id = o.order_id"
                        + " ON CONFLICT DO DEDUPLICATE",
                "default_catalog.default_database.customer_lines_by_line: the primary key does"
                        + " not hold the unique key of table #1");
    }

    /**
     * Every group has at most one row. An output table keyed by the anchor key that deletes by key
     * takes upserts by the anchor key, also if it could be read back; one that needs the whole old
     * row of a delete gets it by reading the row back.
     */
    @Test
    void testSingleRowGroupsReadBackOnlyIfTheSinkNeedsOldRows() {
        readBack();
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.AUTO);
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("customer_id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("order_id")
                        .index("order_id")
                        .build();
        util.tableEnv()
                .createTable(
                        "order_names",
                        TableDescriptor.forConnector("values")
                                .schema(schema)
                                .option("sink-insert-only", "false")
                                .option("sink.supports-delete-by-key", "true")
                                .build());
        for (boolean fullDeletes : new boolean[] {false, true}) {
            util.tableEnv()
                    .createTable(
                            fullDeletes ? "order_names_full_deletes" : "order_names_readable",
                            TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                                    .schema(schema)
                                    .option(SharedStoreTableFactory.STORE, "plan-test")
                                    .option(SharedStoreTableFactory.SINK_FULL_DELETES, fullDeletes)
                                    .build());
        }
        String query =
                " SELECT o.order_id, o.customer_id, c.name FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id";
        for (String sink : new String[] {"order_names", "order_names_readable"}) {
            assertThat(explain("INSERT INTO " + sink + query))
                    .contains("groups=[anchor #0(order_id)]")
                    .contains("mode=[UPSERT]")
                    .doesNotContain("readBack=[");
        }
        assertThat(explain("INSERT INTO order_names_full_deletes" + query))
                .contains("groups=[anchor #0(order_id)]")
                .contains("readBack=[default_catalog.default_database.order_names_full_deletes]");
    }

    /** Orders keyed by their customer and lines keyed by their customer and order. */
    private void chainTables() {
        source(
                "chain_orders",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("amount", DataTypes.INT())
                        .primaryKey("customer_id", "order_id")
                        .index("customer_id")
                        .build(),
                ALL_CHANGES,
                Map.of());
        source(
                "chain_lines",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT().notNull())
                        .column("line_id", DataTypes.INT().notNull())
                        .column("qty", DataTypes.INT())
                        .primaryKey("customer_id", "order_id", "line_id")
                        .index("customer_id", "order_id")
                        .index("customer_id")
                        .build(),
                ALL_CHANGES,
                Map.of());
    }
}
