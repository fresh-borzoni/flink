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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Plan tests for the group refresh delta join. */
class AnchorDeltaJoinTest extends TableTestBase {

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
                                // these plans keep the rows in state
                                .set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                                        false)
                                .set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                                        OptimizerConfigOptions.DeltaJoinGrouping.ANCHOR));

        // a star and a chain: orders -> customers -> regions, orders -> products
        source(
                "orders",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("customer_id", DataTypes.INT())
                        .column("product_id", DataTypes.INT())
                        .column("country", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .primaryKey("order_id")
                        .index("order_id")
                        .index("customer_id")
                        .index("product_id")
                        .index("country")
                        .build(),
                ALL_CHANGES);
        source(
                "customers",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("region_id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("customer_id")
                        .index("customer_id")
                        .index("region_id")
                        .build(),
                ALL_CHANGES);
        source(
                "regions",
                Schema.newBuilder()
                        .column("region_id", DataTypes.INT().notNull())
                        .column("region", DataTypes.STRING())
                        .primaryKey("region_id")
                        .index("region_id")
                        .build(),
                ALL_CHANGES);
        source(
                "products",
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT().notNull())
                        .column("product", DataTypes.STRING())
                        .primaryKey("product_id")
                        .index("product_id")
                        .build(),
                ALL_CHANGES);
        // 1:N children of orders
        source(
                "items",
                Schema.newBuilder()
                        .column("item_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT())
                        .column("qty", DataTypes.INT())
                        .primaryKey("item_id")
                        .index("item_id")
                        .index("order_id")
                        .build(),
                ALL_CHANGES);
        // N:M partner of orders through country
        source(
                "shipments",
                Schema.newBuilder()
                        .column("shipment_id", DataTypes.INT().notNull())
                        .column("country", DataTypes.STRING())
                        .primaryKey("shipment_id")
                        .index("shipment_id")
                        .index("country")
                        .build(),
                ALL_CHANGES);
        // upsert sources: only UPDATE_AFTER and key-only deletes
        source(
                "customers_upsert",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("region_id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("customer_id")
                        .index("customer_id")
                        .build(),
                "I,UA,D",
                Map.of("source.produces-delete-by-key", "true"));
        source(
                "items_upsert",
                Schema.newBuilder()
                        .column("item_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT())
                        .column("qty", DataTypes.INT())
                        .primaryKey("item_id")
                        .index("item_id")
                        .index("order_id")
                        .build(),
                "I,UA,D",
                Map.of("source.produces-delete-by-key", "true"));

        sink(
                "order_sink_by_key",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("product", DataTypes.STRING())
                        .primaryKey("order_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
        sink(
                "order_sink",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("product", DataTypes.STRING())
                        .primaryKey("order_id")
                        .build(),
                Map.of());
        sink(
                "order_region_sink",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .primaryKey("order_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
        sink(
                "order_item_sink",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("item_id", DataTypes.INT().notNull())
                        .column("qty", DataTypes.INT())
                        .primaryKey("order_id", "item_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
        sink(
                "item_sink",
                Schema.newBuilder()
                        .column("item_id", DataTypes.INT().notNull())
                        .column("order_id", DataTypes.INT())
                        .primaryKey("item_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
        sink(
                "name_amount_log",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .build(),
                Map.of("sink-changelog-mode-enforced", ALL_CHANGES));
        sink(
                "order_item_log",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT())
                        .column("item_id", DataTypes.INT())
                        .column("qty", DataTypes.INT())
                        .build(),
                Map.of("sink-changelog-mode-enforced", ALL_CHANGES));
        sink(
                "order_shipment_sink",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("shipment_id", DataTypes.INT().notNull())
                        .primaryKey("order_id", "shipment_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
    }

    private void source(String name, Schema schema, String changelogMode) {
        source(name, schema, changelogMode, Map.of());
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

    private void sink(String name, Schema schema, Map<String, String> options) {
        TableDescriptor.Builder descriptor =
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("sink-insert-only", "false")
                        .option("sink-changelog-mode-enforced", "I,UA,D");
        options.forEach(descriptor::option);
        util.tableEnv().createTable(name, descriptor.build());
    }

    private void verifyPlan(String insert) {
        util.verifyRelPlanInsert(
                insert,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    private String explain(String insert) {
        return util.tableEnv().explainSql(insert, ExplainDetail.CHANGELOG_MODE);
    }

    @Test
    void testStarLeftJoinUpsertsByFactKey() {
        String sql =
                "INSERT INTO order_sink_by_key SELECT o.order_id, c.name, p.product FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " LEFT JOIN products p ON o.product_id = p.product_id";
        assertThat(explain(sql))
                .contains("GroupRefreshDeltaJoin(")
                .contains("groups=[anchor #0(order_id)]")
                .contains("mode=[UPSERT]")
                .doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testStarLeftJoinKeepsGroupsForFullDeleteSink() {
        String sql =
                "INSERT INTO order_sink SELECT o.order_id, c.name, p.product FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " LEFT JOIN products p ON o.product_id = p.product_id";
        assertThat(explain(sql)).contains("mode=[STATE]").doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testChainLeftJoin() {
        String sql =
                "INSERT INTO order_region_sink SELECT o.order_id, c.name, r.region FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " LEFT JOIN regions r ON c.region_id = r.region_id";
        assertThat(explain(sql))
                .contains("groups=[anchor #0(order_id)]")
                .contains("mode=[UPSERT]")
                .doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testOneToManyLeftJoinKeepsGroups() {
        String sql =
                "INSERT INTO order_item_log SELECT o.order_id, i.item_id, i.qty"
                        + " FROM orders o LEFT JOIN items i ON o.order_id = i.order_id";
        assertThat(explain(sql))
                .contains("groups=[anchor #0(order_id)]")
                .contains("mode=[STATE]")
                .doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testRightJoinAnchorsOnRightSide() {
        String sql =
                "INSERT INTO order_item_log SELECT o.order_id, i.item_id, i.qty"
                        + " FROM items i RIGHT JOIN orders o ON o.order_id = i.order_id";
        assertThat(explain(sql)).contains("groups=[anchor #1(order_id)]").contains("mode=[STATE]");
        verifyPlan(sql);
    }

    @Test
    void testInnerJoinPrefersAnchorWithSingleRowGroups() {
        String sql =
                "INSERT INTO order_sink_by_key SELECT o.order_id, c.name, CAST(NULL AS STRING)"
                        + " FROM customers c JOIN orders o ON o.customer_id = c.customer_id";
        assertThat(explain(sql)).contains("groups=[anchor #1(order_id)]").contains("mode=[UPSERT]");
    }

    @Test
    void testManyToManyJoinWithDeletes() {
        String sql =
                "INSERT INTO order_shipment_sink SELECT o.order_id, s.shipment_id FROM orders o"
                        + " JOIN shipments s ON o.country = s.country";
        assertThat(explain(sql)).contains("mode=[STATE]").doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testFullOuterJoinIsNotConverted() {
        String sql =
                "INSERT INTO order_item_log SELECT o.order_id, i.item_id, i.qty"
                        + " FROM orders o FULL JOIN items i ON o.order_id = i.order_id";
        assertThat(explain(sql)).doesNotContain("GroupRefreshDeltaJoin").contains("Join(joinType");
    }

    /** A tree without outer joins and without deletes stays with the incremental delta join. */
    @Test
    void testInsertOnlyInnerJoinKeepsTheIncrementalDeltaJoin() {
        source(
                "order_log",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("customer_id", DataTypes.INT().notNull())
                        .primaryKey("order_id")
                        .index("order_id")
                        .index("customer_id")
                        .build(),
                "I");
        source(
                "customer_log",
                Schema.newBuilder()
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .primaryKey("customer_id")
                        .index("customer_id")
                        .build(),
                "I");
        sink(
                "order_customer_sink",
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT().notNull())
                        .column("customer_id", DataTypes.INT().notNull())
                        .column("name", DataTypes.STRING())
                        .primaryKey("order_id", "customer_id")
                        .build(),
                Map.of("sink.supports-delete-by-key", "true"));
        String sql =
                "INSERT INTO order_customer_sink SELECT o.order_id, c.customer_id, c.name"
                        + " FROM order_log o JOIN customer_log c ON o.customer_id = c.customer_id"
                        + " ON CONFLICT DO DEDUPLICATE";
        assertThat(explain(sql)).contains("DeltaJoin(").doesNotContain("GroupRefreshDeltaJoin");
    }

    @Test
    void testDisabledByDefault() {
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED,
                        false);
        String sql =
                "INSERT INTO order_sink_by_key SELECT o.order_id, c.name, p.product FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " LEFT JOIN products p ON o.product_id = p.product_id"
                        + " ON CONFLICT DO DEDUPLICATE";
        assertThat(explain(sql)).doesNotContain("GroupRefreshDeltaJoin");
    }

    @Test
    void testInputWithoutOldImagesFallsBack() {
        // items rows can move to another order, and this source cannot say where they were
        String sql =
                "INSERT INTO order_item_log SELECT o.order_id, i.item_id, i.qty"
                        + " FROM orders o LEFT JOIN items_upsert i ON o.order_id = i.order_id";
        assertThat(explain(sql)).doesNotContain("GroupRefreshDeltaJoin");
    }

    @Test
    void testOneToManyInnerJoinAnchorsOnChild() {
        // anchored on the child, every group has one row and neither input needs before images:
        // the anchor needs none, and orders is reached through its primary key
        String sql =
                "INSERT INTO item_sink SELECT i.item_id, o.order_id"
                        + " FROM orders o JOIN items i ON o.order_id = i.order_id";
        assertThat(explain(sql)).contains("groups=[anchor #1(item_id)]").contains("mode=[UPSERT]");
        verifyPlan(sql);
    }

    @Test
    void testUpsertSourceWithoutOldImagesNeeded() {
        // customers are looked up by their primary key, so the normalizer that the planner adds
        // after the upsert source is not needed
        String sql =
                "INSERT INTO order_sink_by_key SELECT o.order_id, c.name, CAST(NULL AS STRING)"
                        + " FROM orders o LEFT JOIN customers_upsert c"
                        + " ON o.customer_id = c.customer_id";
        assertThat(explain(sql))
                .contains("GroupRefreshDeltaJoin(")
                .doesNotContain("ChangelogNormalize");
        verifyPlan(sql);
    }

    @Test
    void testAnchorKeyIsReadWhenTheQueryDoesNotUseIt() {
        // projection push-down would drop orders.order_id; the anchor needs it, so it is read
        String sql =
                "INSERT INTO name_amount_log SELECT c.name, o.amount FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id";
        assertThat(explain(sql))
                .contains("GroupRefreshDeltaJoin(")
                .contains("groups=[anchor #0(order_id)]")
                .contains("orders, project=[customer_id, amount, order_id]");
        verifyPlan(sql);
    }

    @Test
    void testUpsertOnlyByTheAnchorKey() {
        // keyed by (order, item), upserting by the item would keep the row of the old order when
        // the item moves, so the operator must retract it
        String sql =
                "INSERT INTO order_shipment_sink SELECT o.order_id, i.item_id"
                        + " FROM orders o JOIN items i ON o.order_id = i.order_id";
        assertThat(explain(sql)).contains("groups=[anchor #1(item_id)]").contains("mode=[STATE]");
    }

    @Test
    void testFilterOnNonKeyColumnRequestsBeforeImages() {
        String sql =
                "INSERT INTO order_sink_by_key SELECT o.order_id, c.name, CAST(NULL AS STRING)"
                        + " FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " WHERE o.amount > 10";
        assertThat(explain(sql)).contains("GroupRefreshDeltaJoin(");
        verifyPlan(sql);
    }

    @Test
    void testFilterBetweenJoins() {
        String sql =
                "INSERT INTO order_region_sink SELECT order_id, name, region FROM ("
                        + " SELECT o.order_id, o.customer_id, c.name, c.region_id FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " WHERE c.name IS NULL OR c.name <> 'x') t"
                        + " LEFT JOIN regions r ON t.region_id = r.region_id";
        assertThat(explain(sql)).contains("GroupRefreshDeltaJoin(").doesNotContain("Join(joinType");
        verifyPlan(sql);
    }

    @Test
    void testJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO order_region_sink SELECT o.order_id, c.name, r.region FROM orders o"
                        + " LEFT JOIN customers c ON o.customer_id = c.customer_id"
                        + " LEFT JOIN regions r ON c.region_id = r.region_id");
    }
}
