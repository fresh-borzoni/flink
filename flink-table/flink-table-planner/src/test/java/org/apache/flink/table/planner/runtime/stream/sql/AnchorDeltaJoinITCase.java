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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.factories.SharedStoreTableFactory;
import org.apache.flink.table.planner.factories.SharedStoreTableFactory.Change;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamAbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Randomized end-to-end tests of the group refresh delta join against tables that change while the
 * job runs.
 *
 * <p>Every run generates three tables with random scripts of inserts, updates (including updates of
 * join keys) and deletes, and a random tree of two INNER/LEFT/RIGHT joins: a star or a chain, with
 * N:M, 1:N or N:1 edges. The scripts run in the {@link SharedStoreTableFactory} store, whose
 * sources emit a change only after applying it and whose lookups read the current state after a
 * random delay, so changes of different tables interleave freely with the lookups. When the job has
 * finished, the sink must equal the join over the final state of the tables, computed by a batch
 * job.
 *
 * <p>The number of runs per test can be raised with the system property {@code
 * anchor.delta.join.runs}.
 */
class AnchorDeltaJoinITCase extends StreamAbstractTestBase {

    private static final int RUNS =
            Integer.parseInt(System.getProperties().getProperty("anchor.delta.join.runs", "12"));
    private static final String STORE = "anchor-delta-join-it";
    private static final String[] JOIN_TYPES = {"JOIN", "LEFT JOIN", "RIGHT JOIN"};

    private StreamTableEnvironment tEnv;
    private int failAfter = -1;
    private boolean restoredFromCheckpoint;
    // write into an upsert sink in the store, which is not rolled back on a restore
    private boolean sinkInStore;
    private @Nullable String expectedMode;
    // a part that the optimized plan must contain
    private @Nullable String expectedInPlan;

    @BeforeEach
    void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        false)
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED, true)
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.ANCHOR);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY, 8);
    }

    @AfterEach
    void cleanup() {
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
    }

    /** N:M edges on mutable keys, every combination of join types, into a retract sink. */
    @Test
    void testManyToManyTreesConverge() throws Exception {
        for (int run = 0; run < Math.max(RUNS, 18); run++) {
            Random random = new Random(1000 + run);
            // every combination of join types, for a star (a) and a chain (b)
            String j1 = JOIN_TYPES[run % 3];
            String j2 = JOIN_TYPES[(run / 3) % 3];
            String x = (run / 9) % 2 == 0 ? "a" : "b";
            String query =
                    String.format(
                            "SELECT a_id, b_id, c_id, a_v, b_v, c_v FROM A %s B ON a_k1 = b_k1"
                                    + " %s C ON %s_k2 = c_k2",
                            j1, j2, x);
            runAndCompare(random, query, null, run);
        }
    }

    /** N:1 stars and chains anchored on the fact table, into an upsert sink keyed by its key. */
    @Test
    void testStarsAndChainsUpsertByAnchorKey() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(2000 + run);
            String j1 = random.nextBoolean() ? "JOIN" : "LEFT JOIN";
            String j2 = random.nextBoolean() ? "JOIN" : "LEFT JOIN";
            String x = random.nextBoolean() ? "a" : "b";
            String query =
                    String.format(
                            "SELECT a_id, b_v, c_v FROM A %s B ON a_k1 = b_id %s C ON %s_k2 = c_id",
                            j1, j2, x);
            runAndCompare(random, query, "a_id", run);
        }
    }

    /** 1:N edges from the anchor, where children move between parents. */
    @Test
    void testOneToManyConverges() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(3000 + run);
            String j1 = random.nextBoolean() ? "JOIN" : "LEFT JOIN";
            String j2 = JOIN_TYPES[random.nextInt(3)];
            String query =
                    String.format(
                            "SELECT a_id, b_id, c_id, b_v, c_v FROM A %s B ON a_id = b_k1"
                                    + " %s C ON b_k2 = c_k2",
                            j1, j2);
            runAndCompare(random, query, null, run);
        }
    }

    /**
     * GROUP output into an upsert sink keyed by a key the planner declares: a row of C can be read
     * under two rows of B by one recompute while it moves between them.
     */
    @Test
    void testGroupIntoUpsertSinkConverges() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(9000 + run);
            String query =
                    "SELECT a_id, c_id, b_id, c_v FROM A JOIN (B JOIN C ON b_id = c_k2)"
                            + " ON a_k1 = b_k1";
            runAndCompare(random, query, "a_id,c_id", run);
        }
    }

    /** Four inputs with joins on both sides of the top join. */
    @Test
    void testFourWayBushyTreesConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(10000 + run);
            String j1 = JOIN_TYPES[random.nextInt(3)];
            String j2 = JOIN_TYPES[random.nextInt(3)];
            String j3 = JOIN_TYPES[random.nextInt(3)];
            String query =
                    String.format(
                            "SELECT a_id, b_id, c_id, d_id, b_v, d_v FROM (A %s B ON a_k1 = b_k1)"
                                    + " %s (C %s D ON c_k1 = d_k1) ON b_k2 = c_k2",
                            j1, j2, j3);
            runAndCompare(random, query, null, run);
        }
    }

    /** Bushy trees: the anchor joins a subtree that is itself a join. */
    @Test
    void testBushyTreesConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(5000 + run);
            String j1 = JOIN_TYPES[random.nextInt(3)];
            String j2 = JOIN_TYPES[random.nextInt(3)];
            String query =
                    String.format(
                            "SELECT a_id, b_id, c_id, a_v, b_v, c_v FROM A %s (B %s C ON b_k2 = c_k2)"
                                    + " ON a_k1 = b_k1",
                            j1, j2);
            runAndCompare(random, query, null, run);
        }
    }

    /** A table joined with itself: every change must reach the anchors of both occurrences. */
    @Test
    void testSelfJoinsConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(6000 + run);
            String j1 = JOIN_TYPES[random.nextInt(3)];
            String query =
                    String.format(
                            "SELECT x.a_id AS id1, y.a_id AS id2, x.a_v AS v1, y.a_v AS v2"
                                    + " FROM A x %s A y ON x.a_k1 = y.a_k2",
                            j1);
            runAndCompare(random, query, null, run);
        }
    }

    /**
     * Calcs between joins: a projection that drops the keys of the lower join, so that a group can
     * hold the same row several times, and a filter on a padded side.
     */
    @Test
    void testCalcsBetweenJoinsConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(7000 + run);
            String j1 = JOIN_TYPES[random.nextInt(3)];
            String j2 = JOIN_TYPES[random.nextInt(3)];
            // the projection drops the key of B, so that a group can hold the same row several
            // times; with a RIGHT join B is the anchor, and its key must be read back
            String query =
                    random.nextBoolean()
                            ? String.format(
                                    "SELECT a_id, c_id, c_v FROM (SELECT a_id, b_k2 FROM A %s B"
                                            + " ON a_k1 = b_k1) t %s C ON t.b_k2 = c_k2",
                                    j1, j2)
                            : String.format(
                                    "SELECT a_id, b_id, c_id, c_v FROM (SELECT a_id, b_id, b_k2"
                                            + " FROM A %s B ON a_k1 = b_k1"
                                            + " WHERE b_v IS NULL OR b_v > 3) t"
                                            + " %s C ON t.b_k2 = c_k2",
                                    j1, j2);
            runAndCompare(random, query, null, run);
        }
    }

    /** Failures restore the operators from checkpoints while the sources replay their changes. */
    @Test
    void testRecoversFromFailures() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(8000 + run);
            failAfter = 3 + random.nextInt(10);
            String j1 = JOIN_TYPES[random.nextInt(3)];
            String j2 = JOIN_TYPES[random.nextInt(3)];
            String x = random.nextBoolean() ? "a" : "b";
            String query =
                    String.format(
                            "SELECT a_id, b_id, c_id, a_v, b_v, c_v FROM A %s B ON a_k1 = b_k1"
                                    + " %s C ON %s_k2 = c_k2",
                            j1, j2, x);
            runAndCompare(random, query, null, run);
            assertThat(restoredFromCheckpoint)
                    .as("run %s restored from a checkpoint", run)
                    .isTrue();
        }
    }

    /**
     * Failures with an upsert sink that is not rolled back, like a key-value store: rows that the
     * failed run emitted after the last checkpoint stay in the sink, and the restored run must
     * still bring the sink to the join's result.
     */
    @Test
    void testUpsertRecoversIntoSinkThatIsNotRolledBack() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        sinkInStore = true;
        expectedMode = "UPSERT";
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(9000 + run);
            failAfter = 3 + random.nextInt(10);
            String j1 = random.nextBoolean() ? "JOIN" : "LEFT JOIN";
            String j2 = random.nextBoolean() ? "JOIN" : "LEFT JOIN";
            String x = random.nextBoolean() ? "a" : "b";
            String query =
                    String.format(
                            "SELECT a_id, b_v, c_v FROM A %s B ON a_k1 = b_id %s C ON %s_k2 = c_id",
                            j1, j2, x);
            runAndCompare(random, query, "a_id", run);
            assertThat(restoredFromCheckpoint)
                    .as("run %s restored from a checkpoint", run)
                    .isTrue();
        }
    }

    /**
     * A row that moves away from its partner and is never touched again: only its old image can
     * tell the partner's anchor that the line is gone.
     */
    @Test
    void testRowLeavingItsPartnerIsRetracted() throws Exception {
        for (int run = 0; run < 5; run++) {
            List<Change> rowsOfB = new ArrayList<>();
            rowsOfB.add(Change.upsert(10, 1, 1, 0));
            // unrelated changes, so that the anchor is recomputed before the row moves
            for (int i = 0; i < 40; i++) {
                rowsOfB.add(Change.upsert(20, 3, 3, i));
            }
            rowsOfB.add(Change.upsert(10, 2, 1, 0));
            Map<String, List<Change>> scripts = new LinkedHashMap<>();
            scripts.put("A", List.of(Change.upsert(1, 1, 1, 0)));
            scripts.put("B", rowsOfB);
            String query = "SELECT a_id, b_id, a_v, b_v FROM A LEFT JOIN B ON a_k1 = b_k1";
            assertThat(run(scripts, query, null, run, false)).as(query).isEmpty();
        }
    }

    /**
     * Trees of three tables with group refreshes: the join writes an output table that applies
     * writes only when the sink is flushed before a checkpoint, and reads the rows of an anchor row
     * back from it instead of keeping them in state.
     */
    @Test
    void testTreesReadBackConverge() throws Exception {
        configureReadBack();
        for (int run = 0; run < Math.max(RUNS, 18); run++) {
            Random random = new Random(10_000 + run);
            List<String> diff = runReadBack(random, readBackQuery(random), run);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    /**
     * Failures restore the join from a checkpoint. The output table is not rolled back: it keeps
     * the rows that the failed attempt flushed, and the join reads them back.
     */
    @Test
    void testTreesReadBackRecoverFromFailures() throws Exception {
        configureReadBack();
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        for (int run = 0; run < Math.max(RUNS, 18); run++) {
            Random random = new Random(11_000 + run);
            failAfter = 3 + random.nextInt(10);
            List<String> diff = runReadBack(random, readBackQuery(random), run);
            assertThat(diff).as("run %s", run).isEmpty();
            assertThat(restoredFromCheckpoint)
                    .as("run %s restored from a checkpoint", run)
                    .isTrue();
        }
    }

    private void configureReadBack() {
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED, true);
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
    }

    /**
     * A star (on a) or chain (on b) of three tables with random join types. The output table holds
     * the ids of all rows, -1 for padded ones, so that it is keyed by them whichever table anchors.
     */
    private static String readBackQuery(Random random) {
        return String.format(
                "SELECT COALESCE(a_id, -1) AS a_id, COALESCE(b_id, -1) AS b_id,"
                        + " COALESCE(c_id, -1) AS c_id, a_v, b_v, c_v FROM A %s B ON a_k1 = b_k1"
                        + " %s C ON %s_k2 = c_k2",
                JOIN_TYPES[random.nextInt(3)],
                JOIN_TYPES[random.nextInt(3)],
                random.nextBoolean() ? "a" : "b");
    }

    /** Runs one random scenario into an output table that the join reads back. */
    private List<String> runReadBack(Random random, String query, int run) throws Exception {
        Map<String, List<Change>> scripts = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> table : tables().entrySet()) {
            scripts.put(table.getKey(), script(random, table.getValue().size()));
        }
        return runReadBack(scripts, query, run);
    }

    /** Runs one scenario with the given scripts into an output table that the join reads back. */
    private List<String> runReadBack(Map<String, List<Change>> scripts, String query, int run)
            throws Exception {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        true);
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
        Map<String, List<String>> tables = tables();
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            List<Change> script = scripts.getOrDefault(table.getKey(), List.of());
            SharedStoreTableFactory.register(STORE, table.getKey(), script);
            int tableFailAfter =
                    table.getKey().equals("A") && failAfter >= 0 && !script.isEmpty()
                            ? Math.min(failAfter, script.size() - 1)
                            : -1;
            createStoreTable(table.getKey(), table.getValue(), false, tableFailAfter);
        }
        // without a cache, every anchor row not written since the last checkpoint is read back
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                        run % 2 == 0 ? 0 : 10000);

        String sink = "read_back_" + run;
        Schema.Builder schema = Schema.newBuilder();
        for (String column : List.of("a_id", "b_id", "c_id", "a_v", "b_v", "c_v")) {
            schema.column(
                    column, column.endsWith("_id") ? DataTypes.INT().notNull() : DataTypes.INT());
        }
        schema.primaryKey("a_id", "b_id", "c_id");
        // whichever table anchors, its id can be looked up
        schema.index("a_id");
        schema.index("b_id");
        schema.index("c_id");
        tEnv.createTemporaryTable(
                sink,
                TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                        .schema(schema.build())
                        .option(SharedStoreTableFactory.STORE, STORE)
                        .option(SharedStoreTableFactory.SINK_BUFFER_UNTIL_FLUSH, true)
                        .build());

        // the join declares the primary key of the output table as its key: no conflicts to
        // resolve, and no materialization in the sink
        String insert = "INSERT INTO " + sink + " " + query;
        String plan = tEnv.explainSql(insert);
        String execPlan = plan.substring(plan.indexOf("== Optimized Execution Plan =="));
        assertThat(execPlan)
                .as(query)
                .contains("GroupRefreshDeltaJoin(")
                .contains("readBack=[")
                .doesNotContain("upsertMaterialize=[true]");
        if (expectedInPlan != null) {
            assertThat(execPlan).as(query).contains(expectedInPlan);
        }
        if (run % 3 == 2) {
            String json = tEnv.compilePlanSql(insert).asJsonString();
            assertThat(json).contains("\"readBack\"");
            tEnv.loadPlan(PlanReference.fromJsonString(json)).execute().await();
        } else {
            tEnv.executeSql(insert).await();
        }

        restoredFromCheckpoint = SharedStoreTableFactory.restoredFromCheckpoint(STORE, "A");
        List<String> actual = sorted(SharedStoreTableFactory.sinkRows(STORE, sink));
        List<String> expected = sorted(expected(tables, query));
        if (actual.equals(expected)) {
            return Collections.emptyList();
        }
        List<String> diff = new ArrayList<>();
        diff.add("query: " + query);
        diff.add("expected: " + expected);
        diff.add("actual:   " + actual);
        return diff;
    }

    /**
     * Stars of B and C joined on A's key: mostly B and C change, so that their changes refresh only
     * their own lines, while A changes now and then and refreshes whole groups. Children move
     * between parents, and parents lose their last and gain their first children.
     */
    @Test
    void testChildChangesOfStarsConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(12_000 + run);
            List<String> diff =
                    run(childScripts(random), starQuery(random, false), null, run, false);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    /** As {@link #testChildChangesOfStarsConverge}, reading single rows back from the output. */
    @Test
    void testChildChangesOfStarsReadBackConverge() throws Exception {
        configureReadBack();
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(13_000 + run);
            List<String> diff = runReadBack(childScripts(random), starQuery(random, true), run);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    /**
     * As {@link #testChildChangesOfStarsConverge}, with pages of one row: groups with several rows
     * of B or C are read and refreshed page by page.
     */
    @Test
    void testPagedStarsConverge() throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 1);
        long paged = 0;
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(16_000 + run);
            List<String> diff =
                    run(childScripts(random), pagedStarQuery(random, false, run), null, run, false);
            assertThat(diff).as("run %s", run).isEmpty();
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /** As {@link #testPagedStarsConverge}, reading the rows emitted before back in pages. */
    @Test
    void testPagedStarsReadBackConverge() throws Exception {
        configureReadBack();
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 1);
        long paged = 0;
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(17_000 + run);
            List<String> diff =
                    runReadBack(childScripts(random), pagedStarQuery(random, true, run), run);
            assertThat(diff).as("run %s", run).isEmpty();
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /**
     * Facts A referencing B, which references C: a change of B or C locates the facts through
     * lookups by the referencing columns. Read in pages of one row, a change that leads to many
     * facts hands them over page by page.
     */
    @Test
    void testLocateInPagesConverges() throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 1);
        long paged = 0;
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(18_000 + run);
            String query =
                    "SELECT a_id, b_v, c_v FROM A LEFT JOIN B ON a_k1 = b_id"
                            + " LEFT JOIN C ON b_k1 = c_id";
            runAndCompare(random, query, "a_id", run);
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /**
     * A star of B and C; in every second run with a condition beyond the keys, so that a parent can
     * have many children of which none matches, and is padded after the last page.
     */
    private static String pagedStarQuery(Random random, boolean readBack, int run) {
        String query = starQuery(random, readBack);
        return run % 2 == 0
                ? query
                : query.replace("a_id = b_k1", "a_id = b_k1 AND b_v >= a_v")
                        .replace("a_id = c_k1", "a_id = c_k1 AND c_v >= a_v");
    }

    private static String starQuery(Random random, boolean readBack) {
        String columns =
                readBack
                        ? "COALESCE(a_id, -1) AS a_id, COALESCE(b_id, -1) AS b_id,"
                                + " COALESCE(c_id, -1) AS c_id, a_v, b_v, c_v"
                        : "a_id, b_id, c_id, a_v, b_v, c_v";
        return String.format(
                "SELECT %s FROM A %s B ON a_id = b_k1 %s C ON a_id = c_k1",
                columns,
                random.nextBoolean() ? "JOIN" : "LEFT JOIN",
                random.nextBoolean() ? "JOIN" : "LEFT JOIN");
    }

    /** A few parents that rarely change, and many changes of their children. */
    private static Map<String, List<Change>> childScripts(Random random) {
        Map<String, List<Change>> scripts = new LinkedHashMap<>();
        List<Change> parents = new ArrayList<>();
        for (int id = 1; id <= 4; id++) {
            parents.add(
                    Change.upsert(id, random.nextInt(3), random.nextInt(3), random.nextInt(10)));
        }
        for (int i = random.nextInt(3); i > 0; i--) {
            int id = 1 + random.nextInt(4);
            parents.add(
                    random.nextInt(3) == 0
                            ? Change.delete(id, null, null, null)
                            : Change.upsert(id, 0, 0, random.nextInt(10)));
        }
        scripts.put("A", parents);
        scripts.put("B", childScript(random));
        scripts.put("C", childScript(random));
        return scripts;
    }

    /** Children of the parents 1 to 4 (key 1): mostly updates of their values, some moves. */
    private static List<Change> childScript(Random random) {
        List<Change> script = new ArrayList<>();
        Map<Integer, Integer> parentOf = new HashMap<>();
        for (int i = 20 + random.nextInt(40); i > 0; i--) {
            int id = 1 + random.nextInt(8);
            Integer parent = parentOf.get(id);
            int dice = random.nextInt(10);
            if (parent != null && dice < 2) {
                script.add(Change.delete(id, null, null, null));
                parentOf.remove(id);
                continue;
            }
            if (parent == null || dice < 3) {
                // a new child, or a child that moves to another parent
                parent = 1 + random.nextInt(4);
                parentOf.put(id, parent);
            }
            script.add(Change.upsert(id, parent, random.nextInt(3), random.nextInt(10)));
        }
        return script;
    }

    /**
     * Chains whose tables join their parent on the parent's key and the anchor key: B joins A on
     * A's key, C joins B on B's key and B's anchor key, and D joins C on the anchor key. A refresh
     * reads all rows of C of its group at once, and rows of C and D name their group themselves.
     */
    @Test
    void testChainsKeyedByTheAnchorKeyConverge() throws Exception {
        expectedInPlan = "readsGroupsOf=[#2]";
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(6000 + run);
            String query =
                    run % 2 == 0
                            ? "SELECT a_id, b_id, c_id, a_v, b_v, c_v FROM A"
                                    + CHAIN_KEYED_BY_ANCHOR_KEY
                            : "SELECT a_id, b_id, c_id, d_id, a_v, b_v, c_v, d_v FROM A"
                                    + CHAIN_KEYED_BY_ANCHOR_KEY
                                    + " LEFT JOIN D ON d_k1 = c_k1";
            runAndCompare(random, query, null, run);
        }
    }

    @Test
    void testChainsKeyedByTheAnchorKeyReadBackConverge() throws Exception {
        configureReadBack();
        expectedInPlan = "readsGroupsOf=[#2]";
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(7000 + run);
            String query =
                    "SELECT COALESCE(a_id, -1) AS a_id, COALESCE(b_id, -1) AS b_id,"
                            + " COALESCE(c_id, -1) AS c_id, a_v, b_v, c_v FROM A"
                            + CHAIN_KEYED_BY_ANCHOR_KEY;
            List<String> diff = runReadBack(random, query, run);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    /**
     * Snowflakes: A references B, which references C. Every row of A has at most one output row,
     * and the output table, keyed by all ids, needs the old row of every change: the join reads it
     * back.
     */
    @Test
    void testSnowflakesReadBackConverge() throws Exception {
        configureReadBack();
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(19_000 + run);
            String query =
                    "SELECT COALESCE(a_id, -1) AS a_id, COALESCE(b_id, -1) AS b_id,"
                            + " COALESCE(c_id, -1) AS c_id, a_v, b_v, c_v FROM A"
                            + " LEFT JOIN B ON a_k1 = b_id LEFT JOIN C ON b_k1 = c_id";
            List<String> diff = runReadBack(random, query, run);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    private static final String CHAIN_KEYED_BY_ANCHOR_KEY =
            " LEFT JOIN B ON b_k1 = a_id LEFT JOIN C ON c_k1 = b_k1 AND c_k2 = b_id";

    /** Without read-after-visible lookups the join does not converge, and the test notices. */
    @Test
    void testStaleLookupsAreDetected() throws Exception {
        int mismatches = 0;
        for (int run = 0; run < 40 && mismatches == 0; run++) {
            Random random = new Random(4000 + run);
            String query =
                    "SELECT a_id, b_id, c_id, a_v, b_v, c_v FROM A LEFT JOIN B ON a_k1 = b_k1"
                            + " LEFT JOIN C ON b_k2 = c_k2";
            if (!run(random, query, null, run, true).isEmpty()) {
                mismatches++;
            }
        }
        assertThat(mismatches).isGreaterThan(0);
    }

    // --------------------------------------------------------------------------------------------

    private void runAndCompare(Random random, String query, String sinkKey, int run)
            throws Exception {
        List<String> diff = run(random, query, sinkKey, run, false);
        assertThat(diff).as("run %s of %s", run, query).isEmpty();
    }

    private static Map<String, List<String>> tables() {
        Map<String, List<String>> tables = new LinkedHashMap<>();
        tables.put("A", List.of("a_id", "a_k1", "a_k2", "a_v"));
        tables.put("B", List.of("b_id", "b_k1", "b_k2", "b_v"));
        tables.put("C", List.of("c_id", "c_k1", "c_k2", "c_v"));
        tables.put("D", List.of("d_id", "d_k1", "d_v"));
        return tables;
    }

    /** Runs one random scenario and returns the differences between sink and expected rows. */
    private List<String> run(
            Random random, String query, String sinkKey, int run, boolean staleReads)
            throws Exception {
        Map<String, List<Change>> scripts = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> table : tables().entrySet()) {
            scripts.put(table.getKey(), script(random, table.getValue().size()));
        }
        return run(scripts, query, sinkKey, run, staleReads);
    }

    /** Runs one scenario with the given scripts. */
    private List<String> run(
            Map<String, List<Change>> scripts,
            String query,
            String sinkKey,
            int run,
            boolean staleReads)
            throws Exception {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        false);
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
        Map<String, List<String>> tables = tables();
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            List<Change> script = scripts.getOrDefault(table.getKey(), List.of());
            SharedStoreTableFactory.register(STORE, table.getKey(), script);
            // only the first table fails, before its last change, so that it fails exactly once
            int tableFailAfter =
                    table.getKey().equals("A") && failAfter >= 0 && !script.isEmpty()
                            ? Math.min(failAfter, script.size() - 1)
                            : -1;
            createStoreTable(table.getKey(), table.getValue(), staleReads, tableFailAfter);
        }

        String sink = "sink_" + run;
        List<String> outputColumns = outputColumns(query);
        if (sinkInStore) {
            createStoreSink(sink, outputColumns, sinkKey);
        } else {
            createSink(sink, outputColumns, sinkKey);
        }
        String insert = "INSERT INTO " + sink + " " + query;
        String plan = tEnv.explainSql(insert);
        assertThat(plan).as(query).contains("GroupRefreshDeltaJoin(");
        if (expectedMode != null) {
            assertThat(plan).as(query).contains("mode=[" + expectedMode + "]");
        }
        if (expectedInPlan != null) {
            assertThat(plan).as(query).contains(expectedInPlan);
        }
        tEnv.executeSql(insert).await();

        restoredFromCheckpoint = SharedStoreTableFactory.restoredFromCheckpoint(STORE, "A");
        List<String> actual =
                sorted(
                        sinkInStore
                                ? SharedStoreTableFactory.sinkRows(STORE, sink)
                                : TestValuesTableFactory.getResultsAsStrings(sink));
        List<String> expected = sorted(expected(tables, query));
        if (actual.equals(expected)) {
            return Collections.emptyList();
        }
        List<String> diff = new ArrayList<>();
        diff.add("query: " + query);
        diff.add("expected: " + expected);
        diff.add("actual:   " + actual);
        return diff;
    }

    /** A random script of upserts and deletes over a few ids and join keys. */
    private static List<Change> script(Random random, int arity) {
        int length = 4 + random.nextInt(20);
        List<Change> script = new ArrayList<>();
        Map<Integer, Boolean> live = new HashMap<>();
        for (int i = 0; i < length; i++) {
            int id = 1 + random.nextInt(5);
            if (live.getOrDefault(id, false) && random.nextInt(10) < 3) {
                Object[] key = new Object[arity];
                key[0] = id;
                script.add(Change.delete(key));
                live.put(id, false);
                continue;
            }
            Object[] row = new Object[arity];
            row[0] = id;
            for (int f = 1; f < arity - 1; f++) {
                // join keys: a few values, so that edges are N:M and rows move between partners
                row[f] = random.nextInt(8) == 0 ? null : 1 + random.nextInt(3);
            }
            row[arity - 1] = random.nextInt(10);
            script.add(Change.upsert(row));
            live.put(id, true);
        }
        return script;
    }

    private void createStoreTable(
            String name, List<String> columns, boolean staleReads, int failAfter) {
        Schema.Builder schema = Schema.newBuilder();
        schema.column(columns.get(0), DataTypes.INT().notNull());
        for (int i = 1; i < columns.size(); i++) {
            schema.column(columns.get(i), DataTypes.INT());
        }
        schema.primaryKey(columns.get(0));
        // every column but the value can be looked up
        for (int i = 0; i < columns.size() - 1; i++) {
            schema.index(columns.get(i));
        }
        tEnv.dropTemporaryTable(name);
        tEnv.createTemporaryTable(
                name,
                TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                        .schema(schema.build())
                        .option(SharedStoreTableFactory.STORE, STORE)
                        .option(SharedStoreTableFactory.STALE_READS, staleReads)
                        .option(SharedStoreTableFactory.FAIL_AFTER, failAfter)
                        .build());
    }

    private void createSink(String name, List<String> columns, String key) {
        List<String> keyColumns = key == null ? List.of() : List.of(key.split(","));
        Schema.Builder schema = Schema.newBuilder();
        for (String column : columns) {
            schema.column(
                    column,
                    keyColumns.contains(column) ? DataTypes.INT().notNull() : DataTypes.INT());
        }
        TableDescriptor.Builder descriptor =
                TableDescriptor.forConnector("values").option("sink-insert-only", "false");
        if (key != null) {
            schema.primaryKey(keyColumns);
            descriptor.option("sink.supports-delete-by-key", "true");
        }
        tEnv.createTemporaryTable(name, descriptor.schema(schema.build()).build());
    }

    private void createStoreSink(String name, List<String> columns, String key) {
        Schema.Builder schema = Schema.newBuilder();
        for (String column : columns) {
            schema.column(column, column.equals(key) ? DataTypes.INT().notNull() : DataTypes.INT());
        }
        schema.primaryKey(key);
        tEnv.createTemporaryTable(
                name,
                TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                        .schema(schema.build())
                        .option(SharedStoreTableFactory.STORE, STORE)
                        .build());
    }

    private static List<String> outputColumns(String query) {
        String select = query.substring("SELECT ".length(), query.indexOf(" FROM "));
        List<String> columns = new ArrayList<>();
        for (String item : select.split(", ")) {
            int alias = item.indexOf(" AS ");
            String column = alias >= 0 ? item.substring(alias + 4) : item;
            columns.add(column.substring(column.indexOf('.') + 1));
        }
        return columns;
    }

    /** The join over the final state of the tables, computed by a batch job. */
    private static List<String> expected(Map<String, List<String>> tables, String query)
            throws Exception {
        return expected(STORE, tables, query);
    }

    /** The join over the final state of the tables of a store, computed by a batch job. */
    static List<String> expected(String store, Map<String, List<String>> tables, String query)
            throws Exception {
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            Collection<GenericRowData> rows =
                    SharedStoreTableFactory.currentRows(store, table.getKey());
            List<Row> data =
                    rows.stream()
                            .map(
                                    row -> {
                                        Object[] fields = new Object[row.getArity()];
                                        for (int i = 0; i < fields.length; i++) {
                                            fields[i] = row.getField(i);
                                        }
                                        return Row.of(fields);
                                    })
                            .collect(Collectors.toList());
            Schema.Builder schema = Schema.newBuilder();
            for (String column : table.getValue()) {
                schema.column(column, DataTypes.INT());
            }
            batch.createTemporaryTable(
                    table.getKey(),
                    TableDescriptor.forConnector("values")
                            .schema(schema.build())
                            .option("data-id", TestValuesTableFactory.registerData(data))
                            .option("bounded", "true")
                            .build());
        }
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = batch.executeSql(query).collect()) {
            it.forEachRemaining(row -> result.add(row.toString()));
        }
        return result;
    }

    private static List<String> sorted(List<String> rows) {
        List<String> copy = new ArrayList<>(rows);
        Collections.sort(copy);
        return copy;
    }
}
