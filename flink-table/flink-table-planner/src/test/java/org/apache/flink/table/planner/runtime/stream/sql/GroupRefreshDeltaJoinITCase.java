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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
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
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Randomized end-to-end tests of delta joins that refresh groups: INNER, LEFT, RIGHT and FULL joins
 * of two tables whose rows are inserted, updated (also in their join keys) and deleted.
 *
 * <p>Every run generates random scripts for the tables. The {@link SharedStoreTableFactory} sources
 * emit a change only after applying it, with its old row, and lookups read the current state after
 * a random delay, so changes of the two tables interleave freely with lookups. When the job has
 * finished, the sink must equal the join over the final state of the tables, computed by a batch
 * job.
 */
class GroupRefreshDeltaJoinITCase extends StreamAbstractTestBase {

    private static final int RUNS =
            Integer.parseInt(System.getProperties().getProperty("group.refresh.runs", "12"));
    private static final String STORE = "group-refresh-delta-join-it";
    private static final String[] JOIN_TYPES = {"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"};

    /**
     * Queries whose output table holds the join key and the ids of both rows in its primary key, so
     * that the join can read the rows of a join key back from it. Ids of padded rows are -1, and
     * rows whose join key is NULL are written under the key 0.
     */
    private static final String[] READ_BACK_QUERIES = {
        "SELECT a_k1 AS k, a_id, b_id, a_v, b_v FROM A JOIN B ON a_k1 = b_k1",
        "SELECT COALESCE(a_k1, 0) AS k, a_id, COALESCE(b_id, -1) AS b_id, a_v, b_v"
                + " FROM A LEFT JOIN B ON a_k1 = b_k1",
        "SELECT COALESCE(b_k1, 0) AS k, COALESCE(a_id, -1) AS a_id, b_id, a_v, b_v"
                + " FROM A RIGHT JOIN B ON a_k1 = b_k1",
        "SELECT COALESCE(a_k1, b_k1, 0) AS k, COALESCE(a_id, -1) AS a_id,"
                + " COALESCE(b_id, -1) AS b_id, a_v, b_v FROM A FULL JOIN B ON a_k1 = b_k1"
    };

    /**
     * Joins with a parent: B has at most one row per join key, its primary key. Changes of A, the
     * children, refresh only their own rows, read back by the primary key of the output table.
     */
    private static final String[] PARENT_READ_BACK_QUERIES = {
        "SELECT a_k1 AS k, a_id, b_id, a_v, b_v FROM A JOIN B ON a_k1 = b_id",
        "SELECT COALESCE(a_k1, 0) AS k, a_id, COALESCE(b_id, -1) AS b_id, a_v, b_v"
                + " FROM A LEFT JOIN B ON a_k1 = b_id",
        "SELECT b_id AS k, COALESCE(a_id, -1) AS a_id, b_id, a_v, b_v"
                + " FROM A RIGHT JOIN B ON a_k1 = b_id",
        "SELECT COALESCE(a_k1, b_id, 0) AS k, COALESCE(a_id, -1) AS a_id,"
                + " COALESCE(b_id, -1) AS b_id, a_v, b_v FROM A FULL JOIN B ON a_k1 = b_id"
    };

    private static final List<String> READ_BACK_COLUMNS =
            List.of("k", "a_id", "b_id", "a_v", "b_v");

    private TableEnvironment tEnv;

    /** Whether read-back sinks let the join read what it wrote, or only after a checkpoint. */
    private boolean readYourWrites = true;

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
                        OptimizerConfigOptions.DeltaJoinGrouping.JOIN_KEY);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY, 8);
    }

    @AfterEach
    void cleanup() {
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
    }

    /** Many-to-many join keys that rows move between, for every join type. */
    @Test
    void testAllJoinTypesConverge() throws Exception {
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_k1",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(1000 + run), query, run);
        }
    }

    /**
     * With pages of two rows, most groups are read and refreshed in pages: rows of both inputs, and
     * the rows emitted before, kept in state.
     */
    @Test
    void testPagedGroupsConverge() throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 2);
        long paged = 0;
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_k1%s",
                            JOIN_TYPES[run % JOIN_TYPES.length],
                            run % 8 >= 4 ? " AND a_v >= b_v" : "");
            runAndCompare(new Random(14000 + run), query, run);
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /**
     * A refresh in pages deletes the rows it replaces before it inserts their successors, as a sink
     * keyed coarser than the rows needs: the rows of a child, padded or joined with its parent,
     * have different row keys but the same key in the sink.
     */
    @Test
    void testPagedGroupsConvergeInSinkKeyedByChild() throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 2);
        long paged = 0;
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query =
                    "SELECT a_id, a_k1, b_id, a_v, b_v FROM A LEFT JOIN B ON a_k1 = b_id"
                            + " WHERE a_k1 IS NOT NULL";
            List<String> diff =
                    run(new Random(16000 + run), query, run, false, List.of("a_id", "a_k1"));
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /** As {@link #testPagedGroupsConverge}, reading the rows emitted before back in pages. */
    @Test
    void testPagedReadBackConverges() throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_PAGE_SIZE, 2);
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
        long paged = 0;
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query = READ_BACK_QUERIES[run % READ_BACK_QUERIES.length];
            if (run % 8 >= 4) {
                query += " AND a_v >= b_v";
            }
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 10000);
            List<String> diff = runReadBack(new Random(15000 + run), query, -1, run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
            paged += SharedStoreTableFactory.pagedLookups(STORE);
        }
        assertThat(paged).as("lookups read in pages").isPositive();
    }

    /** A condition beyond the join keys decides which rows match, and so which are padded. */
    @Test
    void testRemainingConditionConverges() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_k1 AND a_v >= b_v",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(2000 + run), query, run);
        }
    }

    /** Joins on two keys, one of them the key of the other table. */
    @Test
    void testCompositeKeysConverge() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_k1 AND a_k2 = b_id",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(3000 + run), query, run);
        }
    }

    /**
     * Joins on the primary key of one table: its rows never move to another join key, so the
     * planner may drop their old images, and the join still refreshes groups.
     */
    @Test
    void testJoinOnPrimaryKeyConverges() throws Exception {
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_id",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(4500 + run), query, run);
        }
    }

    /** A filter and a projection between a table and the join. */
    @Test
    void testCalcOnInputConverges() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, b_v FROM A %s (SELECT b_id, b_k1, b_v FROM B"
                                    + " WHERE b_v > 3) t ON a_k1 = t.b_k1",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(4000 + run), query, run);
        }
    }

    /**
     * The join writes an output table that applies writes only when the sink is flushed before a
     * checkpoint, and reads the rows of a join key back from it instead of keeping them in state.
     */
    @Test
    void testReadBackConverges() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            String query = READ_BACK_QUERIES[run % READ_BACK_QUERIES.length];
            if (run % 8 >= 4) {
                query += " AND a_v >= b_v";
            }
            // without a cache, every join key not written since the last checkpoint is read back
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 10000);
            List<String> diff = runReadBack(new Random(6000 + run), query, -1, run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
        }
    }

    /**
     * Failures restore the join from a checkpoint. The output table is not rolled back: it keeps
     * the rows that the failed attempt flushed, and loses those it did not, and the join must still
     * bring it to the join's result.
     */
    @Test
    void testReadBackRecoversFromFailures() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            Random random = new Random(7000 + run);
            String query = READ_BACK_QUERIES[run % READ_BACK_QUERIES.length];
            if (run % 8 >= 4) {
                query += " AND a_v >= b_v";
            }
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 10000);
            List<String> diff = runReadBack(random, query, 3 + random.nextInt(10), run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
            assertThat(SharedStoreTableFactory.restoredFromCheckpoint(STORE, "A"))
                    .as("run %s restored from a checkpoint", run)
                    .isTrue();
        }
    }

    /** Children refresh only their own rows, also across failures. */
    @Test
    void testParentJoinsConverge() throws Exception {
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        for (int run = 0; run < Math.max(RUNS, 16); run++) {
            Random random = new Random(12000 + run);
            String query = PARENT_READ_BACK_QUERIES[run % PARENT_READ_BACK_QUERIES.length];
            if (run % 8 >= 4) {
                query += " AND a_v >= b_v";
            }
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 3);
            int failAfter = run % 3 == 0 ? 3 + random.nextInt(10) : -1;
            List<String> diff = runReadBack(random, query, failAfter, run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
        }
    }

    /**
     * Without read-your-writes, the join keeps the groups it wrote in memory until a checkpoint
     * completed after them, and reads them back from then on.
     */
    @Test
    void testReadBackAfterCheckpointConverges() throws Exception {
        readYourWrites = false;
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "10 ms");
        for (int run = 0; run < RUNS; run++) {
            Random random = new Random(8000 + run);
            String query = READ_BACK_QUERIES[run % READ_BACK_QUERIES.length];
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 10000);
            int failAfter = run % 3 == 0 ? 3 + random.nextInt(10) : -1;
            List<String> diff = runReadBack(random, query, failAfter, run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
        }
    }

    /**
     * With read-your-writes, groups are read back at any time: the join needs no checkpoints, and
     * keeps only a cache of recent groups.
     */
    @Test
    void testReadYourWritesConvergeWithoutCheckpoints() throws Exception {
        for (int run = 0; run < RUNS; run++) {
            String query = READ_BACK_QUERIES[run % READ_BACK_QUERIES.length];
            tEnv.getConfig()
                    .set(
                            ExecutionConfigOptions
                                    .TABLE_EXEC_DELTA_JOIN_GROUP_REFRESH_OUTPUT_CACHE_SIZE,
                            run % 2 == 0 ? 0 : 3);
            List<String> diff = runReadBack(new Random(9000 + run), query, -1, run);
            assertThat(diff).as("run %s of %s", run, query).isEmpty();
        }
    }

    /**
     * An unaligned barrier may overtake rows on their way to the sink, so its completion does not
     * prove that the sink has them: without read-your-writes, the join cannot read its rows back,
     * and it does not keep them in state unless asked to.
     */
    @Test
    void testUnalignedCheckpointsRefuseReadBack() {
        readYourWrites = false;
        tEnv.getConfig().set("execution.checkpointing.interval", "20 ms");
        tEnv.getConfig().set("execution.checkpointing.unaligned.enabled", "true");
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        true);
        SharedStoreTableFactory.clear(STORE);
        for (Map.Entry<String, List<String>> table : tables().entrySet()) {
            SharedStoreTableFactory.register(STORE, table.getKey(), Collections.emptyList());
            createStoreTable(table.getKey(), table.getValue(), false, -1);
        }
        createReadBackSink("read_back_unaligned");
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "INSERT INTO read_back_unaligned "
                                                + READ_BACK_QUERIES[1]
                                                + " ON CONFLICT DO DEDUPLICATE"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "cannot read the rows it emitted back"
                                        + " (default_catalog.default_database.read_back_unaligned:"
                                        + " unaligned checkpoints do not prove that the sink"
                                        + " received the rows)"));
    }

    /**
     * A compiled plan that reads its output back is stopped with a savepoint and restored from it
     * and its compiled plan. The restored join remembers no group, so it reads every group back
     * from the output table: rows written before the savepoint that must go are found there and
     * deleted.
     */
    @Test
    void testReadBackRestoresFromSavepoint(@TempDir Path savepoints) throws Exception {
        SharedStoreTableFactory.clear(STORE);
        SharedStoreTableFactory.register(
                STORE,
                "A",
                List.of(
                        Change.upsert(1, 7, 0, 10),
                        Change.upsert(2, 8, 0, 20),
                        // after the savepoint
                        Change.upsert(3, 7, 0, 30)));
        SharedStoreTableFactory.register(
                STORE,
                "B",
                List.of(
                        Change.upsert(100, 7, 0, 1),
                        // after the savepoint: A 2 gets a match, A 1 loses its match
                        Change.upsert(200, 8, 0, 2),
                        Change.delete(100, null, null, null)));
        SharedStoreTableFactory.pauseAt(STORE, "A", 2);
        SharedStoreTableFactory.pauseAt(STORE, "B", 1);
        String query = READ_BACK_QUERIES[1];
        String insert = "INSERT INTO read_back_restored " + query;

        tEnv = readBackEnvironment(null);
        String json = tEnv.compilePlanSql(insert).asJsonString();
        assertThat(json).contains("\"readBack\"");
        JobClient job =
                tEnv.loadPlan(PlanReference.fromJsonString(json))
                        .execute()
                        .getJobClient()
                        .orElseThrow();
        awaitApplied("A", 2);
        awaitApplied("B", 1);
        // the join of the first parts of the scripts; the running job holds every slot, so no
        // batch job computes it
        awaitSinkRows(
                "read_back_restored",
                sorted(List.of("+I[7, 1, 100, 10, 1]", "+I[8, 2, -1, 20, null]")));
        String savepoint = stopWithSavepoint(job, savepoints);

        SharedStoreTableFactory.resume(STORE, "A");
        SharedStoreTableFactory.resume(STORE, "B");
        tEnv = readBackEnvironment(savepoint);
        tEnv.loadPlan(PlanReference.fromJsonString(json)).execute().await();
        assertThat(sorted(SharedStoreTableFactory.sinkRows(STORE, "read_back_restored")))
                .isEqualTo(sorted(expected(tables(), query)))
                .doesNotContain("+I[8, 2, -1, 20, null]", "+I[7, 1, 100, 10, 1]");
    }

    /**
     * By default, joins with an anchor are grouped by it, unless the output table can be read back
     * by the join key only: both converge.
     */
    @Test
    void testAutoGroupingConverges() throws Exception {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING,
                        OptimizerConfigOptions.DeltaJoinGrouping.AUTO);
        for (int run = 0; run < RUNS; run++) {
            String query =
                    String.format(
                            "SELECT a_id, b_id, a_v, b_v FROM A %s B ON a_k1 = b_k1",
                            JOIN_TYPES[run % JOIN_TYPES.length]);
            runAndCompare(new Random(10000 + run), query, run);
            List<String> diff =
                    runReadBack(
                            new Random(11000 + run),
                            READ_BACK_QUERIES[run % READ_BACK_QUERIES.length],
                            -1,
                            run);
            assertThat(diff).as("run %s", run).isEmpty();
        }
    }

    /** Without lookups that see every emitted change, the join does not converge, and we notice. */
    @Test
    void testStaleLookupsAreDetected() throws Exception {
        int mismatches = 0;
        for (int run = 0; run < 40 && mismatches == 0; run++) {
            String query = "SELECT a_id, b_id, a_v, b_v FROM A FULL JOIN B ON a_k1 = b_k1";
            if (!run(new Random(5000 + run), query, run, true).isEmpty()) {
                mismatches++;
            }
        }
        assertThat(mismatches).isGreaterThan(0);
    }

    // --------------------------------------------------------------------------------------------

    /** An environment with the store tables and a read-back sink, restoring from a savepoint. */
    private TableEnvironment readBackEnvironment(@Nullable String savepoint) {
        Configuration configuration = new Configuration();
        if (savepoint != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepoint, false, RecoveryClaimMode.NO_CLAIM),
                    configuration);
        }
        TableEnvironment env =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        env.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED, true);
        env.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY, 8);
        env.getConfig().set("parallelism.default", "4");
        env.getConfig().set("execution.checkpointing.interval", "20 ms");
        env.getConfig().set("execution.checkpointing.unaligned.enabled", "false");
        tEnv = env;
        for (Map.Entry<String, List<String>> table : tables().entrySet()) {
            createStoreTable(table.getKey(), table.getValue(), false, -1);
        }
        createReadBackSink("read_back_restored");
        return env;
    }

    private static void awaitApplied(String table, int changes) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> SharedStoreTableFactory.applied(STORE, table) >= changes);
    }

    private static void awaitSinkRows(String sink, List<String> expected) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> sorted(SharedStoreTableFactory.sinkRows(STORE, sink)).equals(expected));
    }

    /** Stops the job with a savepoint, retrying while not all of its tasks run yet. */
    private static String stopWithSavepoint(JobClient job, Path directory) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            try {
                return job.stopWithSavepoint(
                                false, directory.toString(), SavepointFormatType.DEFAULT)
                        .get();
            } catch (ExecutionException e) {
                if (e.getMessage() == null
                        || !e.getMessage().contains("Not all required tasks")
                        || System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
    }

    private void runAndCompare(Random random, String query, int run) throws Exception {
        List<String> diff = run(random, query, run, false);
        assertThat(diff).as("run %s of %s", run, query).isEmpty();
    }

    /**
     * Runs one random scenario into an output table that the join reads back, and returns the
     * differences between the output table and the expected rows. Table A fails once after the
     * given number of changes, unless it is negative.
     */
    private List<String> runReadBack(Random random, String query, int failAfter, int run)
            throws Exception {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        true);
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
        Map<String, List<String>> tables = tables();
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            List<Change> script = script(random, table.getValue().size(), true);
            SharedStoreTableFactory.register(STORE, table.getKey(), script);
            int tableFailAfter =
                    table.getKey().equals("A") && failAfter >= 0 && !script.isEmpty()
                            ? Math.min(failAfter, script.size() - 1)
                            : -1;
            createStoreTable(table.getKey(), table.getValue(), false, tableFailAfter);
        }

        String sink = "read_back_" + run;
        createReadBackSink(sink);
        // the join declares the primary key of the output table as its key: no conflicts to
        // resolve, and no materialization in the sink
        String insert = "INSERT INTO " + sink + " " + query;
        String plan = tEnv.explainSql(insert);
        String execPlan = plan.substring(plan.indexOf("== Optimized Execution Plan =="));
        boolean compiled = run % 3 == 2;
        // the projection moved into the join, and the sink keeps no state
        assertThat(execPlan)
                .as(query)
                .contains("groups=[join key]")
                .contains("readBack=[")
                .contains(readYourWrites ? "readableAfter=[write]" : "readableAfter=[checkpoint]")
                .doesNotContain(" AS k")
                .doesNotContain("upsertMaterialize=[true]");
        if (compiled) {
            // the read-back survives a compiled plan
            String json = tEnv.compilePlanSql(insert).asJsonString();
            assertThat(json).contains("\"readBack\"");
            tEnv.loadPlan(PlanReference.fromJsonString(json)).execute().await();
        } else {
            tEnv.executeSql(insert).await();
        }

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
     * An output table keyed by the join key and the ids of both rows, which applies writes only
     * when the sink is flushed and can be looked up by the join key.
     */
    private void createReadBackSink(String name) {
        Schema.Builder schema = Schema.newBuilder();
        for (String column : READ_BACK_COLUMNS) {
            schema.column(
                    column, column.endsWith("_v") ? DataTypes.INT() : DataTypes.INT().notNull());
        }
        schema.primaryKey("k", "a_id", "b_id");
        schema.index("k");
        tEnv.createTemporaryTable(
                name,
                TableDescriptor.forConnector(SharedStoreTableFactory.IDENTIFIER)
                        .schema(schema.build())
                        .option(SharedStoreTableFactory.STORE, STORE)
                        .option(SharedStoreTableFactory.SINK_BUFFER_UNTIL_FLUSH, true)
                        .option(SharedStoreTableFactory.SINK_READ_YOUR_WRITES, readYourWrites)
                        .build());
    }

    private static Map<String, List<String>> tables() {
        Map<String, List<String>> tables = new LinkedHashMap<>();
        tables.put("A", List.of("a_id", "a_k1", "a_k2", "a_v"));
        tables.put("B", List.of("b_id", "b_k1", "b_k2", "b_v"));
        return tables;
    }

    /** Runs one random scenario and returns the differences between sink and expected rows. */
    private List<String> run(Random random, String query, int run, boolean staleReads)
            throws Exception {
        return run(random, query, run, staleReads, null);
    }

    /** As {@link #run}, into a sink with the given primary key if not null. */
    private List<String> run(
            Random random,
            String query,
            int run,
            boolean staleReads,
            @Nullable List<String> sinkKey)
            throws Exception {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_READ_BACK,
                        false);
        SharedStoreTableFactory.clear(STORE);
        TestValuesTableFactory.clearAllData();
        Map<String, List<String>> tables = tables();
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            SharedStoreTableFactory.register(
                    STORE, table.getKey(), script(random, table.getValue().size(), true));
            createStoreTable(table.getKey(), table.getValue(), staleReads, -1);
        }

        String sink = "sink_" + run;
        createSink(sink, outputColumns(query), sinkKey);
        String insert = "INSERT INTO " + sink + " " + query;
        String plan = tEnv.explainSql(insert);
        assertThat(plan).as(query).contains("GroupRefreshDeltaJoin(");
        if (sinkKey != null) {
            assertThat(plan).as(query).doesNotContain("upsertMaterialize=[true]");
        }
        if (tEnv.getConfig()
                        .get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING)
                == OptimizerConfigOptions.DeltaJoinGrouping.JOIN_KEY) {
            assertThat(plan).as(query).contains("groups=[join key]");
        }
        tEnv.executeSql(insert).await();

        List<String> actual = sorted(TestValuesTableFactory.getResultsAsStrings(sink));
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
    private static List<Change> script(Random random, int arity, boolean nullKeys) {
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
                // join keys: a few values, so that keys are many-to-many and rows move
                row[f] = nullKeys && random.nextInt(8) == 0 ? null : 1 + random.nextInt(3);
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

    private void createSink(String name, List<String> columns, @Nullable List<String> key) {
        Schema.Builder schema = Schema.newBuilder();
        for (String column : columns) {
            schema.column(
                    column,
                    key != null && key.contains(column)
                            ? DataTypes.INT().notNull()
                            : DataTypes.INT());
        }
        if (key != null) {
            schema.primaryKey(key);
        }
        tEnv.createTemporaryTable(
                name,
                TableDescriptor.forConnector("values")
                        .option("sink-insert-only", "false")
                        .schema(schema.build())
                        .build());
    }

    private static List<String> outputColumns(String query) {
        String select = query.substring("SELECT ".length(), query.indexOf(" FROM "));
        List<String> columns = new ArrayList<>();
        for (String item : select.split(", ")) {
            columns.add(item.substring(item.indexOf('.') + 1));
        }
        return columns;
    }

    /** The join over the final state of the tables, computed by a batch job. */
    private static List<String> expected(Map<String, List<String>> tables, String query)
            throws Exception {
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        for (Map.Entry<String, List<String>> table : tables.entrySet()) {
            Collection<GenericRowData> rows =
                    SharedStoreTableFactory.currentRows(STORE, table.getKey());
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
