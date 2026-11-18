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

package org.apache.flink.table.runtime.operators.join.deltajoin.anchor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupLookup;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOperatorBase;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.LineRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Evaluates an {@link AnchorJoinTree} with lookups: {@link #locate} finds the anchor keys whose
 * groups may contain a row, and {@link #group} computes the lines of the group of an anchor key.
 * Every lookup starts as soon as the row that holds its key is known. Results arrive on the given
 * executor (the mailbox), so generated code runs only in the task thread.
 *
 * <p>An evaluation shares an in-flight lookup with the same key only if that lookup was issued
 * after the evaluation started, so it never reads a state older than its trigger.
 */
final class AnchorJoinEvaluator {

    private final AnchorJoinTree tree;
    private final AnchorLookupSpec[] lookupSpecs;
    private final @Nullable GeneratedJoinCondition[] generatedConditions;
    private final GeneratedFilterCondition[][] generatedFilters;
    private final long timeoutMs;
    private final Executor mailbox;
    private final int maxLookupsInFlight;

    private long clock;
    private int lookupsInFlight;
    private final ArrayDeque<Runnable> waitingLookups = new ArrayDeque<>();
    private long numLookups;
    private long numSharedLookups;

    private transient Lookup[] lookups;
    private transient JoinCondition[] conditions;
    private transient FilterCondition[][] filters;
    private transient LineRowData[] leftViews;
    private transient LineRowData[] rightViews;
    private transient LineRowData[][] filterViews;
    private transient RowData.FieldGetter[] anchorKeyGetters;
    private transient RowDataSerializer anchorKeySerializer;

    /** Reads the fields of an anchor key row. */
    private transient RowData.FieldGetter[] keyRowGetters;

    /** Per table, reads the anchor key from rows of the table before the anchor, or null. */
    private transient RowData.FieldGetter[][] locateKeyGetters;

    /** Per lookup, whether it starts at the anchor table and needs only the anchor key. */
    private transient boolean[] keyedByAnchorKey;

    /** Per table, the lookups into the tables one step further away from the anchor. */
    private transient int[][] lookupsAwayFrom;

    /** Per lookup directed away from the anchor, the table it returns rows of; -1 otherwise. */
    private transient int[] leafOfLookup;

    AnchorJoinEvaluator(
            AnchorJoinTree tree,
            AnchorLookupSpec[] lookupSpecs,
            @Nullable GeneratedJoinCondition[] generatedConditions,
            GeneratedFilterCondition[][] generatedFilters,
            long timeoutMs,
            Executor mailbox,
            int maxLookupsInFlight) {
        this.tree = tree;
        this.lookupSpecs = lookupSpecs;
        this.generatedConditions = generatedConditions;
        this.generatedFilters = generatedFilters;
        this.timeoutMs = timeoutMs;
        this.mailbox = mailbox;
        this.maxLookupsInFlight = maxLookupsInFlight;
    }

    /** A new stamp; an evaluation takes one when it starts. */
    long newStamp() {
        return ++clock;
    }

    /** The number of lookups issued to the storage. */
    long getNumLookups() {
        return numLookups;
    }

    /** The number of lookups answered by an identical lookup in flight. */
    long getNumSharedLookups() {
        return numSharedLookups;
    }

    private void issueOrWait(Runnable lookup) {
        if (lookupsInFlight < maxLookupsInFlight) {
            lookup.run();
        } else {
            waitingLookups.add(lookup);
        }
    }

    private void lookupDone() {
        lookupsInFlight--;
        while (lookupsInFlight < maxLookupsInFlight && !waitingLookups.isEmpty()) {
            waitingLookups.poll().run();
        }
    }

    /**
     * Opens the lookups marked in {@code usedLookups}, and the conditions and filters if {@code
     * evaluatesGroups} is set. Locating anchors needs neither conditions nor filters.
     */
    void open(
            RuntimeContext runtimeContext,
            OpenContext openContext,
            boolean[] usedLookups,
            boolean evaluatesGroups)
            throws Exception {
        ClassLoader cl = runtimeContext.getUserCodeClassLoader();
        lookups = new Lookup[lookupSpecs.length];
        for (int i = 0; i < lookupSpecs.length; i++) {
            if (usedLookups[i]) {
                lookups[i] = new Lookup(lookupSpecs[i]);
                lookups[i].open(runtimeContext, openContext, cl);
            }
        }
        if (evaluatesGroups) {
            openConditionsAndFilters(runtimeContext, openContext, cl);
        }
        RowType anchorType = tree.getLeafType(tree.getAnchorLeaf());
        int[] keyFields = tree.getAnchorKeyFields();
        anchorKeyGetters = new RowData.FieldGetter[keyFields.length];
        keyRowGetters = new RowData.FieldGetter[keyFields.length];
        LogicalType[] keyTypes = new LogicalType[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            keyTypes[i] = anchorType.getTypeAt(keyFields[i]);
            anchorKeyGetters[i] = RowData.createFieldGetter(keyTypes[i], keyFields[i]);
            keyRowGetters[i] = RowData.createFieldGetter(keyTypes[i], i);
        }
        anchorKeySerializer = new RowDataSerializer(keyTypes);

        locateKeyGetters = new RowData.FieldGetter[tree.getNumLeaves()][];
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            int[] fields = tree.getAnchorKeyFieldsBeforeAnchor(leaf);
            if (fields == null) {
                continue;
            }
            RowType type = tree.getLeafType(tree.getTableBeforeAnchor(leaf));
            locateKeyGetters[leaf] = new RowData.FieldGetter[fields.length];
            for (int i = 0; i < fields.length; i++) {
                locateKeyGetters[leaf][i] =
                        RowData.createFieldGetter(type.getTypeAt(fields[i]), fields[i]);
            }
        }

        lookupsAwayFrom = new int[tree.getNumLeaves()][];
        leafOfLookup = new int[lookupSpecs.length];
        Arrays.fill(leafOfLookup, -1);
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            int[] away = tree.getLeavesAwayFromAnchor(leaf);
            lookupsAwayFrom[leaf] = new int[away.length];
            for (int i = 0; i < away.length; i++) {
                lookupsAwayFrom[leaf][i] = tree.getLookupBetween(leaf, away[i]);
                leafOfLookup[lookupsAwayFrom[leaf][i]] = away[i];
            }
        }
        keyedByAnchorKey = new boolean[lookupSpecs.length];
        for (int lookup : lookupsAwayFrom[tree.getAnchorLeaf()]) {
            keyedByAnchorKey[lookup] =
                    usedLookups[lookup]
                            && containsAll(keyFields, lookupSpecs[lookup].getSourceKeyFields());
        }
    }

    private static boolean containsAll(int[] fields, int[] required) {
        for (int r : required) {
            boolean found = false;
            for (int f : fields) {
                found |= f == r;
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private void openConditionsAndFilters(
            RuntimeContext runtimeContext, OpenContext openContext, ClassLoader cl)
            throws Exception {
        int numNodes = tree.getNumNodes();
        conditions = new JoinCondition[numNodes];
        filters = new FilterCondition[numNodes][];
        leftViews = new LineRowData[numNodes];
        rightViews = new LineRowData[numNodes];
        filterViews = new LineRowData[numNodes][];
        for (int n = 0; n < numNodes; n++) {
            AnchorJoinTree.Node node = tree.getNode(n);
            if (!node.isLeaf()) {
                leftViews[n] = tree.getNode(node.getLeft()).getOutput().view();
                rightViews[n] = tree.getNode(node.getRight()).getOutput().view();
                if (generatedConditions[n] != null) {
                    conditions[n] = generatedConditions[n].newInstance(cl);
                    FunctionUtils.setFunctionRuntimeContext(conditions[n], runtimeContext);
                    FunctionUtils.openFunction(conditions[n], openContext);
                }
            }
            GeneratedFilterCondition[] nodeFilters = generatedFilters[n];
            filters[n] = new FilterCondition[nodeFilters.length];
            filterViews[n] = new LineRowData[nodeFilters.length];
            for (int f = 0; f < nodeFilters.length; f++) {
                filters[n][f] = nodeFilters[f].newInstance(cl);
                FunctionUtils.setFunctionRuntimeContext(filters[n][f], runtimeContext);
                FunctionUtils.openFunction(filters[n][f], openContext);
                filterViews[n][f] = node.getFilterInputs().get(f).view();
            }
        }
    }

    void close() throws Exception {
        if (lookups != null) {
            for (Lookup lookup : lookups) {
                if (lookup != null) {
                    lookup.close();
                }
            }
        }
        if (conditions != null) {
            for (JoinCondition condition : conditions) {
                if (condition != null) {
                    FunctionUtils.closeFunction(condition);
                }
            }
        }
        if (filters != null) {
            for (FilterCondition[] nodeFilters : filters) {
                if (nodeFilters == null) {
                    continue;
                }
                for (FilterCondition filter : nodeFilters) {
                    FunctionUtils.closeFunction(filter);
                }
            }
        }
    }

    /** The key of an anchor row, as a binary row. */
    BinaryRowData anchorKeyOf(RowData anchorRow) {
        GenericRowData key = new GenericRowData(anchorKeyGetters.length);
        for (int i = 0; i < anchorKeyGetters.length; i++) {
            key.setField(i, anchorKeyGetters[i].getFieldOrNull(anchorRow));
        }
        return anchorKeySerializer.toBinaryRow(key, true);
    }

    /** The keys of the anchor rows connected to the given row of the given table. */
    CompletableFuture<Collection<BinaryRowData>> locate(int leaf, RowData row, long start) {
        CompletableFuture<List<RowData>> rows =
                CompletableFuture.completedFuture(Collections.singletonList(row));
        for (int lookupId : tree.getLocateLookups(leaf)) {
            Lookup lookup = lookups[lookupId];
            rows =
                    rows.thenCompose(
                            current -> {
                                List<CompletableFuture<List<RowData>>> next =
                                        new ArrayList<>(current.size());
                                for (RowData r : current) {
                                    next.add(lookup.lookup(r, start));
                                }
                                return flatten(next);
                            });
        }
        RowData.FieldGetter[] keyGetters = locateKeyGetters[leaf];
        return rows.thenApply(
                found -> {
                    Set<BinaryRowData> keys = new LinkedHashSet<>();
                    for (RowData r : found) {
                        BinaryRowData key =
                                keyGetters == null ? anchorKeyOf(r) : keyOf(r, keyGetters);
                        if (key != null) {
                            keys.add(key);
                        }
                    }
                    return keys;
                });
    }

    /** The anchor key read from a row with the given getters, or null if a field is NULL. */
    private @Nullable BinaryRowData keyOf(RowData row, RowData.FieldGetter[] getters) {
        GenericRowData key = new GenericRowData(getters.length);
        for (int i = 0; i < getters.length; i++) {
            Object value = getters[i].getFieldOrNull(row);
            if (value == null) {
                // NULL never equals anything
                return null;
            }
            key.setField(i, value);
        }
        return anchorKeySerializer.toBinaryRow(key, true);
    }

    /** A row of the anchor table that holds only the given anchor key. */
    private RowData anchorRowOfKey(RowData anchorKey) {
        int[] keyFields = tree.getAnchorKeyFields();
        GenericRowData row =
                new GenericRowData(tree.getLeafType(tree.getAnchorLeaf()).getFieldCount());
        for (int i = 0; i < keyFields.length; i++) {
            row.setField(keyFields[i], keyRowGetters[i].getFieldOrNull(anchorKey));
        }
        return row;
    }

    /**
     * All lines that contain the anchor row with the given key; the rows of the tables in {@code
     * known}, read by {@link #openPages}, stand in for their lookups.
     */
    CompletableFuture<List<RowData[]>> group(
            RowData anchorKey, long start, Map<Integer, List<RowData>> known) {
        return evaluate(anchorKey, new Evaluation(start, null, knownLookups(known)));
    }

    /**
     * The lines of the group of the given key that contain given rows of one table, which must be
     * joined to the anchor table directly (see {@link AnchorJoinTree#canRefreshRowsOf}). The given
     * rows stand in for the table, which is never looked up: {@code latest} holds the current
     * versions of changed rows, {@code older} versions of them that the group may have emitted.
     *
     * <p>Lines of the anchor's side of the join that adds the table are called side lines. For
     * every side line, the result tells which of the versions matched it, and holds the lines it
     * forms with older versions and, if the join preserves the anchor's side, its padded lines: the
     * lines it would form if no row of the table matched it.
     */
    CompletableFuture<ScopedLines> linesOfRows(
            RowData anchorKey, long start, int leaf, List<RowData> latest, List<RowData> older) {
        return linesOfRows(anchorKey, start, leaf, latest, older, Collections.emptyMap());
    }

    /**
     * As {@link #linesOfRows(RowData, long, int, List, List)}; the rows of the tables in {@code
     * known}, read by {@link #openPages}, stand in for their lookups.
     */
    CompletableFuture<ScopedLines> linesOfRows(
            RowData anchorKey,
            long start,
            int leaf,
            List<RowData> latest,
            List<RowData> older,
            Map<Integer, List<RowData>> known) {
        RowScope scope = new RowScope(leaf, latest, older);
        return evaluate(anchorKey, new Evaluation(start, scope, knownLookups(known)))
                .thenApply(scope::classify);
    }

    /**
     * Whether the rows of the given table for one anchor key can be read in pages: the table is
     * joined to the anchor table directly, keyed by the anchor key, and its lookup reads in pages.
     */
    boolean readsInPages(int leaf) {
        if (!tree.canRefreshRowsOf(leaf)) {
            return false;
        }
        int lookupId = tree.getLookupBetween(tree.getAnchorLeaf(), leaf);
        return lookups[lookupId] != null
                && lookups[lookupId].pagedFunction != null
                && keyedByAnchorKey[lookupId]
                && conditions[tree.getJoinTowardsAnchor(leaf)] != null;
    }

    /** Opens the pages of the rows of the given table that join the anchor row with the key. */
    TablePages openPages(RowData anchorKey, int leaf, int pageSize) {
        Preconditions.checkArgument(readsInPages(leaf), "Table %s is not read in pages.", leaf);
        Lookup lookup = lookups[tree.getLookupBetween(tree.getAnchorLeaf(), leaf)];
        return lookup.openPages(anchorRowOfKey(anchorKey), pageSize);
    }

    /** The given rows of tables by the lookups into them from the anchor table. */
    private Map<Integer, List<RowData>> knownLookups(Map<Integer, List<RowData>> known) {
        Map<Integer, List<RowData>> byLookup = new HashMap<>();
        for (Map.Entry<Integer, List<RowData>> entry : known.entrySet()) {
            byLookup.put(
                    tree.getLookupBetween(tree.getAnchorLeaf(), entry.getKey()), entry.getValue());
        }
        return byLookup;
    }

    private CompletableFuture<List<RowData[]>> evaluate(RowData anchorKey, Evaluation evaluation) {
        long start = evaluation.start;
        int anchorLeaf = tree.getAnchorLeaf();
        // first, so that it never waits for capacity behind lookups that depend on it
        CompletableFuture<List<RowData>> anchorRows =
                lookups[tree.getAnchorLookup()].lookup(anchorKey, start);
        RowData keyOnly = null;
        for (int lookupId : lookupsAwayFrom[anchorLeaf]) {
            if (keyedByAnchorKey[lookupId]) {
                // needs only the anchor key: starts together with the lookup of the anchor row
                keyOnly = keyOnly == null ? anchorRowOfKey(anchorKey) : keyOnly;
                evaluation.lookup(lookupId, keyOnly);
            }
        }
        return anchorRows.thenCompose(
                anchors -> {
                    List<CompletableFuture<List<RowData[]>>> groups =
                            new ArrayList<>(anchors.size());
                    for (RowData anchor : anchors) {
                        evaluation.startAwayFrom(anchorLeaf, anchor);
                    }
                    for (RowData anchor : anchors) {
                        groups.add(
                                linesContaining(
                                        tree.getRootNode(), anchorLeaf, anchor, evaluation));
                    }
                    return flatten(groups);
                });
    }

    /**
     * The lines of the given node that contain the given row of the given table.
     *
     * <p>At a join, the lines of the side holding the table are extended with the matching lines of
     * the other side, found by looking up the other side's edge table and evaluating the other side
     * outwards from every row found. A line without a match is kept, padded with NULLs, if its side
     * is preserved by the join.
     */
    private CompletableFuture<List<RowData[]>> linesContaining(
            int nodeId, int leaf, RowData row, Evaluation evaluation) {
        AnchorJoinTree.Node node = tree.getNode(nodeId);
        if (node.isLeaf()) {
            RowData[] line = new RowData[tree.getNumLeaves()];
            line[leaf] = row;
            return CompletableFuture.completedFuture(Collections.singletonList(line));
        }
        boolean inLeft = tree.getNode(node.getLeft()).containsLeaf(leaf);
        int sideNode = inLeft ? node.getLeft() : node.getRight();
        int otherNode = inLeft ? node.getRight() : node.getLeft();
        AnchorJoinTree.Edge edge = tree.getEdge(node.getEdge());
        int sideLeaf = inLeft ? edge.getLeftLeaf() : edge.getRightLeaf();
        int otherLeaf = inLeft ? edge.getRightLeaf() : edge.getLeftLeaf();
        int lookupId = inLeft ? edge.getLookupRight() : edge.getLookupLeft();
        boolean preserved =
                inLeft
                        ? node.getJoinType() == FlinkJoinType.LEFT
                        : node.getJoinType() == FlinkJoinType.RIGHT;

        return linesContaining(sideNode, leaf, row, evaluation)
                .thenCompose(
                        partials -> {
                            List<CompletableFuture<List<RowData[]>>> extended =
                                    new ArrayList<>(partials.size());
                            for (RowData[] partial : partials) {
                                extended.add(
                                        extend(
                                                nodeId,
                                                partial,
                                                partial[sideLeaf],
                                                lookupId,
                                                otherNode,
                                                otherLeaf,
                                                preserved,
                                                evaluation));
                            }
                            return flatten(extended);
                        })
                .thenApply(lines -> applyFilters(nodeId, lines));
    }

    private CompletableFuture<List<RowData[]>> extend(
            int nodeId,
            RowData[] partial,
            @Nullable RowData keySource,
            int lookupId,
            int otherNode,
            int otherLeaf,
            boolean preserved,
            Evaluation evaluation) {
        RowScope scope = evaluation.scope;
        boolean scoped = scope != null && scope.node == nodeId && scope.lookupId == lookupId;
        CompletableFuture<List<RowData[]>> candidates;
        if (keySource == null) {
            // the edge table of this side was padded, so its join key is NULL
            candidates = CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            candidates =
                    evaluation
                            .lookup(lookupId, keySource)
                            .thenCompose(
                                    rows -> {
                                        List<CompletableFuture<List<RowData[]>>> others =
                                                new ArrayList<>(rows.size());
                                        for (RowData other : rows) {
                                            others.add(
                                                    linesContaining(
                                                            otherNode,
                                                            otherLeaf,
                                                            other,
                                                            evaluation));
                                        }
                                        return flatten(others);
                                    });
        }
        return candidates.thenApply(
                others -> {
                    List<RowData[]> result = new ArrayList<>();
                    for (RowData[] other : others) {
                        RowData[] merged = merge(partial, other);
                        if (matches(nodeId, merged)) {
                            result.add(merged);
                        }
                    }
                    if (scoped) {
                        scope.matched(partial, result);
                        if (preserved) {
                            // whether it is emitted depends on rows that are not read; it is told
                            // apart from the others by the missing row of the table
                            result.add(partial);
                        }
                        return result;
                    }
                    if (result.isEmpty() && preserved) {
                        result.add(partial);
                    }
                    return result;
                });
    }

    private boolean matches(int nodeId, RowData[] line) {
        JoinCondition condition = conditions[nodeId];
        return condition == null
                || condition.apply(
                        leftViews[nodeId].replace(line), rightViews[nodeId].replace(line));
    }

    private List<RowData[]> applyFilters(int nodeId, List<RowData[]> lines) {
        FilterCondition[] nodeFilters = filters[nodeId];
        if (nodeFilters.length == 0) {
            return lines;
        }
        List<RowData[]> result = new ArrayList<>(lines.size());
        for (RowData[] line : lines) {
            boolean keep = true;
            for (int f = 0; f < nodeFilters.length && keep; f++) {
                keep =
                        nodeFilters[f].apply(
                                FilterCondition.Context.INVALID_CONTEXT,
                                filterViews[nodeId][f].replace(line));
            }
            if (keep) {
                result.add(line);
            }
        }
        return result;
    }

    private static RowData[] merge(RowData[] a, RowData[] b) {
        RowData[] merged = a.clone();
        for (int i = 0; i < b.length; i++) {
            if (b[i] != null) {
                merged[i] = b[i];
            }
        }
        return merged;
    }

    private static <T> CompletableFuture<List<T>> flatten(List<CompletableFuture<List<T>>> parts) {
        if (parts.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        if (parts.size() == 1) {
            return parts.get(0);
        }
        return CompletableFuture.allOf(parts.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            List<T> all = new ArrayList<>();
                            for (CompletableFuture<List<T>> part : parts) {
                                all.addAll(part.join());
                            }
                            return all;
                        });
    }

    /**
     * The lookups of one evaluation of a group. A lookup starts once and its result serves every
     * request of the evaluation with the same key, also after it completed. When a lookup into a
     * table completes, the lookups into the tables beyond it start for every row found, before the
     * evaluation asks for them.
     */
    private final class Evaluation {

        private final long start;
        private final @Nullable RowScope scope;

        /** Per lookup keyed by the anchor key, its rows if they are known already. */
        private final Map<Integer, List<RowData>> known;

        private final Set<Integer> givenStarted = new HashSet<>();
        private final Map<Integer, Map<BinaryRowData, CompletableFuture<List<RowData>>>> results =
                new HashMap<>();

        private Evaluation(long start, @Nullable RowScope scope) {
            this(start, scope, Collections.emptyMap());
        }

        private Evaluation(
                long start, @Nullable RowScope scope, Map<Integer, List<RowData>> known) {
            this.start = start;
            this.scope = scope;
            this.known = known;
        }

        /** The rows of the given lookup for the key in {@code source}. */
        CompletableFuture<List<RowData>> lookup(int lookupId, RowData source) {
            Lookup lookup = lookups[lookupId];
            BinaryRowData key = lookup.keyOrNull(source);
            if (key == null) {
                // NULL never equals anything
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            List<RowData> given =
                    scope != null && lookupId == scope.lookupId
                            ? scope.versions
                            : known.get(lookupId);
            if (given != null) {
                // the given rows stand in for the table; the join condition, which holds the
                // equality columns, keeps those that match
                if (givenStarted.add(lookupId)) {
                    for (RowData row : given) {
                        startAwayFrom(leafOfLookup[lookupId], row);
                    }
                }
                return CompletableFuture.completedFuture(given);
            }
            Map<BinaryRowData, CompletableFuture<List<RowData>>> byKey =
                    results.computeIfAbsent(lookupId, id -> new HashMap<>());
            CompletableFuture<List<RowData>> result = byKey.get(key);
            if (result == null) {
                result = lookup.lookup(source, key, start);
                byKey.put(key, result);
                int leaf = leafOfLookup[lookupId];
                if (leaf >= 0) {
                    result.thenAccept(
                            rows -> {
                                for (RowData row : rows) {
                                    startAwayFrom(leaf, row);
                                }
                            });
                }
            }
            return result;
        }

        /** Starts the lookups into the tables one step further away from the anchor than a row. */
        void startAwayFrom(int leaf, RowData row) {
            for (int lookupId : lookupsAwayFrom[leaf]) {
                lookup(lookupId, row);
            }
        }
    }

    /** What {@link #linesOfRows} evaluates, and what it learns at the join that adds the table. */
    private final class RowScope {

        private final int leaf;
        private final int node;
        private final int lookupId;
        private final boolean preserved;

        /** The tables of the anchor's side of the join. */
        private final boolean[] sideLeaves;

        private final List<RowData> versions;
        private final Set<RowData> latest = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Map<SideKey, Side> sides = new LinkedHashMap<>();

        private RowScope(int leaf, List<RowData> latest, List<RowData> older) {
            Preconditions.checkArgument(
                    tree.canRefreshRowsOf(leaf), "Table %s needs whole groups.", leaf);
            this.leaf = leaf;
            this.node = tree.getJoinTowardsAnchor(leaf);
            Preconditions.checkState(
                    conditions[node] != null, "The join of table %s has no condition.", leaf);
            AnchorJoinTree.Node join = tree.getNode(node);
            AnchorJoinTree.Node left = tree.getNode(join.getLeft());
            boolean anchorInLeft = left.containsLeaf(tree.getAnchorLeaf());
            AnchorJoinTree.Node side = anchorInLeft ? left : tree.getNode(join.getRight());
            this.preserved =
                    anchorInLeft
                            ? join.getJoinType() == FlinkJoinType.LEFT
                            : join.getJoinType() == FlinkJoinType.RIGHT;
            this.lookupId = tree.getLookupBetween(tree.getAnchorLeaf(), leaf);
            this.sideLeaves = new boolean[tree.getNumLeaves()];
            for (int i = 0; i < sideLeaves.length; i++) {
                sideLeaves[i] = side.containsLeaf(i);
            }
            this.versions = new ArrayList<>(latest.size() + older.size());
            versions.addAll(latest);
            versions.addAll(older);
            this.latest.addAll(latest);
        }

        /** The side line {@code partial} formed the given lines with the versions. */
        private void matched(RowData[] partial, List<RowData[]> lines) {
            Side side = sides.computeIfAbsent(new SideKey(partial, sideLeaves), k -> new Side());
            for (RowData[] line : lines) {
                if (latest.contains(line[leaf])) {
                    side.matchedByLatest = true;
                } else {
                    side.matchedByOlder = true;
                }
            }
        }

        private ScopedLines classify(List<RowData[]> lines) {
            ScopedLines result = new ScopedLines(preserved);
            for (RowData[] line : lines) {
                RowData row = line[leaf];
                if (row != null && latest.contains(row)) {
                    result.latest.add(line);
                    continue;
                }
                Side side = sides.get(new SideKey(line, sideLeaves));
                Preconditions.checkState(side != null, "A line without its side line.");
                if (row == null) {
                    side.padded.add(line);
                } else {
                    side.older.add(line);
                }
            }
            result.sides.addAll(sides.values());
            return result;
        }
    }

    /** The rows of a side line, compared by identity: the lines of one evaluation share rows. */
    private static final class SideKey {
        private final RowData[] rows;
        private final int hash;

        private SideKey(RowData[] line, boolean[] sideLeaves) {
            this.rows = new RowData[line.length];
            int h = 1;
            for (int i = 0; i < line.length; i++) {
                if (sideLeaves[i]) {
                    rows[i] = line[i];
                }
                h = 31 * h + System.identityHashCode(rows[i]);
            }
            this.hash = h;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SideKey)) {
                return false;
            }
            RowData[] other = ((SideKey) o).rows;
            for (int i = 0; i < rows.length; i++) {
                if (rows[i] != other[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** The result of {@link #linesOfRows}. */
    static final class ScopedLines {
        /** Whether the join that adds the table keeps side lines that match no row of it. */
        final boolean preservesSides;

        /** The lines with the current versions. */
        final List<RowData[]> latest = new ArrayList<>();

        final List<Side> sides = new ArrayList<>();

        private ScopedLines(boolean preservesSides) {
            this.preservesSides = preservesSides;
        }
    }

    /** What the versions did to one side line. */
    static final class Side {
        boolean matchedByLatest;
        boolean matchedByOlder;

        /** The lines of the side line with older versions. */
        final List<RowData[]> older = new ArrayList<>();

        /** The lines of the side line if no row of the table matched it. */
        final List<RowData[]> padded = new ArrayList<>();
    }

    /** The pages of the rows of one table for one key. Used in the mailbox thread only. */
    final class TablePages implements GroupRefreshOperatorBase.RowPages {
        private final Lookup lookup;
        private final SupportsPagedLookup.LookupCursor cursor;
        private boolean closed;

        private TablePages(Lookup lookup, SupportsPagedLookup.LookupCursor cursor) {
            this.lookup = lookup;
            this.cursor = cursor;
        }

        @Override
        public void next(BiConsumer<List<RowData>, Boolean> onRows, Consumer<Throwable> onError) {
            CompletableFuture<SupportsPagedLookup.LookupPage> page;
            try {
                page = cursor.nextPage();
            } catch (Throwable t) {
                close();
                onError.accept(t);
                return;
            }
            if (timeoutMs > 0) {
                page = page.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
            }
            page.whenComplete(
                    (rows, error) ->
                            mailbox.execute(
                                    () -> {
                                        if (error != null) {
                                            close();
                                            onError.accept(lookup.wrap(error, "A page of"));
                                            return;
                                        }
                                        List<RowData> result;
                                        try {
                                            result = lookup.pageRows(rows.getRows());
                                        } catch (Throwable t) {
                                            close();
                                            onError.accept(t);
                                            return;
                                        }
                                        if (rows.isLast()) {
                                            close();
                                        }
                                        onRows.accept(result, rows.isLast());
                                    }));
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                cursor.close();
            }
        }
    }

    /** A lookup requested at {@code stamp}; its result is shared by later identical requests. */
    private static final class InFlight {
        final long stamp;
        final CompletableFuture<List<RowData>> result = new CompletableFuture<>();

        InFlight(long stamp) {
            this.stamp = stamp;
        }
    }

    /** A lookup whose result is delivered on the mailbox thread. */
    private final class Lookup {

        private final AnchorLookupSpec spec;

        private AsyncFunction<RowData, Object> fetcher;
        private DataStructureConverter<Object, Object> converter;
        private @Nullable FlatMapFunction<RowData, RowData> calc;
        private RowDataSerializer resultSerializer;
        private RowData.FieldGetter[] keyGetters;
        private RowDataSerializer keySerializer;
        private final Map<BinaryRowData, InFlight> inFlight = new HashMap<>();

        /** The lookup function of the fetcher if it reads in pages. */
        private @Nullable SupportsPagedLookup pagedFunction;

        private Lookup(AnchorLookupSpec spec) {
            this.spec = spec;
        }

        void open(RuntimeContext runtimeContext, OpenContext openContext, ClassLoader cl)
                throws Exception {
            fetcher = spec.getFetcher().newInstance(cl);
            FunctionUtils.setFunctionRuntimeContext(fetcher, runtimeContext);
            FunctionUtils.openFunction(fetcher, openContext);
            converter = DataStructureConverters.getConverter(spec.getFetcherResultType());
            converter.open(cl);
            if (spec.getCalc() != null) {
                calc = spec.getCalc().newInstance(cl);
                FunctionUtils.setFunctionRuntimeContext(calc, runtimeContext);
                FunctionUtils.openFunction(calc, openContext);
            }
            resultSerializer = new RowDataSerializer(spec.getResultType());
            int[] keyFields = spec.getSourceKeyFields();
            LogicalType[] keyTypes = spec.getSourceKeyTypes();
            keyGetters = new RowData.FieldGetter[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                keyGetters[i] = RowData.createFieldGetter(keyTypes[i], keyFields[i]);
            }
            keySerializer = new RowDataSerializer(keyTypes);
            if (spec.isPaged()) {
                pagedFunction = GroupLookup.pagedFunctionOf(spec.getFetcher());
            }
        }

        /** Opens the pages of the rows that match the key of {@code source}. */
        TablePages openPages(RowData source, int pageSize) {
            GenericRowData keyRow = new GenericRowData(keyGetters.length);
            for (int i = 0; i < keyGetters.length; i++) {
                keyRow.setField(i, keyGetters[i].getFieldOrNull(source));
            }
            SupportsPagedLookup.LookupCursor cursor = pagedFunction.openCursor(keyRow, pageSize);
            return new TablePages(this, cursor);
        }

        /** The rows of a page after the projection and filters of the table. */
        private List<RowData> pageRows(Collection<RowData> rows) throws Exception {
            List<RowData> result = new ArrayList<>(rows.size());
            Collector<RowData> collector =
                    new Collector<RowData>() {
                        @Override
                        public void collect(RowData record) {
                            result.add(resultSerializer.copy(record));
                        }

                        @Override
                        public void close() {}
                    };
            for (RowData row : rows) {
                if (calc == null) {
                    collector.collect(row);
                } else {
                    calc.flatMap(row, collector);
                }
            }
            return result;
        }

        private BinaryRowData keyOf(RowData source) {
            GenericRowData key = new GenericRowData(keyGetters.length);
            for (int i = 0; i < keyGetters.length; i++) {
                key.setField(i, keyGetters[i].getFieldOrNull(source));
            }
            return keySerializer.toBinaryRow(key, true);
        }

        void close() throws Exception {
            if (fetcher != null) {
                FunctionUtils.closeFunction(fetcher);
            }
            if (calc != null) {
                FunctionUtils.closeFunction(calc);
            }
        }

        /**
         * Looks up the rows matching the key of {@code source}, for an evaluation that started at
         * {@code start}.
         */
        CompletableFuture<List<RowData>> lookup(RowData source, long start) {
            BinaryRowData key = keyOrNull(source);
            if (key == null) {
                // NULL never equals anything
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            return lookup(source, key, start);
        }

        /** The key of {@code source}, or null if a key field is NULL. */
        @Nullable
        BinaryRowData keyOrNull(RowData source) {
            for (int keyField : spec.getSourceKeyFields()) {
                if (source.isNullAt(keyField)) {
                    return null;
                }
            }
            return keyOf(source);
        }

        /** Looks up the rows matching {@code key}, the key of {@code source}. */
        CompletableFuture<List<RowData>> lookup(RowData source, BinaryRowData key, long start) {
            InFlight shared = inFlight.get(key);
            if (shared != null && shared.stamp >= start) {
                numSharedLookups++;
                return shared.result;
            }
            InFlight request = new InFlight(newStamp());
            inFlight.put(key, request);
            issueOrWait(() -> invoke(source, key, request));
            return request.result;
        }

        private void invoke(RowData source, BinaryRowData key, InFlight request) {
            lookupsInFlight++;
            numLookups++;
            CompletableFuture<Collection<Object>> raw = new CompletableFuture<>();
            try {
                fetcher.asyncInvoke(
                        source,
                        new ResultFuture<Object>() {
                            @Override
                            public void complete(Collection<Object> result) {
                                raw.complete(result == null ? Collections.emptyList() : result);
                            }

                            @Override
                            public void completeExceptionally(Throwable error) {
                                raw.completeExceptionally(error);
                            }

                            @Override
                            public void complete(CollectionSupplier<Object> supplier) {
                                // a supplier must be evaluated on the mailbox thread
                                mailbox.execute(
                                        () -> {
                                            try {
                                                complete(supplier.get());
                                            } catch (Throwable e) {
                                                raw.completeExceptionally(e);
                                            }
                                        });
                            }
                        });
            } catch (Throwable t) {
                raw.completeExceptionally(t);
            }
            if (timeoutMs > 0) {
                raw.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
            }
            CompletableFuture<List<RowData>> result = request.result;
            raw.whenComplete(
                    (rows, error) ->
                            mailbox.execute(
                                    () -> {
                                        if (inFlight.get(key) == request) {
                                            inFlight.remove(key);
                                        }
                                        lookupDone();
                                        if (error != null) {
                                            result.completeExceptionally(
                                                    wrap(error, "The lookup of"));
                                            return;
                                        }
                                        try {
                                            result.complete(toResultRows(rows));
                                        } catch (Throwable t) {
                                            result.completeExceptionally(t);
                                        }
                                    }));
        }

        private Throwable wrap(Throwable error, String what) {
            if (!(error instanceof TimeoutException)) {
                return error;
            }
            TimeoutException timeout =
                    new TimeoutException(
                            String.format(
                                    "%s %s did not complete within %d ms, see '%s'.",
                                    what,
                                    spec.getDescription(),
                                    timeoutMs,
                                    ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT.key()));
            timeout.initCause(error);
            return timeout;
        }

        private List<RowData> toResultRows(Collection<Object> rows) throws Exception {
            List<RowData> result = new ArrayList<>(rows.size());
            Collector<RowData> collector =
                    new Collector<RowData>() {
                        @Override
                        public void collect(RowData record) {
                            result.add(resultSerializer.copy(record));
                        }

                        @Override
                        public void close() {}
                    };
            for (Object row : rows) {
                RowData internal = (RowData) converter.toInternal(row);
                if (calc == null) {
                    collector.collect(internal);
                } else {
                    calc.flatMap(internal, collector);
                }
            }
            return result;
        }
    }
}
