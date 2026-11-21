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
import org.apache.flink.table.connector.source.lookup.SupportsVersionedLookup;
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
import org.apache.flink.types.RowKind;
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
import java.util.TreeMap;
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
    private long numCachedReads;

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

    /** Per table, reads the anchor key from rows of its locate table, or null. */
    private transient RowData.FieldGetter[][] locateKeyGetters;

    /** Per lookup into a table read per group, reads the lookup key from its rows; else null. */
    private transient RowData.FieldGetter[][] groupIndexGetters;

    /** Per lookup in {@link #groupIndexGetters}, the serializer of its keys. */
    private transient RowDataSerializer[] groupIndexSerializers;

    /** Per lookup, whether it starts at the anchor table and needs only the anchor key. */
    private transient boolean[] keyedByAnchorKey;

    /** Per table, the lookups into the tables one step further away from the anchor. */
    private transient int[][] lookupsAwayFrom;

    /** Per lookup directed away from the anchor, the table it returns rows of; -1 otherwise. */
    private transient int[] leafOfLookup;

    /** Per lookup, the table it returns rows of. */
    private transient int[] targetOfLookup;

    /** Per node, the join above it; -1 for the root. */
    private transient int[] parentNodes;

    /** Per table, its node. */
    private transient int[] leafNodes;

    /** Per table, the serializer of its rows; to compare lines read by different evaluations. */
    private transient RowDataSerializer[] leafSerializers;

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

    /** The number of reads as of an epoch answered by an earlier read of the same epoch. */
    long getNumCachedReads() {
        return numCachedReads;
    }

    /** Drops the kept reads as of the epochs before the given one. */
    void evictReadsBefore(long epoch) {
        if (lookups == null) {
            return;
        }
        for (Lookup lookup : lookups) {
            if (lookup != null) {
                lookup.readsByEpoch.headMap(epoch, false).clear();
            }
        }
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
            int table = tree.getLocateTable(leaf);
            if (table < 0) {
                continue;
            }
            int[] fields = tree.getAnchorKeyFieldsOf(table);
            RowType type = tree.getLeafType(table);
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
        targetOfLookup = new int[lookupSpecs.length];
        Arrays.fill(targetOfLookup, -1);
        targetOfLookup[tree.getAnchorLookup()] = tree.getAnchorLeaf();
        for (int e = 0; e < tree.getNumLeaves() - 1; e++) {
            AnchorJoinTree.Edge edge = tree.getEdge(e);
            targetOfLookup[edge.getLookupRight()] = edge.getRightLeaf();
            targetOfLookup[edge.getLookupLeft()] = edge.getLeftLeaf();
        }
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            if (tree.getGroupLookup(leaf) >= 0) {
                targetOfLookup[tree.getGroupLookup(leaf)] = leaf;
            }
        }
        parentNodes = new int[tree.getNumNodes()];
        Arrays.fill(parentNodes, -1);
        leafNodes = new int[tree.getNumLeaves()];
        for (int n = 0; n < tree.getNumNodes(); n++) {
            AnchorJoinTree.Node node = tree.getNode(n);
            if (node.isLeaf()) {
                leafNodes[node.getLeaf()] = n;
            } else {
                parentNodes[node.getLeft()] = n;
                parentNodes[node.getRight()] = n;
            }
        }
        leafSerializers = new RowDataSerializer[tree.getNumLeaves()];
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            leafSerializers[leaf] = new RowDataSerializer(tree.getLeafType(leaf));
        }
        keyedByAnchorKey = new boolean[lookupSpecs.length];
        for (int lookup : lookupsAwayFrom[tree.getAnchorLeaf()]) {
            keyedByAnchorKey[lookup] =
                    usedLookups[lookup]
                            && containsAll(keyFields, lookupSpecs[lookup].getSourceKeyFields());
        }
        groupIndexGetters = new RowData.FieldGetter[lookupSpecs.length][];
        groupIndexSerializers = new RowDataSerializer[lookupSpecs.length];
        for (int e = 0; e < tree.getNumLeaves() - 1; e++) {
            AnchorJoinTree.Edge edge = tree.getEdge(e);
            indexGroupRows(
                    edge.getLookupRight(),
                    edge.getLeftFields(),
                    edge.getRightLeaf(),
                    edge.getRightFields());
            indexGroupRows(
                    edge.getLookupLeft(),
                    edge.getRightFields(),
                    edge.getLeftLeaf(),
                    edge.getLeftFields());
        }
    }

    /** Prepares serving a lookup from the group read of its table, if the edge maps every key. */
    private void indexGroupRows(int lookupId, int[] sourceFields, int leaf, int[] leafFields) {
        int groupLookup = tree.getGroupLookup(leaf);
        if (leafOfLookup[lookupId] != leaf
                || groupLookup < 0
                || lookups[lookupId] == null
                || lookups[groupLookup] == null) {
            return;
        }
        int[] keyFields = lookupSpecs[lookupId].getSourceKeyFields();
        LogicalType[] keyTypes = lookupSpecs[lookupId].getSourceKeyTypes();
        RowType leafType = tree.getLeafType(leaf);
        RowData.FieldGetter[] getters = new RowData.FieldGetter[keyFields.length];
        for (int k = 0; k < keyFields.length; k++) {
            int field = -1;
            for (int i = 0; i < sourceFields.length && field < 0; i++) {
                if (sourceFields[i] == keyFields[k]) {
                    field = leafFields[i];
                }
            }
            if (field < 0 || !leafType.getTypeAt(field).copy(true).equals(keyTypes[k].copy(true))) {
                // the rows are looked up one by one
                return;
            }
            getters[k] = RowData.createFieldGetter(leafType.getTypeAt(field), field);
        }
        groupIndexGetters[lookupId] = getters;
        groupIndexSerializers[lookupId] = new RowDataSerializer(keyTypes);
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

    /** Whether locating anchors for rows of the given table reads a lookup of its path in pages. */
    boolean locatesInPages(int leaf) {
        for (int lookupId : tree.getLocateLookups(leaf)) {
            if (lookups[lookupId] != null && lookups[lookupId].pagedFunction != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * As {@link #locate}, reading paged lookups page by page and handing keys to {@code onKeys} as
     * found, so a row with many anchors never holds them all. Keys can repeat.
     */
    CompletableFuture<Void> locateInPages(
            int leaf,
            RowData row,
            long start,
            int pageSize,
            Consumer<Collection<BinaryRowData>> onKeys) {
        return locateFrom(tree.getLocateLookups(leaf), 0, row, start, pageSize, leaf, onKeys);
    }

    private CompletableFuture<Void> locateFrom(
            int[] path,
            int hop,
            RowData row,
            long start,
            int pageSize,
            int leaf,
            Consumer<Collection<BinaryRowData>> onKeys) {
        if (hop == path.length) {
            RowData.FieldGetter[] keyGetters = locateKeyGetters[leaf];
            BinaryRowData key = keyGetters == null ? anchorKeyOf(row) : keyOf(row, keyGetters);
            if (key != null) {
                onKeys.accept(Collections.singletonList(key));
            }
            return CompletableFuture.completedFuture(null);
        }
        Lookup lookup = lookups[path[hop]];
        if (lookup.pagedFunction == null) {
            return lookup.lookup(row, start)
                    .thenCompose(
                            rows -> {
                                List<CompletableFuture<Void>> located =
                                        new ArrayList<>(rows.size());
                                for (RowData r : rows) {
                                    located.add(
                                            locateFrom(
                                                    path, hop + 1, r, start, pageSize, leaf,
                                                    onKeys));
                                }
                                return CompletableFuture.allOf(
                                        located.toArray(new CompletableFuture[0]));
                            });
        }
        CompletableFuture<Void> done = new CompletableFuture<>();
        TablePages pages;
        try {
            pages = lookup.openPages(row, pageSize);
        } catch (Throwable t) {
            done.completeExceptionally(t);
            return done;
        }
        locatePages(pages, path, hop, start, pageSize, leaf, onKeys, done);
        return done;
    }

    /** Locates the rows of the next page, then the page after it, until the last one. */
    private void locatePages(
            TablePages pages,
            int[] path,
            int hop,
            long start,
            int pageSize,
            int leaf,
            Consumer<Collection<BinaryRowData>> onKeys,
            CompletableFuture<Void> done) {
        pages.next(
                (rows, last) -> {
                    List<CompletableFuture<Void>> located = new ArrayList<>(rows.size());
                    for (RowData r : rows) {
                        located.add(locateFrom(path, hop + 1, r, start, pageSize, leaf, onKeys));
                    }
                    CompletableFuture.allOf(located.toArray(new CompletableFuture[0]))
                            .whenComplete(
                                    (ignored, error) ->
                                            mailbox.execute(
                                                    () -> {
                                                        if (error != null) {
                                                            pages.close();
                                                            done.completeExceptionally(error);
                                                        } else if (last) {
                                                            done.complete(null);
                                                        } else {
                                                            locatePages(
                                                                    pages, path, hop, start,
                                                                    pageSize, leaf, onKeys, done);
                                                        }
                                                    }));
                },
                done::completeExceptionally);
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

    /**
     * The lines that a change of one row can alter, at two cuts that differ only in the row's
     * table; {@code before} and {@code after} are the row at either cut, null where it is absent.
     * Returns the lines at the first cut, then at the second.
     *
     * <p>Besides the lines holding the row, a padded line appears or disappears where its preserved
     * line loses or finds its last match, so every preserved line that a changed line matched at
     * either cut is checked at both. Lines that did not change come out equal at both cuts.
     */
    CompletableFuture<List<List<RowData[]>>> deltaLines(
            int leaf,
            @Nullable RowData before,
            @Nullable RowData after,
            long[] epochsBefore,
            long[] epochsAfter) {
        return deltaLines(
                leaf,
                before == null ? Collections.emptyList() : Collections.singletonList(before),
                after == null ? Collections.emptyList() : Collections.singletonList(after),
                epochsBefore,
                epochsAfter);
    }

    /** As {@link #deltaLines(int, RowData, RowData, long[], long[])} for several rows at once. */
    @SuppressWarnings("unchecked")
    CompletableFuture<List<List<RowData[]>>> deltaLines(
            int leaf,
            List<RowData> befores,
            List<RowData> afters,
            long[] epochsBefore,
            long[] epochsAfter) {
        // both cuts read every other table as of the same epoch
        Map<VersionedRead, CompletableFuture<List<RowData>>>[] shared = new Map[lookups.length];
        Evaluation[] cuts = {
            new Evaluation(epochsBefore, shared), new Evaluation(epochsAfter, shared)
        };
        List<List<RowData[]>> lines = new ArrayList<>(2);
        lines.add(linesOf(leaf, befores));
        lines.add(linesOf(leaf, afters));
        return climb(leafNodes[leaf], lines, cuts);
    }

    private List<RowData[]> linesOf(int leaf, List<RowData> rows) {
        List<RowData[]> lines = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            RowData[] line = new RowData[tree.getNumLeaves()];
            line[leaf] = row;
            lines.add(line);
        }
        return lines;
    }

    /** Extends the lines of a node at both cuts to the join above it, up to the root. */
    private CompletableFuture<List<List<RowData[]>>> climb(
            int nodeId, List<List<RowData[]>> lines, Evaluation[] cuts) {
        if (nodeId == tree.getRootNode()) {
            return CompletableFuture.completedFuture(lines);
        }
        int joinId = parentNodes[nodeId];
        AnchorJoinTree.Node join = tree.getNode(joinId);
        boolean inLeft = join.getLeft() == nodeId;
        int otherNode = inLeft ? join.getRight() : join.getLeft();
        AnchorJoinTree.Edge edge = tree.getEdge(join.getEdge());
        int sideLeaf = inLeft ? edge.getLeftLeaf() : edge.getRightLeaf();
        int otherLeaf = inLeft ? edge.getRightLeaf() : edge.getLeftLeaf();
        int towardsOther = inLeft ? edge.getLookupRight() : edge.getLookupLeft();
        int towardsSide = inLeft ? edge.getLookupLeft() : edge.getLookupRight();
        FlinkJoinType type = join.getJoinType();
        boolean sidePreserved =
                type == FlinkJoinType.FULL
                        || (inLeft ? type == FlinkJoinType.LEFT : type == FlinkJoinType.RIGHT);
        boolean otherPreserved =
                type == FlinkJoinType.FULL
                        || (inLeft ? type == FlinkJoinType.RIGHT : type == FlinkJoinType.LEFT);
        Extended[] extended = new Extended[2];
        List<CompletableFuture<Void>> parts = new ArrayList<>(2);
        for (int c = 0; c < 2; c++) {
            int cut = c;
            parts.add(
                    extendAll(
                                    joinId,
                                    lines.get(c),
                                    sideLeaf,
                                    towardsOther,
                                    otherNode,
                                    otherLeaf,
                                    sidePreserved,
                                    cuts[c])
                            .thenAccept(result -> extended[cut] = result));
        }
        return CompletableFuture.allOf(parts.toArray(new CompletableFuture[0]))
                .thenCompose(
                        ignored -> {
                            if (!otherPreserved) {
                                return climb(joinId, linesOf(joinId, extended), cuts);
                            }
                            // the preserved lines whose padding the change may have flipped
                            Map<List<BinaryRowData>, RowData[]> candidates = new LinkedHashMap<>();
                            for (Extended e : extended) {
                                candidates.putAll(e.matched);
                            }
                            List<CompletableFuture<Void>> checks = new ArrayList<>();
                            for (int c = 0; c < 2; c++) {
                                Extended e = extended[c];
                                for (Map.Entry<List<BinaryRowData>, RowData[]> candidate :
                                        candidates.entrySet()) {
                                    if (e.matched.containsKey(candidate.getKey())) {
                                        continue;
                                    }
                                    RowData[] other = candidate.getValue();
                                    checks.add(
                                            hasMatch(
                                                            joinId,
                                                            other,
                                                            otherLeaf,
                                                            towardsSide,
                                                            nodeId,
                                                            sideLeaf,
                                                            cuts[c])
                                                    .thenAccept(
                                                            found -> {
                                                                if (!found) {
                                                                    e.padded.add(other.clone());
                                                                }
                                                            }));
                                }
                            }
                            return CompletableFuture.allOf(checks.toArray(new CompletableFuture[0]))
                                    .thenCompose(
                                            done -> climb(joinId, linesOf(joinId, extended), cuts));
                        });
    }

    private List<List<RowData[]>> linesOf(int joinId, Extended[] extended) {
        List<List<RowData[]>> lines = new ArrayList<>(2);
        for (Extended e : extended) {
            List<RowData[]> all = new ArrayList<>(e.lines);
            all.addAll(applyFilters(joinId, e.padded));
            lines.add(all);
        }
        return lines;
    }

    /**
     * Extends the given lines of one side of a join with the matching lines of the other side, and
     * remembers the lines of the other side that matched.
     */
    private CompletableFuture<Extended> extendAll(
            int joinId,
            List<RowData[]> partials,
            int sideLeaf,
            int towardsOther,
            int otherNode,
            int otherLeaf,
            boolean sidePreserved,
            Evaluation evaluation) {
        Extended result = new Extended();
        List<CompletableFuture<Void>> parts = new ArrayList<>(partials.size());
        for (RowData[] partial : partials) {
            parts.add(
                    linesOfOtherSide(
                                    partial[sideLeaf],
                                    towardsOther,
                                    otherNode,
                                    otherLeaf,
                                    evaluation)
                            .thenAccept(
                                    others -> {
                                        boolean any = false;
                                        for (RowData[] other : others) {
                                            RowData[] merged = merge(partial, other);
                                            if (matches(joinId, merged)) {
                                                result.lines.add(merged);
                                                result.matched.putIfAbsent(lineKey(other), other);
                                                any = true;
                                            }
                                        }
                                        if (!any && sidePreserved) {
                                            result.lines.add(partial);
                                        }
                                    }));
        }
        return CompletableFuture.allOf(parts.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            List<RowData[]> filtered =
                                    new ArrayList<>(applyFilters(joinId, result.lines));
                            result.lines.clear();
                            result.lines.addAll(filtered);
                            return result;
                        });
    }

    /** The lines of a node holding a row of its table that matches the key of {@code source}. */
    private CompletableFuture<List<RowData[]>> linesOfOtherSide(
            @Nullable RowData source, int lookupId, int node, int leaf, Evaluation evaluation) {
        if (source == null) {
            // the edge table of this side was padded, so its join key is NULL
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return evaluation
                .lookup(lookupId, source)
                .thenCompose(
                        rows -> {
                            List<CompletableFuture<List<RowData[]>>> lines =
                                    new ArrayList<>(rows.size());
                            for (RowData row : rows) {
                                lines.add(linesContaining(node, leaf, row, evaluation));
                            }
                            return flatten(lines);
                        });
    }

    /** Whether a line of the preserved side of a join matches any line of the other side. */
    private CompletableFuture<Boolean> hasMatch(
            int joinId,
            RowData[] preserved,
            int preservedLeaf,
            int towardsSide,
            int sideNode,
            int sideLeaf,
            Evaluation evaluation) {
        return linesOfOtherSide(
                        preserved[preservedLeaf], towardsSide, sideNode, sideLeaf, evaluation)
                .thenApply(
                        sides -> {
                            for (RowData[] side : sides) {
                                if (matches(joinId, merge(preserved, side))) {
                                    return true;
                                }
                            }
                            return false;
                        });
    }

    /** The rows of a line as binary rows, to compare lines read by different evaluations. */
    private List<BinaryRowData> lineKey(RowData[] line) {
        List<BinaryRowData> key = new ArrayList<>(line.length);
        for (int leaf = 0; leaf < line.length; leaf++) {
            if (line[leaf] == null) {
                key.add(null);
                continue;
            }
            BinaryRowData row = leafSerializers[leaf].toBinaryRow(line[leaf]).copy();
            row.setRowKind(RowKind.INSERT);
            key.add(row);
        }
        return key;
    }

    /** The lines of one cut extended over one join. */
    private static final class Extended {
        final List<RowData[]> lines = new ArrayList<>();

        /** The lines of the other side that matched, by their rows. */
        final Map<List<BinaryRowData>, RowData[]> matched = new LinkedHashMap<>();

        /** Lines of the other side that match nothing at this cut, padded. */
        final List<RowData[]> padded = new ArrayList<>();
    }

    private CompletableFuture<List<RowData[]>> evaluate(RowData anchorKey, Evaluation evaluation) {
        long start = evaluation.start;
        int anchorLeaf = tree.getAnchorLeaf();
        // first, so that it never waits for capacity behind lookups that depend on it
        CompletableFuture<List<RowData>> anchorRows =
                lookups[tree.getAnchorLookup()].lookup(anchorKey, start);
        // group reads first: the lookups below may be served by them
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            int groupLookup = tree.getGroupLookup(leaf);
            if (groupLookup >= 0 && lookups[groupLookup] != null) {
                evaluation.readGroup(leaf, groupLookup, anchorKey);
            }
        }
        evaluation.startAwayFromGroups();
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

        /** Per table read by group, all its rows of the group. */
        private final Map<Integer, CompletableFuture<List<RowData>>> groupRows = new HashMap<>();

        /**
         * Per lookup into a table read by group, the rows of the group by the key of the lookup.
         */
        private final Map<Integer, CompletableFuture<Map<BinaryRowData, List<RowData>>>>
                groupIndexes = new HashMap<>();

        private Evaluation(long start, @Nullable RowScope scope) {
            this(start, scope, Collections.emptyMap());
        }

        /** Per table, the epoch it is read as of; null to read by {@link #start}. */
        private final @Nullable long[] epochOfLeaf;

        /** With {@link #epochOfLeaf}: per lookup, the reads by key and epoch, shared by cuts. */
        private final @Nullable Map<VersionedRead, CompletableFuture<List<RowData>>>[] versioned;

        private Evaluation(
                long start, @Nullable RowScope scope, Map<Integer, List<RowData>> known) {
            this.start = start;
            this.scope = scope;
            this.known = known;
            this.epochOfLeaf = null;
            this.versioned = null;
        }

        /** Reads every table as of its own epoch. */
        private Evaluation(
                long[] epochOfLeaf, Map<VersionedRead, CompletableFuture<List<RowData>>>[] shared) {
            Preconditions.checkArgument(
                    epochOfLeaf.length == tree.getNumLeaves(), "Every table needs an epoch.");
            this.start = 0L;
            this.scope = null;
            this.known = Collections.emptyMap();
            this.epochOfLeaf = epochOfLeaf;
            this.versioned = shared;
        }

        /** The rows of the given lookup for the key in {@code source}. */
        CompletableFuture<List<RowData>> lookup(int lookupId, RowData source) {
            Lookup lookup = lookups[lookupId];
            BinaryRowData key = lookup.keyOrNull(source);
            if (key == null) {
                // NULL never equals anything
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            if (epochOfLeaf != null) {
                if (versioned[lookupId] == null) {
                    versioned[lookupId] = new HashMap<>();
                }
                long epoch = epochOfLeaf[targetOfLookup[lookupId]];
                return versioned[lookupId].computeIfAbsent(
                        new VersionedRead(key, epoch), read -> lookup.lookupAsOf(key, epoch));
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
            int target = leafOfLookup[lookupId];
            if (result == null && target >= 0 && groupIndexGetters[lookupId] != null) {
                CompletableFuture<List<RowData>> rows = groupRows.get(target);
                if (rows != null) {
                    // served by the group read
                    result =
                            groupIndexes
                                    .computeIfAbsent(
                                            lookupId, id -> rows.thenApply(all -> index(id, all)))
                                    .thenApply(
                                            index ->
                                                    index.getOrDefault(
                                                            key, Collections.emptyList()));
                    byKey.put(key, result);
                    return result;
                }
            }
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

        /** Reads all rows of a table that belong to the group of the anchor key. */
        void readGroup(int leaf, int groupLookup, RowData anchorKey) {
            Lookup lookup = lookups[groupLookup];
            BinaryRowData key = lookup.keyOrNull(anchorKey);
            CompletableFuture<List<RowData>> rows =
                    key == null
                            ? CompletableFuture.completedFuture(Collections.emptyList())
                            : lookup.lookup(anchorKey, key, start);
            groupRows.put(leaf, rows);
        }

        /** Starts the lookups beyond the rows of every table read by group, once they arrive. */
        void startAwayFromGroups() {
            for (Map.Entry<Integer, CompletableFuture<List<RowData>>> entry :
                    groupRows.entrySet()) {
                int leaf = entry.getKey();
                entry.getValue()
                        .thenAccept(
                                all -> {
                                    for (RowData row : all) {
                                        startAwayFrom(leaf, row);
                                    }
                                });
            }
        }

        /** The rows of a table read by group, by the key of a lookup into the table. */
        private Map<BinaryRowData, List<RowData>> index(int lookupId, List<RowData> rows) {
            RowData.FieldGetter[] getters = groupIndexGetters[lookupId];
            Map<BinaryRowData, List<RowData>> index = new HashMap<>();
            for (RowData row : rows) {
                GenericRowData key = new GenericRowData(getters.length);
                boolean hasNull = false;
                for (int i = 0; i < getters.length; i++) {
                    Object value = getters[i].getFieldOrNull(row);
                    hasNull |= value == null;
                    key.setField(i, value);
                }
                if (!hasNull) {
                    // NULL never equals anything
                    index.computeIfAbsent(
                                    groupIndexSerializers[lookupId].toBinaryRow(key, true),
                                    k -> new ArrayList<>())
                            .add(row);
                }
            }
            return index;
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

        /** The lookup function of the fetcher if it reads as of epochs. */
        private @Nullable SupportsVersionedLookup versionedFunction;

        /** The reads as of every kept epoch, by key. */
        private final TreeMap<Long, Map<BinaryRowData, CompletableFuture<List<RowData>>>>
                readsByEpoch = new TreeMap<>();

        /** Reads the fields of a key row of this lookup. */
        private RowData.FieldGetter[] keyRowGetters;

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
            keyRowGetters = new RowData.FieldGetter[keyTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                keyRowGetters[i] = RowData.createFieldGetter(keyTypes[i], i);
            }
            if (spec.isPaged()) {
                pagedFunction = GroupLookup.pagedFunctionOf(spec.getFetcher());
            }
            versionedFunction = GroupLookup.versionedFunctionOrNull(spec.getFetcher());
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

        /**
         * Reads the rows matching {@code key} as of the epoch. Such reads never change, so they are
         * kept, also in flight, until {@link #evictReadsBefore} drops their epoch.
         */
        private CompletableFuture<List<RowData>> lookupAsOf(BinaryRowData key, long epoch) {
            Preconditions.checkState(
                    versionedFunction != null,
                    "The lookup %s cannot read as of an epoch.",
                    spec.getDescription());
            Map<BinaryRowData, CompletableFuture<List<RowData>>> ofEpoch =
                    readsByEpoch.computeIfAbsent(epoch, e -> new HashMap<>());
            CompletableFuture<List<RowData>> kept = ofEpoch.get(key);
            if (kept != null) {
                numCachedReads++;
                return kept;
            }
            CompletableFuture<List<RowData>> read = new CompletableFuture<>();
            ofEpoch.put(key, read);
            read.whenComplete(
                    (rows, error) -> {
                        if (error != null) {
                            ofEpoch.remove(key, read);
                        }
                    });
            issueOrWait(() -> invokeAsOf(key, epoch, read));
            return read;
        }

        private void invokeAsOf(
                BinaryRowData key, long epoch, CompletableFuture<List<RowData>> result) {
            lookupsInFlight++;
            numLookups++;
            CompletableFuture<Collection<RowData>> raw;
            try {
                GenericRowData keyRow = new GenericRowData(keyRowGetters.length);
                for (int i = 0; i < keyRowGetters.length; i++) {
                    keyRow.setField(i, keyRowGetters[i].getFieldOrNull(key));
                }
                raw = versionedFunction.asyncLookupAsOf(keyRow, epoch);
            } catch (Throwable t) {
                raw = new CompletableFuture<>();
                raw.completeExceptionally(t);
            }
            if (timeoutMs > 0) {
                raw = raw.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
            }
            raw.whenComplete(
                    (rows, error) ->
                            mailbox.execute(
                                    () -> {
                                        lookupDone();
                                        if (error != null) {
                                            result.completeExceptionally(
                                                    wrap(error, "The read as of an epoch of"));
                                            return;
                                        }
                                        try {
                                            result.complete(
                                                    pageRows(
                                                            rows == null
                                                                    ? Collections.emptyList()
                                                                    : rows));
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

    /** A read of the rows of a key as of an epoch. */
    private static final class VersionedRead {
        private final BinaryRowData key;
        private final long epoch;

        private VersionedRead(BinaryRowData key, long epoch) {
            this.key = key;
            this.epoch = epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof VersionedRead)) {
                return false;
            }
            VersionedRead other = (VersionedRead) o;
            return epoch == other.epoch && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return 31 * key.hashCode() + Long.hashCode(epoch);
        }
    }
}
