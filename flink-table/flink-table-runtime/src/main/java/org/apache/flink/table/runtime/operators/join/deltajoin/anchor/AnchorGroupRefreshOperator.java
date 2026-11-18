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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOperatorBase;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.LRUMap;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Refreshes the groups of the anchor key of a tree of joins: a group is every output row that
 * contains the anchor row with one key. The input is the notes of {@link AnchorLocateOperator}s,
 * keyed by the anchor key, and a refresh evaluates the tree from the anchor row, see {@link
 * AnchorJoinEvaluator}.
 *
 * <p>Changes of a table that can refresh its own lines (see {@link
 * AnchorJoinTree#canRefreshRowsOf}) replace only the rows of the changed rows' lines, if they are
 * the only changes of the group since its last refresh. Otherwise, and after a restore, the whole
 * group is refreshed.
 */
public class AnchorGroupRefreshOperator extends GroupRefreshOperatorBase
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AnchorGroupRefreshOperator.class);

    /** How many groups the operator remembers the last refresh of; others are refreshed whole. */
    static final int MAX_REMEMBERED_GROUPS = 100_000;

    /** The last refresh of a group was a refresh of the whole group. */
    private static final int WHOLE = -1;

    private final AnchorJoinTree tree;
    private final AnchorLookupSpec[] lookupSpecs;
    private final @Nullable GeneratedJoinCondition[] conditions;
    private final GeneratedFilterCondition[][] filters;
    private final AnchorNotes notes;

    private transient AnchorJoinEvaluator evaluator;

    /** Per table, whether its changes refresh only their own lines. */
    private transient boolean[] scopedLeaves;

    private transient boolean anyScopedLeaf;
    private transient RowDataSerializer[] leafSerializers;
    private transient RowData.FieldGetter[][] uniqueKeyGetters;
    private transient RowDataSerializer[] uniqueKeySerializers;

    /** Per anchor key, the changed rows since the refresh of the key last started. */
    private transient Map<RowData, Scope> scopes;

    /** The tables whose rows of one group are read in pages, see {@link #readGroup}. */
    private transient int[] pagedLeaves;

    /**
     * Per anchor key, the table whose lines its refreshes since the last refresh of the whole group
     * replaced, or {@link #WHOLE}; least recently refreshed first.
     */
    private transient Map<RowData, Integer> lastRefreshes;

    public AnchorGroupRefreshOperator(
            StreamOperatorParameters<RowData> parameters,
            GroupRefreshSpec spec,
            AnchorJoinTree tree,
            AnchorLookupSpec[] lookupSpecs,
            @Nullable GeneratedJoinCondition[] conditions,
            GeneratedFilterCondition[][] filters,
            AnchorNotes notes) {
        super(parameters, spec);
        this.tree = tree;
        this.lookupSpecs = lookupSpecs;
        this.conditions = conditions;
        this.filters = filters;
        this.notes = notes;
    }

    @Override
    protected void openGroupReader() throws Exception {
        evaluator =
                new AnchorJoinEvaluator(
                        tree,
                        lookupSpecs,
                        conditions,
                        filters,
                        spec.getLookupTimeoutMs(),
                        runnable -> mailboxExecutor.execute(runnable::run, "anchor lookup result"),
                        spec.getCapacity());
        evaluator.open(
                getRuntimeContext(),
                DefaultOpenContext.INSTANCE,
                tree.lookupsUsedToRecompute(lookupSpecs.length),
                true);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("numSharedLookups", evaluator::getNumSharedLookups);
        openScopes();
        List<Integer> paged = new ArrayList<>();
        for (int leaf = 0; leaf < tree.getNumLeaves(); leaf++) {
            if (pageSize() > 0 && evaluator.readsInPages(leaf)) {
                paged.add(leaf);
            }
        }
        pagedLeaves = paged.stream().mapToInt(Integer::intValue).toArray();
        if (!paged.isEmpty()) {
            LOG.info(
                    "Groups with more than {} rows of one of the tables {} are read in pages.",
                    pageSize(),
                    paged);
        }
    }

    private void openScopes() {
        int numLeaves = tree.getNumLeaves();
        scopedLeaves = new boolean[numLeaves];
        leafSerializers = new RowDataSerializer[numLeaves];
        uniqueKeyGetters = new RowData.FieldGetter[numLeaves][];
        uniqueKeySerializers = new RowDataSerializer[numLeaves];
        scopes = new HashMap<>();
        lastRefreshes = new LRUMap<>(MAX_REMEMBERED_GROUPS);
        boolean rowsReadable = rowsCanBeReadByKey();
        List<Integer> scoped = new ArrayList<>();
        for (int leaf = 0; leaf < numLeaves; leaf++) {
            int[] uniqueKey = spec.getTableKey(leaf);
            if (!rowsReadable
                    || !notes.carriesRows(leaf)
                    || uniqueKey.length == 0
                    || conditions == null
                    || conditions[tree.getJoinTowardsAnchor(leaf)] == null) {
                continue;
            }
            RowType type = spec.getTableType(leaf);
            scopedLeaves[leaf] = true;
            leafSerializers[leaf] = new RowDataSerializer(type);
            uniqueKeyGetters[leaf] = new RowData.FieldGetter[uniqueKey.length];
            LogicalType[] keyTypes = new LogicalType[uniqueKey.length];
            for (int i = 0; i < uniqueKey.length; i++) {
                keyTypes[i] = type.getTypeAt(uniqueKey[i]);
                uniqueKeyGetters[leaf][i] = RowData.createFieldGetter(keyTypes[i], uniqueKey[i]);
            }
            uniqueKeySerializers[leaf] = new RowDataSerializer(keyTypes);
            scoped.add(leaf);
        }
        anyScopedLeaf = !scoped.isEmpty();
        if (anyScopedLeaf) {
            LOG.info("Changes of the tables {} refresh only their own lines.", scoped);
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData key = (RowData) getCurrentKey();
        RowData note = element.getValue();
        byte change = notes.changeOf(note);
        if (anyScopedLeaf) {
            addToScope(key, note, change);
        }
        // the note of the new row follows and requests the refresh
        if (change != AnchorNotes.REPLACED) {
            requestRefresh(key);
        }
    }

    /** Remembers which rows of the group changed, every version of them, and the latest one. */
    private void addToScope(RowData key, RowData note, byte change) {
        Scope scope = scopes.get(key);
        if (scope == null) {
            scope = new Scope();
            scopes.put(key instanceof BinaryRowData ? ((BinaryRowData) key).copy() : key, scope);
        }
        if (scope.rows == null) {
            return;
        }
        int leaf = notes.leafOf(note);
        RowData row = notes.rowOf(note);
        if (!scopedLeaves[leaf] || row == null || (scope.leaf >= 0 && scope.leaf != leaf)) {
            scope.makeWhole();
            return;
        }
        scope.leaf = leaf;
        BinaryRowData image = leafSerializers[leaf].toBinaryRow(row).copy();
        image.setRowKind(RowKind.INSERT);
        Versions versions =
                scope.rows.computeIfAbsent(uniqueKeyOf(leaf, image), k -> new Versions());
        if (!versions.seen.contains(image)) {
            versions.seen.add(image);
        }
        versions.latest = change == AnchorNotes.ADD ? image : null;
        versions.replaced = change == AnchorNotes.REPLACED;
        if (scope.rows.size() > MAX_SCOPE_ROWS) {
            scope.makeWhole();
        }
    }

    private BinaryRowData uniqueKeyOf(int leaf, RowData row) {
        RowData.FieldGetter[] getters = uniqueKeyGetters[leaf];
        GenericRowData key = new GenericRowData(getters.length);
        for (int i = 0; i < getters.length; i++) {
            key.setField(i, getters[i].getFieldOrNull(row));
        }
        return uniqueKeySerializers[leaf].toBinaryRow(key).copy();
    }

    /**
     * Reads the lines of a group. If tables joined to the anchor table directly read in pages, the
     * first page of each is read first. A table with more rows than a page is then read page by
     * page: every page stands in for the table in an evaluation of the tree (see {@link
     * AnchorJoinEvaluator#linesOfRows}). A line of the anchor's side that no page matched is padded
     * after the last page, if the join keeps it. Further large tables are read whole.
     */
    @Override
    protected void readGroup(RowData key, GroupRead read) {
        if (pageSize() == 0 || pagedLeaves.length == 0) {
            readWhole(key, Collections.emptyMap(), read);
            return;
        }
        int n = pagedLeaves.length;
        List<List<RowData>> first = new ArrayList<>(Collections.nCopies(n, null));
        AnchorJoinEvaluator.TablePages[] pages = new AnchorJoinEvaluator.TablePages[n];
        int[] missing = {n};
        boolean[] failed = {false};
        Consumer<Throwable> onError =
                error -> {
                    if (!failed[0]) {
                        failed[0] = true;
                        for (AnchorJoinEvaluator.TablePages open : pages) {
                            if (open != null) {
                                open.close();
                            }
                        }
                        read.failed(error);
                    }
                };
        for (int i = 0; i < n; i++) {
            int index = i;
            pages[i] = evaluator.openPages(key, pagedLeaves[i], pageSize());
            pages[i].next(
                    (rows, last) -> {
                        if (failed[0]) {
                            return;
                        }
                        first.set(index, rows);
                        if (last) {
                            pages[index] = null;
                        }
                        if (--missing[0] == 0) {
                            firstPagesRead(key, first, pages, read, onError);
                        }
                    },
                    onError);
        }
    }

    private void firstPagesRead(
            RowData key,
            List<List<RowData>> first,
            AnchorJoinEvaluator.TablePages[] pages,
            GroupRead read,
            Consumer<Throwable> onError) {
        Map<Integer, List<RowData>> known = new HashMap<>();
        int large = -1;
        List<Integer> moreLarge = new ArrayList<>();
        for (int i = 0; i < pages.length; i++) {
            if (pages[i] == null) {
                known.put(pagedLeaves[i], first.get(i));
            } else if (large < 0) {
                large = i;
            } else {
                moreLarge.add(i);
            }
        }
        int paged = large;
        drainAll(
                key,
                moreLarge,
                first,
                pages,
                known,
                () -> {
                    if (paged < 0) {
                        readWhole(key, known, read);
                    } else {
                        read.pages(
                                new TreePages(
                                        key,
                                        pagedLeaves[paged],
                                        first.get(paged),
                                        pages[paged],
                                        known));
                    }
                },
                onError);
    }

    /** Reads the remaining pages of the given tables, whose rows are then known. */
    private void drainAll(
            RowData key,
            List<Integer> indexes,
            List<List<RowData>> first,
            AnchorJoinEvaluator.TablePages[] pages,
            Map<Integer, List<RowData>> known,
            Runnable onDone,
            Consumer<Throwable> onError) {
        if (indexes.isEmpty()) {
            onDone.run();
            return;
        }
        int index = indexes.get(0);
        drain(
                key,
                pages[index],
                new ArrayList<>(first.get(index)),
                rows -> {
                    pages[index] = null;
                    known.put(pagedLeaves[index], rows);
                    drainAll(
                            key,
                            indexes.subList(1, indexes.size()),
                            first,
                            pages,
                            known,
                            onDone,
                            onError);
                },
                onError);
    }

    private void readWhole(RowData key, Map<Integer, List<RowData>> known, GroupRead read) {
        evaluator
                .group(key, evaluator.newStamp(), known)
                .whenComplete(
                        (lines, error) -> {
                            if (error != null) {
                                read.failed(error);
                            } else {
                                read.lines(lines);
                            }
                        });
    }

    // ------------------------------------------------------------------------------------------
    // refreshes of the lines of changed rows
    // ------------------------------------------------------------------------------------------

    @Override
    protected void discardScope(RowData key) {
        if (scopes != null) {
            scopes.remove(key);
        }
    }

    @Override
    protected boolean startScopedRefresh(RowData key) {
        if (!anyScopedLeaf) {
            return false;
        }
        Scope scope = scopes.remove(key);
        if (scope == null || scope.rows == null || scope.leaf < 0) {
            // a change of another table, too many rows, or a key restored from a checkpoint
            return false;
        }
        Integer last = lastRefreshes.get(key);
        if (last == null || (last != WHOLE && last != scope.leaf)) {
            // lines of another table may have been emitted with rows of this one that are gone
            return false;
        }
        List<RowData> latest = new ArrayList<>();
        List<RowData> older = new ArrayList<>();
        for (Versions versions : scope.rows.values()) {
            if (versions.replaced) {
                // a refresh of another note overtook the new version of a replaced row
                return false;
            }
            if (versions.latest != null) {
                latest.add(versions.latest);
            }
            for (BinaryRowData version : versions.seen) {
                if (!version.equals(versions.latest)) {
                    older.add(version);
                }
            }
        }
        remember(key, scope.leaf);
        evaluator
                .linesOfRows(key, evaluator.newStamp(), scope.leaf, latest, older)
                .whenComplete(
                        (lines, error) -> {
                            if (error != null) {
                                fail(error);
                                return;
                            }
                            try {
                                refreshRows(key, lines);
                            } catch (Throwable t) {
                                fail(t);
                            }
                        });
        return true;
    }

    @Override
    protected void refreshWholeGroup(RowData key) throws Exception {
        if (anyScopedLeaf) {
            remember(key, WHOLE);
        }
        super.refreshWholeGroup(key);
    }

    private void remember(RowData key, int refresh) {
        // a forgotten group is refreshed whole next time
        lastRefreshes.put(
                key instanceof BinaryRowData ? ((BinaryRowData) key).copy() : key, refresh);
    }

    /**
     * Reads the rows that the lines of the changed rows emitted before, and replaces them. Every
     * version of a changed row that the group may have emitted is among the evaluated ones: the
     * last refresh read either the version before the first change since, or one of the versions
     * these changes produced; a version produced by a change that did not arrive yet is replaced
     * when it arrives.
     */
    private void refreshRows(RowData key, AnchorJoinEvaluator.ScopedLines lines) throws Exception {
        Set<RowData> rowKeys = new LinkedHashSet<>();
        for (RowData[] line : lines.latest) {
            rowKeys.addAll(rowKeysOfLine(line));
        }
        // row keys of side lines that may have lost their last match, if the group emitted them
        List<List<RowData>> undecided = new ArrayList<>();
        for (AnchorJoinEvaluator.Side side : lines.sides) {
            List<RowData> olderKeys = new ArrayList<>();
            for (RowData[] line : side.older) {
                olderKeys.addAll(rowKeysOfLine(line));
            }
            rowKeys.addAll(olderKeys);
            if (!lines.preservesSides) {
                continue;
            }
            if (side.matchedByLatest) {
                // the side line has a match now: its padded rows must be gone
                for (RowData[] line : side.padded) {
                    rowKeys.addAll(rowKeysOfLine(line));
                }
            } else if (side.matchedByOlder) {
                if (olderKeys.isEmpty()) {
                    // whether the group emitted the match is unknown
                    refreshWholeGroup(key);
                    return;
                }
                undecided.add(olderKeys);
            }
        }
        readPreviousRows(
                key,
                rowKeys,
                previous ->
                        mailboxExecutor.execute(
                                () -> completeRows(key, rowKeys, previous, lines, undecided),
                                "Complete refresh of the changed rows of key %s",
                                key),
                this::fail);
    }

    private void completeRows(
            RowData key,
            Set<RowData> rowKeys,
            Map<RowData, BinaryRowData> previous,
            AnchorJoinEvaluator.ScopedLines lines,
            List<List<RowData>> undecided)
            throws Exception {
        for (List<RowData> olderKeys : undecided) {
            for (RowData rowKey : olderKeys) {
                if (previous.containsKey(rowKey)) {
                    // the side line matched a row that changed and matches no more; whether
                    // another row of the table matches it is unknown
                    refreshWholeGroup(key);
                    return;
                }
            }
        }
        completeScopedRefresh(key, rowKeys, previous, lines.latest);
    }

    @Override
    protected long numLookups() {
        return evaluator == null ? 0 : evaluator.getNumLookups();
    }

    @Override
    public void endInput() throws Exception {
        waitUntilIdle();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (evaluator != null) {
            evaluator.close();
        }
    }

    /** The lines of a group in pages of one table joined to the anchor table. */
    private final class TreePages implements LinePages {
        private final RowData key;
        private final int leaf;
        private @Nullable List<RowData> firstPage;
        private final AnchorJoinEvaluator.TablePages more;
        private final Map<Integer, List<RowData>> known;

        /** The row keys of padded lines of side lines that a page matched. */
        private final Set<RowData> matchedPadded = new HashSet<>();

        private TreePages(
                RowData key,
                int leaf,
                List<RowData> firstPage,
                AnchorJoinEvaluator.TablePages more,
                Map<Integer, List<RowData>> known) {
            this.key = key;
            this.leaf = leaf;
            this.firstPage = firstPage;
            this.more = more;
            this.known = known;
        }

        @Override
        public void next(
                BiConsumer<List<RowData[]>, Boolean> onLines, Consumer<Throwable> onError) {
            if (firstPage != null) {
                // the first page is never the last, or the table would have been read whole
                List<RowData> page = firstPage;
                firstPage = null;
                evaluate(page, false, onLines, onError);
                return;
            }
            more.next((rows, last) -> evaluate(rows, last, onLines, onError), onError);
        }

        private void evaluate(
                List<RowData> rows,
                boolean last,
                BiConsumer<List<RowData[]>, Boolean> onLines,
                Consumer<Throwable> onError) {
            evaluator
                    .linesOfRows(
                            key, evaluator.newStamp(), leaf, rows, Collections.emptyList(), known)
                    .whenComplete(
                            (lines, error) -> {
                                if (error != null) {
                                    more.close();
                                    onError.accept(error);
                                    return;
                                }
                                try {
                                    onLines.accept(linesOf(lines, last), last);
                                } catch (Throwable t) {
                                    more.close();
                                    onError.accept(t);
                                }
                            });
        }

        /**
         * The lines of a page; after the last page also the padded lines of side lines that no page
         * matched. Side lines of different pages are told apart by the row keys of their padded
         * lines.
         */
        private List<RowData[]> linesOf(AnchorJoinEvaluator.ScopedLines lines, boolean last)
                throws Exception {
            List<RowData[]> out = new ArrayList<>(lines.latest);
            if (!lines.preservesSides) {
                return out;
            }
            for (AnchorJoinEvaluator.Side side : lines.sides) {
                if (side.matchedByLatest) {
                    for (RowData[] line : side.padded) {
                        matchedPadded.addAll(rowKeysOfLine(line));
                    }
                }
            }
            if (last) {
                for (AnchorJoinEvaluator.Side side : lines.sides) {
                    if (side.matchedByLatest) {
                        continue;
                    }
                    boolean matchedBefore = false;
                    for (RowData[] line : side.padded) {
                        for (RowData rowKey : rowKeysOfLine(line)) {
                            matchedBefore |= matchedPadded.contains(rowKey);
                        }
                    }
                    if (!matchedBefore) {
                        out.addAll(side.padded);
                    }
                }
            }
            return out;
        }
    }

    /** The changes of a group since its refresh last started. */
    private static final class Scope {
        /** The table of the changes, or -1 before the first. */
        private int leaf = -1;

        /** Per unique key of a changed row, its versions; null if the group is refreshed whole. */
        private @Nullable LinkedHashMap<RowData, Versions> rows = new LinkedHashMap<>();

        private void makeWhole() {
            rows = null;
        }
    }

    /** The versions of one changed row. */
    private static final class Versions {
        /** Every version a change carried, as the old or the new row. */
        private final List<BinaryRowData> seen = new ArrayList<>(2);

        /** The current version, or null if the last change removed the row from the group. */
        private @Nullable BinaryRowData latest;

        /** Whether the last change replaced the row, and its new version did not arrive yet. */
        private boolean replaced;
    }
}
