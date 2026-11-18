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

package org.apache.flink.table.runtime.operators.join.deltajoin.refresh;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Refreshes the groups of the join key of a join of two tables (INNER, LEFT, RIGHT or FULL): a
 * change marks the group of its join key, and a refresh reads the rows of both inputs with that
 * key, by lookups or from a cache, and joins them.
 *
 * <p>If one input has at most one row per join key (a parent), changes of the other input refresh
 * only their own rows. Rows with a NULL in the join key are emitted padded directly from the
 * change.
 */
public class JoinKeyGroupRefreshOperator extends GroupRefreshOperatorBase
        implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JoinKeyGroupRefreshOperator.class);

    /** The rows of one input and join key are cached only up to this number. */
    static final int MAX_CACHED_ROWS = 10_000;

    private final JoinKeyGroupSpec groups;

    private transient GroupJoiner joiner;
    private transient @Nullable FilterCondition condition;
    private transient @Nullable DeltaJoinCache cache;

    /** Cache loads in flight per join key; true once a change of the key arrived meanwhile. */
    private transient Map<RowData, Boolean> leftLoads;

    private transient Map<RowData, Boolean> rightLoads;

    private transient RowDataSerializer leftSerializer;
    private transient RowDataSerializer rightSerializer;
    private transient RowDataKeySelector leftUniqueKey;
    private transient RowDataKeySelector rightUniqueKey;
    private transient boolean[] inputEnded;

    /** The input whose changes refresh only their own rows, or -1. */
    private transient int scopedSide;

    /** Per join key, the children that changed since the refresh of the key last started. */
    private transient Map<RowData, Scope> scopes;

    private transient int scopedArity;
    private transient RowData.FieldGetter[] scopedUniqueKeyGetters;
    private transient RowData.FieldGetter[] scopedJoinKeyGetters;
    private transient int parentArity;
    private transient RowData.FieldGetter[] parentJoinKeyGetters;

    // metrics, also used by tests
    private transient long lookups;
    private transient long cacheHits;

    public JoinKeyGroupRefreshOperator(
            StreamOperatorParameters<RowData> parameters,
            GroupRefreshSpec spec,
            JoinKeyGroupSpec groups) {
        super(parameters, spec);
        this.groups = groups;
    }

    @VisibleForTesting
    JoinKeyGroupRefreshOperator(
            GroupRefreshSpec spec, JoinKeyGroupSpec groups, MailboxExecutor mailboxExecutor) {
        super(spec, mailboxExecutor);
        this.groups = groups;
    }

    @Override
    protected void openGroupReader() throws Exception {
        leftSerializer = new RowDataSerializer(groups.leftType);
        rightSerializer = new RowDataSerializer(groups.rightType);
        leftUniqueKey = groups.leftUniqueKey.copy();
        rightUniqueKey = groups.rightUniqueKey.copy();
        inputEnded = new boolean[2];
        if (groups.condition != null) {
            condition = groups.condition.newInstance(getRuntimeContext().getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(condition, getRuntimeContext());
            FunctionUtils.openFunction(condition, DefaultOpenContext.INSTANCE);
        }
        joiner = new GroupJoiner(groups.joinType, condition);
        groups.leftLookup.open(
                getRuntimeContext(),
                mailboxExecutor,
                getProcessingTimeService(),
                spec.lookupTimeoutMs);
        groups.rightLookup.open(
                getRuntimeContext(),
                mailboxExecutor,
                getProcessingTimeService(),
                spec.lookupTimeoutMs);
        if (groups.leftCacheSize > 0) {
            cache = new DeltaJoinCache(groups.leftCacheSize, groups.rightCacheSize);
            cache.registerMetrics(getRuntimeContext().getMetricGroup());
        }
        leftLoads = new HashMap<>();
        rightLoads = new HashMap<>();
        getRuntimeContext().getMetricGroup().gauge("numInputCacheHits", () -> cacheHits);

        scopedSide = groups.scopedSide >= 0 && rowsCanBeReplaced() ? groups.scopedSide : -1;
        scopes = new HashMap<>();
        if (scopedSide >= 0) {
            RowType scopedType = scopedSide == 0 ? groups.leftType : groups.rightType;
            scopedArity = scopedType.getFieldCount();
            scopedUniqueKeyGetters = new RowData.FieldGetter[groups.scopedUniqueKey.length];
            for (int i = 0; i < scopedUniqueKeyGetters.length; i++) {
                scopedUniqueKeyGetters[i] =
                        RowData.createFieldGetter(
                                scopedType.getTypeAt(groups.scopedUniqueKey[i]), i);
            }
            scopedJoinKeyGetters = new RowData.FieldGetter[groups.scopedJoinKey.length];
            for (int i = 0; i < scopedJoinKeyGetters.length; i++) {
                scopedJoinKeyGetters[i] =
                        RowData.createFieldGetter(scopedType.getTypeAt(groups.scopedJoinKey[i]), i);
            }
            RowType parentType = scopedSide == 0 ? groups.rightType : groups.leftType;
            parentArity = parentType.getFieldCount();
            parentJoinKeyGetters = new RowData.FieldGetter[groups.parentJoinKey.length];
            for (int i = 0; i < parentJoinKeyGetters.length; i++) {
                parentJoinKeyGetters[i] =
                        RowData.createFieldGetter(parentType.getTypeAt(groups.parentJoinKey[i]), i);
            }
            LOG.info(
                    "Changes of the {} input refresh only their own rows.",
                    scopedSide == 0 ? "left" : "right");
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), true);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), false);
    }

    private void processElement(RowData row, boolean isLeft) throws Exception {
        RowData key = (RowData) getCurrentKey();
        boolean retract =
                row.getRowKind() == RowKind.UPDATE_BEFORE || row.getRowKind() == RowKind.DELETE;
        if (hasNull(key)) {
            // never matches: the output of the row depends on this change only
            if (joiner.preserves(isLeft)) {
                emitUnmatched(GroupJoiner.pad(row, isLeft), retract);
            }
            return;
        }
        if (cache != null) {
            RowData uk = (isLeft ? leftUniqueKey : rightUniqueKey).getKey(row);
            if (retract) {
                cache.deleteCache(key, uk, !isLeft);
            } else {
                RowData copy = (isLeft ? leftSerializer : rightSerializer).copy(row);
                copy.setRowKind(RowKind.INSERT);
                cache.upsertCache(key, uk, copy, !isLeft);
            }
            Map<RowData, Boolean> loads = isLeft ? leftLoads : rightLoads;
            if (loads.containsKey(key)) {
                loads.put(key, true);
            }
        }
        if (scopedSide >= 0) {
            addToScope(key, row, isLeft, retract);
        }
        requestRefresh(key);
    }

    /** Remembers which children of the group changed, and their latest version. */
    private void addToScope(RowData key, RowData row, boolean isLeft, boolean retract)
            throws Exception {
        Scope scope = scopes.get(key);
        if (scope == null) {
            scope = new Scope();
            scopes.put(copyKey(key), scope);
        }
        if ((isLeft ? 0 : 1) != scopedSide) {
            scope.makeWhole();
            return;
        }
        if (scope.changes == null) {
            return;
        }
        RowData uk = copyKey((isLeft ? leftUniqueKey : rightUniqueKey).getKey(row));
        RowData image = null;
        if (!retract) {
            image = (isLeft ? leftSerializer : rightSerializer).copy(row);
            image.setRowKind(RowKind.INSERT);
        }
        scope.changes.put(uk, image);
        if (scope.changes.size() > MAX_SCOPE_ROWS) {
            scope.makeWhole();
        }
    }

    // ------------------------------------------------------------------------------------------
    // refreshes of whole groups
    // ------------------------------------------------------------------------------------------

    @Override
    protected void readGroup(RowData key, GroupRead read) throws Exception {
        Side[] sides = new Side[2];
        int[] missing = {2};
        boolean[] failed = {false};
        Consumer<Throwable> onError =
                error -> {
                    if (!failed[0]) {
                        failed[0] = true;
                        for (Side side : sides) {
                            if (side != null && side.more != null) {
                                side.more.close();
                            }
                        }
                        read.failed(error);
                    }
                };
        for (int side = 0; side < 2; side++) {
            int s = side;
            readSide(
                    key,
                    side == 0,
                    pageSize(),
                    result -> {
                        if (failed[0]) {
                            if (result.more != null) {
                                result.more.close();
                            }
                            return;
                        }
                        sides[s] = result;
                        if (--missing[0] == 0) {
                            combine(key, sides, read);
                        }
                    },
                    onError);
        }
    }

    /**
     * Joins the rows of both inputs: at once if both were read whole, otherwise in pages of the
     * input that has more, with all rows of the other.
     */
    private void combine(RowData key, Side[] sides, GroupRead read) {
        try {
            if (sides[0].more == null && sides[1].more == null) {
                read.lines(joiner.join(sides[0].rows, sides[1].rows));
                return;
            }
            if (sides[0].more != null && sides[1].more != null) {
                // both inputs are large: the right one is held whole
                drain(
                        key,
                        sides[1].more,
                        new ArrayList<>(sides[1].rows),
                        rows -> read.pages(new JoinPages(sides[0], true, rows)),
                        error -> {
                            sides[0].more.close();
                            read.failed(error);
                        });
                return;
            }
            boolean pagedLeft = sides[0].more != null;
            read.pages(
                    new JoinPages(
                            sides[pagedLeft ? 0 : 1], pagedLeft, sides[pagedLeft ? 1 : 0].rows));
        } catch (Throwable t) {
            read.failed(t);
        }
    }

    /**
     * Reads the rows of one input with the join key, from the cache or by a lookup: all of them, or
     * the first page and the pages that follow if the lookup reads in pages and there are more than
     * {@code pageSize} rows (0 reads all rows at once).
     */
    private void readSide(
            RowData key,
            boolean isLeft,
            int pageSize,
            Consumer<Side> onSide,
            Consumer<Throwable> onError)
            throws Exception {
        GroupLookup lookup = isLeft ? groups.leftLookup : groups.rightLookup;
        if (pageSize == 0 || !lookup.supportsPages()) {
            readSide(key, isLeft, rows -> onSide.accept(new Side(rows, null)), onError);
            return;
        }
        List<RowData> cached = fromCache(key, isLeft);
        if (cached != null) {
            onSide.accept(new Side(cached, null));
            return;
        }
        lookups++;
        GroupLookup.Pages pages = lookup.openPages(key, pageSize);
        pages.next(
                (rows, last) -> {
                    if (!last) {
                        // too large to cache
                        (isLeft ? leftLoads : rightLoads).remove(key);
                        onSide.accept(new Side(rows, pages));
                        return;
                    }
                    try {
                        if (cache != null) {
                            installLoad(key, rows, isLeft);
                        }
                    } catch (Throwable t) {
                        onError.accept(t);
                        return;
                    }
                    onSide.accept(new Side(rows, null));
                },
                onError);
    }

    /**
     * The cached rows of one input with the join key, or null; a miss registers the load that
     * follows.
     */
    private @Nullable List<RowData> fromCache(RowData key, boolean isLeft) {
        if (cache == null) {
            return null;
        }
        LinkedHashMap<RowData, RowData> cached = cache.getData(key, !isLeft);
        if (isLeft) {
            cache.requestLeftCache();
        } else {
            cache.requestRightCache();
        }
        if (cached != null) {
            cacheHits++;
            if (isLeft) {
                cache.hitLeftCache();
            } else {
                cache.hitRightCache();
            }
            return new ArrayList<>(cached.values());
        }
        (isLeft ? leftLoads : rightLoads).put(key, false);
        return null;
    }

    /** Reads all rows of one input with the join key, from the cache or by a lookup. */
    private void readSide(
            RowData key,
            boolean isLeft,
            Consumer<List<RowData>> onRows,
            Consumer<Throwable> onError)
            throws Exception {
        List<RowData> cached = fromCache(key, isLeft);
        if (cached != null) {
            onRows.accept(cached);
            return;
        }
        lookups++;
        (isLeft ? groups.leftLookup : groups.rightLookup)
                .read(
                        key,
                        rows -> {
                            try {
                                if (cache != null) {
                                    installLoad(key, rows, isLeft);
                                }
                            } catch (Throwable t) {
                                onError.accept(t);
                                return;
                            }
                            onRows.accept(rows);
                        },
                        onError);
    }

    /** Caches rows that were looked up, unless a change of the key arrived during the lookup. */
    private void installLoad(RowData key, List<RowData> rows, boolean isLeft) throws Exception {
        Boolean changedMeanwhile = (isLeft ? leftLoads : rightLoads).remove(key);
        // a change that arrived during the load may or may not be in the loaded rows; large groups
        // are not cached, so that one key cannot fill the memory
        if (Boolean.FALSE.equals(changedMeanwhile)
                && rows.size() <= MAX_CACHED_ROWS
                && cache.getData(key, !isLeft) == null) {
            RowDataKeySelector uk = isLeft ? leftUniqueKey : rightUniqueKey;
            LinkedHashMap<RowData, RowData> byKey = new LinkedHashMap<>();
            for (RowData row : rows) {
                byKey.put(uk.getKey(row), row);
            }
            cache.buildCache(key, byKey, !isLeft);
        }
    }

    // ------------------------------------------------------------------------------------------
    // refreshes of the rows of changed children
    // ------------------------------------------------------------------------------------------

    @Override
    protected void discardScope(RowData key) {
        if (scopes != null) {
            scopes.remove(key);
        }
    }

    @Override
    protected boolean startScopedRefresh(RowData key) throws Exception {
        if (scopedSide < 0) {
            return false;
        }
        Scope scope = scopes.remove(key);
        if (scope == null || scope.changes == null) {
            // a change of the parent, too many children, or a key restored from a checkpoint
            return false;
        }
        readSide(
                key,
                scopedSide != 0,
                parents -> {
                    try {
                        refreshChildren(key, scope, parents);
                    } catch (Throwable t) {
                        fail(t);
                    }
                },
                this::fail);
        return true;
    }

    /**
     * Joins the changed children with the parent, and reads the rows they and the parent emitted
     * before.
     */
    private void refreshChildren(RowData key, Scope scope, List<RowData> parents) throws Exception {
        if (parents.size() > 1) {
            refreshWholeGroup(key);
            return;
        }
        RowData parent = parents.isEmpty() ? null : parents.get(0);
        // the key of the parent is part of the join key: the row keys the parent and its children
        // emitted are known even if the parent is gone now
        RowData parentKey = parentSkeleton(key);
        boolean childPreserved = joiner.preserves(scopedSide == 0);
        boolean parentPreserved = joiner.preserves(scopedSide != 0);
        // the row keys the changed children may have emitted before, with what they are
        Map<RowData, Boolean> matchedBefore = new LinkedHashMap<>();
        List<RowData[]> lines = new ArrayList<>();
        boolean parentMatched = false;
        for (Map.Entry<RowData, RowData> change : scope.changes.entrySet()) {
            RowData skeleton = skeleton(change.getKey(), key);
            // the parent did not change since the last refresh: a child matched it or was padded
            if (!addRowKey(matchedBefore, line(skeleton, parentKey), true)) {
                refreshWholeGroup(key);
                return;
            }
            if (childPreserved && !addRowKey(matchedBefore, line(skeleton, null), false)) {
                refreshWholeGroup(key);
                return;
            }
            RowData child = change.getValue();
            if (child == null) {
                continue;
            }
            RowData[] matched = line(child, parent);
            if (parent != null && joiner.matches(matched[0], matched[1])) {
                lines.add(matched);
                parentMatched = true;
            } else if (childPreserved) {
                lines.add(line(child, null));
            }
        }
        RowData paddedParent = null;
        if (parentPreserved) {
            paddedParent = rowKeyOf(line(null, parentKey));
            if (paddedParent == null) {
                refreshWholeGroup(key);
                return;
            }
        }
        Set<RowData> rowKeys = new LinkedHashSet<>(matchedBefore.keySet());
        if (paddedParent != null) {
            rowKeys.add(paddedParent);
        }
        RowData padded = paddedParent;
        // without a parent, the row of the parent without children must go if it was there
        boolean matchedNow = parentMatched || parent == null;
        readPreviousRows(
                key,
                rowKeys,
                previous -> {
                    try {
                        completeChildren(
                                key, rowKeys, previous, lines, matchedBefore, padded, matchedNow);
                    } catch (Throwable t) {
                        fail(t);
                    }
                },
                this::fail);
    }

    private void completeChildren(
            RowData key,
            Set<RowData> rowKeys,
            Map<RowData, BinaryRowData> previous,
            List<RowData[]> lines,
            Map<RowData, Boolean> matchedBefore,
            @Nullable RowData paddedParent,
            boolean parentMatched)
            throws Exception {
        if (paddedParent != null && !parentMatched) {
            // the row of the parent without children: the changed children decide it only if one
            // of them matched the parent before, and then it may have been the last one
            for (Map.Entry<RowData, Boolean> rowKey : matchedBefore.entrySet()) {
                if (rowKey.getValue() && previous.containsKey(rowKey.getKey())) {
                    refreshWholeGroup(key);
                    return;
                }
            }
            rowKeys.remove(paddedParent);
            previous.remove(paddedParent);
        }
        completeScopedRefresh(key, rowKeys, previous, lines);
    }

    /** Adds the row key of the line; false if the line has none. */
    private boolean addRowKey(Map<RowData, Boolean> rowKeys, RowData[] line, boolean matched)
            throws Exception {
        RowData rowKey = rowKeyOf(line);
        if (rowKey == null) {
            return false;
        }
        // a row key of both kinds counts as matched, so that losing it is never missed
        rowKeys.merge(rowKey, matched, Boolean::logicalOr);
        return true;
    }

    /** The line of a child and a parent, in the order of the inputs. */
    private RowData[] line(@Nullable RowData child, @Nullable RowData parent) {
        return scopedSide == 0 ? new RowData[] {child, parent} : new RowData[] {parent, child};
    }

    /** A parent that only has its join key, which holds its unique key. */
    private RowData parentSkeleton(RowData joinKey) {
        GenericRowData row = new GenericRowData(parentArity);
        for (int i = 0; i < parentJoinKeyGetters.length; i++) {
            row.setField(groups.parentJoinKey[i], parentJoinKeyGetters[i].getFieldOrNull(joinKey));
        }
        return row;
    }

    /** A child that only has its unique key and its join key, which make up its row keys. */
    private RowData skeleton(RowData uniqueKey, RowData joinKey) {
        GenericRowData row = new GenericRowData(scopedArity);
        for (int i = 0; i < scopedUniqueKeyGetters.length; i++) {
            row.setField(
                    groups.scopedUniqueKey[i], scopedUniqueKeyGetters[i].getFieldOrNull(uniqueKey));
        }
        for (int i = 0; i < scopedJoinKeyGetters.length; i++) {
            row.setField(groups.scopedJoinKey[i], scopedJoinKeyGetters[i].getFieldOrNull(joinKey));
        }
        return row;
    }

    @Override
    protected long numLookups() {
        return lookups;
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputEnded[inputId - 1] = true;
        if (inputEnded[0] && inputEnded[1]) {
            waitUntilIdle();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        groups.leftLookup.close();
        groups.rightLookup.close();
        if (condition != null) {
            FunctionUtils.closeFunction(condition);
        }
    }

    private static boolean hasNull(RowData key) {
        for (int i = 0; i < key.getArity(); i++) {
            if (key.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    long getLookups() {
        return lookups;
    }

    @VisibleForTesting
    long getCacheHits() {
        return cacheHits;
    }

    /** The rows of one input read so far, and the pages that follow, if any. */
    private static final class Side {
        private final List<RowData> rows;
        private final @Nullable GroupLookup.Pages more;

        private Side(List<RowData> rows, @Nullable GroupLookup.Pages more) {
            this.rows = rows;
            this.more = more;
        }
    }

    /** The lines of a group in pages of one input, joined with all rows of the other. */
    private final class JoinPages implements LinePages {
        private @Nullable List<RowData> firstPage;
        private final GroupLookup.Pages more;
        private final boolean pagedLeft;
        private final List<RowData> other;
        private final boolean[] otherMatched;

        private JoinPages(Side paged, boolean pagedLeft, List<RowData> other) {
            this.firstPage = paged.rows;
            this.more = paged.more;
            this.pagedLeft = pagedLeft;
            this.other = other;
            this.otherMatched = new boolean[other.size()];
        }

        @Override
        public void next(
                BiConsumer<List<RowData[]>, Boolean> onLines, Consumer<Throwable> onError) {
            if (firstPage != null) {
                // the first page is never the last, or the input would have been read whole
                List<RowData> page = firstPage;
                firstPage = null;
                join(page, false, onLines, onError);
                return;
            }
            more.next((rows, last) -> join(rows, last, onLines, onError), onError);
        }

        private void join(
                List<RowData> page,
                boolean last,
                BiConsumer<List<RowData[]>, Boolean> onLines,
                Consumer<Throwable> onError) {
            List<RowData[]> lines;
            try {
                lines = joiner.joinPart(page, pagedLeft, other, otherMatched);
                if (last) {
                    lines.addAll(joiner.padUnmatched(other, !pagedLeft, otherMatched));
                }
            } catch (Throwable t) {
                more.close();
                onError.accept(t);
                return;
            }
            onLines.accept(lines, last);
        }
    }

    /** The changes of a group since its refresh last started. */
    private static final class Scope {
        /** The latest version of every changed child, null if deleted; null if refreshed whole. */
        private @Nullable LinkedHashMap<RowData, RowData> changes = new LinkedHashMap<>();

        private void makeWhole() {
            changes = null;
        }
    }
}
