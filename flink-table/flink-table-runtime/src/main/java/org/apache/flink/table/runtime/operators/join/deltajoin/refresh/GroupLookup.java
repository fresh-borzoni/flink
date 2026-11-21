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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.connector.source.lookup.SupportsVersionedLookup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.CalcCollectionCollector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Reads the current rows of one table whose key equals a given key, by a lookup.
 *
 * <p>The lookup function takes the key row as its input. A connector may return rows that match
 * only a part of the key (for example, an index on a prefix of the key), so rows of other keys are
 * dropped here. The optional calc applies the filters and projections that sit between the table
 * and the join. Results are handed over in the task thread.
 *
 * <p>A lookup that does not complete within the timeout fails, so that a lost request fails the job
 * instead of stalling its join key forever.
 *
 * <p>If the lookup function also implements {@link SupportsPagedLookup}, the rows of a key can be
 * read in pages through the same function instance, see {@link #openPages}.
 */
public final class GroupLookup implements Serializable {

    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher;
    private final DataType lookupResultType;
    private final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;
    private final RowDataSerializer resultSerializer;
    private final RowDataKeySelector resultKeySelector;

    /** The fields of the key row that are the lookup keys, in the order of the lookup function. */
    private final @Nullable int[] pageKeyFields;

    private final @Nullable LogicalType[] pageKeyTypes;

    private transient AsyncFunction<RowData, Object> fetcher;
    private transient DataStructureConverter<Object, Object> converter;
    private transient RowDataSerializer lookupSerializer;
    private transient @Nullable FlatMapFunction<RowData, RowData> calc;
    private transient @Nullable CalcCollectionCollector calcCollector;
    private transient MailboxExecutor mailboxExecutor;
    private transient ProcessingTimeService timerService;
    private transient long timeoutMs;
    private transient RowDataKeySelector keySelector;
    private transient RowData.FieldGetter[] pageKeyGetters;
    private transient @Nullable SupportsPagedLookup pagedFunction;

    /**
     * @param generatedFetcher the lookup function; its input is the key row
     * @param lookupResultType the type of the rows the lookup function returns
     * @param generatedCalc filters and projections applied to the looked-up rows, if any
     * @param resultSerializer serializer of the rows handed over (after the calc)
     * @param resultKeySelector selects the key from a row handed over
     */
    public GroupLookup(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataType lookupResultType,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            RowDataSerializer resultSerializer,
            RowDataKeySelector resultKeySelector) {
        this(
                generatedFetcher,
                lookupResultType,
                generatedCalc,
                resultSerializer,
                resultKeySelector,
                null,
                null);
    }

    /**
     * @param pageKeyFields if the lookup function implements {@link SupportsPagedLookup}, the
     *     fields of the key row that it takes as its key row, in its order; otherwise null
     * @param pageKeyTypes the types of these fields
     */
    public GroupLookup(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataType lookupResultType,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            RowDataSerializer resultSerializer,
            RowDataKeySelector resultKeySelector,
            @Nullable int[] pageKeyFields,
            @Nullable LogicalType[] pageKeyTypes) {
        Preconditions.checkArgument(
                (pageKeyFields == null) == (pageKeyTypes == null),
                "Pages need the key fields and their types.");
        this.generatedFetcher = generatedFetcher;
        this.lookupResultType = lookupResultType;
        this.generatedCalc = generatedCalc;
        this.resultSerializer = resultSerializer;
        this.resultKeySelector = resultKeySelector;
        this.pageKeyFields = pageKeyFields;
        this.pageKeyTypes = pageKeyTypes;
    }

    /**
     * @param timerService the timers of the task, which fire in the task thread
     * @param timeoutMs how long a lookup may take; 0 or less for no limit
     */
    public void open(
            RuntimeContext runtimeContext,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService timerService,
            long timeoutMs)
            throws Exception {
        ClassLoader cl = runtimeContext.getUserCodeClassLoader();
        this.mailboxExecutor = mailboxExecutor;
        this.timerService = timerService;
        this.timeoutMs = timeoutMs;
        this.fetcher = generatedFetcher.newInstance(cl);
        FunctionUtils.setFunctionRuntimeContext(fetcher, runtimeContext);
        FunctionUtils.openFunction(fetcher, DefaultOpenContext.INSTANCE);
        this.converter = DataStructureConverters.getConverter(lookupResultType);
        converter.open(cl);
        this.lookupSerializer = new RowDataSerializer((RowType) lookupResultType.getLogicalType());
        if (generatedCalc != null) {
            this.calc = generatedCalc.newInstance(cl);
            FunctionUtils.setFunctionRuntimeContext(calc, runtimeContext);
            FunctionUtils.openFunction(calc, DefaultOpenContext.INSTANCE);
            this.calcCollector = new CalcCollectionCollector(resultSerializer);
        }
        this.keySelector = resultKeySelector.copy();
        if (pageKeyFields != null) {
            pagedFunction = pagedFunctionOf(generatedFetcher);
            pageKeyGetters = new RowData.FieldGetter[pageKeyFields.length];
            for (int i = 0; i < pageKeyFields.length; i++) {
                pageKeyGetters[i] = RowData.createFieldGetter(pageKeyTypes[i], pageKeyFields[i]);
            }
        }
    }

    /**
     * The lookup function of a generated fetcher if it implements {@link SupportsVersionedLookup},
     * else null. The fetcher opens and closes it.
     */
    public static @Nullable SupportsVersionedLookup versionedFunctionOrNull(
            GeneratedFunction<?> fetcher) {
        for (Object reference : fetcher.getReferences()) {
            if (reference instanceof SupportsVersionedLookup) {
                return (SupportsVersionedLookup) reference;
            }
        }
        return null;
    }

    /**
     * The lookup function of a generated fetcher, which implements {@link SupportsPagedLookup}. The
     * fetcher opens and closes it.
     */
    public static SupportsPagedLookup pagedFunctionOf(GeneratedFunction<?> fetcher) {
        for (Object reference : fetcher.getReferences()) {
            if (reference instanceof SupportsPagedLookup) {
                return (SupportsPagedLookup) reference;
            }
        }
        throw new IllegalStateException("The lookup function does not support pages.");
    }

    /** Whether the rows of a key can be read in pages. */
    public boolean supportsPages() {
        return pageKeyFields != null;
    }

    /** Opens the pages of the rows of the given key. Called in the task thread. */
    public Pages openPages(RowData key, int pageSize) {
        Preconditions.checkState(pagedFunction != null, "The lookup does not support pages.");
        GenericRowData keyRow = new GenericRowData(pageKeyGetters.length);
        for (int i = 0; i < pageKeyGetters.length; i++) {
            keyRow.setField(i, pageKeyGetters[i].getFieldOrNull(key));
        }
        return new Pages(key, pagedFunction.openCursor(keyRow, pageSize));
    }

    /**
     * Looks up the rows of the given key. Exactly one of the callbacks is called, in the task
     * thread.
     */
    public void read(RowData key, Consumer<List<RowData>> onRows, Consumer<Throwable> onError)
            throws Exception {
        AtomicBoolean done = new AtomicBoolean();
        ScheduledFuture<?> timeout =
                timeoutMs <= 0
                        ? null
                        : timerService.registerTimer(
                                timerService.getCurrentProcessingTime() + timeoutMs,
                                time -> {
                                    if (done.compareAndSet(false, true)) {
                                        onError.accept(
                                                new TimeoutException(
                                                        String.format(
                                                                "The lookup of key %s did not"
                                                                        + " complete within %s ms.",
                                                                key, timeoutMs)));
                                    }
                                });
        fetcher.asyncInvoke(
                key,
                new ResultFuture<Object>() {
                    @Override
                    public void complete(Collection<Object> result) {
                        if (!done.compareAndSet(false, true)) {
                            return;
                        }
                        if (timeout != null) {
                            timeout.cancel(false);
                        }
                        Collection<Object> found =
                                result == null ? Collections.emptyList() : result;
                        mailboxExecutor.execute(
                                () -> {
                                    List<RowData> matching;
                                    try {
                                        matching = keep(key, toInternal(found));
                                    } catch (Throwable t) {
                                        onError.accept(t);
                                        return;
                                    }
                                    onRows.accept(matching);
                                },
                                "Group lookup result for key %s",
                                key);
                    }

                    @Override
                    public void completeExceptionally(Throwable error) {
                        if (!done.compareAndSet(false, true)) {
                            return;
                        }
                        if (timeout != null) {
                            timeout.cancel(false);
                        }
                        mailboxExecutor.execute(
                                () -> onError.accept(error), "Group lookup failure for %s", key);
                    }

                    @Override
                    public void complete(CollectionSupplier<Object> supplier) {
                        Collection<Object> result;
                        try {
                            result = supplier.get();
                        } catch (Throwable t) {
                            completeExceptionally(t);
                            return;
                        }
                        complete(result);
                    }
                });
    }

    /** Converts the rows in the task thread, one by one, as a converter may reuse its output. */
    private List<RowData> toInternal(Collection<Object> found) {
        List<RowData> rows = new ArrayList<>(found.size());
        for (Object o : found) {
            RowData row =
                    (RowData) (converter.isIdentityConversion() ? o : converter.toInternal(o));
            rows.add(converter.isIdentityConversion() ? row : lookupSerializer.copy(row));
        }
        return rows;
    }

    public void close() throws Exception {
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (calc != null) {
            FunctionUtils.closeFunction(calc);
        }
    }

    /** The pages of the rows of one key. Used in the task thread only. */
    public final class Pages implements GroupRefreshOperatorBase.RowPages {

        private final RowData key;
        private final SupportsPagedLookup.LookupCursor cursor;
        private boolean closed;

        private Pages(RowData key, SupportsPagedLookup.LookupCursor cursor) {
            this.key = key;
            this.cursor = cursor;
        }

        @Override
        public void next(BiConsumer<List<RowData>, Boolean> onRows, Consumer<Throwable> onError) {
            Preconditions.checkState(!closed, "The pages of key %s are closed.", key);
            AtomicBoolean done = new AtomicBoolean();
            ScheduledFuture<?> timeout =
                    timeoutMs <= 0
                            ? null
                            : timerService.registerTimer(
                                    timerService.getCurrentProcessingTime() + timeoutMs,
                                    time -> {
                                        if (done.compareAndSet(false, true)) {
                                            close();
                                            onError.accept(
                                                    new TimeoutException(
                                                            String.format(
                                                                    "A page of key %s did not"
                                                                            + " complete within %s ms.",
                                                                    key, timeoutMs)));
                                        }
                                    });
            CompletableFuture<SupportsPagedLookup.LookupPage> page;
            try {
                page = cursor.nextPage();
            } catch (Throwable t) {
                done.set(true);
                if (timeout != null) {
                    timeout.cancel(false);
                }
                close();
                onError.accept(t);
                return;
            }
            page.whenComplete(
                    (rows, error) -> {
                        if (!done.compareAndSet(false, true)) {
                            return;
                        }
                        if (timeout != null) {
                            timeout.cancel(false);
                        }
                        mailboxExecutor.execute(
                                () -> {
                                    if (error != null) {
                                        close();
                                        onError.accept(error);
                                        return;
                                    }
                                    boolean last = rows.isLast();
                                    List<RowData> matching;
                                    try {
                                        matching = keep(key, new ArrayList<>(rows.getRows()));
                                    } catch (Throwable t) {
                                        close();
                                        onError.accept(t);
                                        return;
                                    }
                                    if (last) {
                                        close();
                                    }
                                    onRows.accept(matching, last);
                                },
                                "Group lookup page for key %s",
                                key);
                    });
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                cursor.close();
            }
        }
    }

    private List<RowData> keep(RowData key, List<RowData> rows) throws Exception {
        List<RowData> afterCalc;
        if (calc == null) {
            afterCalc = rows;
        } else {
            calcCollector.reset();
            for (RowData row : rows) {
                calc.flatMap(row, calcCollector);
            }
            afterCalc = new ArrayList<>(calcCollector.getCollection());
        }
        List<RowData> matching = new ArrayList<>(afterCalc.size());
        for (RowData row : afterCalc) {
            if (key.equals(keySelector.getKey(row))) {
                matching.add(resultSerializer.copy(row));
            }
        }
        return matching;
    }
}
