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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsReadBackAfterCheckpoint;
import org.apache.flink.table.connector.sink.abilities.SupportsReadYourWrites;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsEpochs;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.connector.source.lookup.SupportsVersionedLookup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test connector for tables whose rows change while a job looks them up, like the primary-key
 * tables of a log-structured store.
 *
 * <p>Every table belongs to a store and has a script of upserts and deletes. The source applies
 * each change to the store's current state and only then emits it, with {@code UPDATE_BEFORE} and
 * full deletes. Lookups read the current state after a random delay. So a lookup issued after a
 * change was emitted always sees that change (it may see later ones too), and changes of different
 * tables interleave freely.
 *
 * <p>With {@code lookup.stale-reads} the lookups read a snapshot that lags behind the store, which
 * breaks that guarantee; tests use it to show what happens when a connector does not provide it.
 */
public class SharedStoreTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "shared-store";

    public static final ConfigOption<String> STORE =
            ConfigOptions.key("store").stringType().noDefaultValue();

    public static final ConfigOption<Integer> MAX_DELAY_MS =
            ConfigOptions.key("max-delay-ms").intType().defaultValue(2);

    public static final ConfigOption<Boolean> STALE_READS =
            ConfigOptions.key("lookup.stale-reads").booleanType().defaultValue(false);

    /** The source fails once after emitting this many changes; negative means never. */
    public static final ConfigOption<Integer> FAIL_AFTER =
            ConfigOptions.key("fail-after").intType().defaultValue(-1);

    /** The source declares and emits inserts only; the scripts must then only insert new keys. */
    public static final ConfigOption<Boolean> INSERT_ONLY =
            ConfigOptions.key("insert-only").booleanType().defaultValue(false);

    /**
     * The sink keeps its writes in a buffer and applies them to the store only when it is flushed,
     * before each checkpoint barrier and at the end of the input. Writes in the buffer of a failed
     * sink are lost.
     */
    public static final ConfigOption<Boolean> SINK_BUFFER_UNTIL_FLUSH =
            ConfigOptions.key("sink.buffer-until-flush").booleanType().defaultValue(false);

    /**
     * Whether the sink offers that the lookups of its task see what it wrote: a lookup first
     * applies the buffer of the writers of its table that run in its thread.
     */
    public static final ConfigOption<Boolean> SINK_READ_YOUR_WRITES =
            ConfigOptions.key("sink.read-your-writes").booleanType().defaultValue(true);

    /** Whether the sink needs the whole old row of a delete, as some key-value stores do. */
    public static final ConfigOption<Boolean> SINK_FULL_DELETES =
            ConfigOptions.key("sink.full-deletes").booleanType().defaultValue(false);

    // the writers that let the lookups of their task read what they wrote, by the store and table
    // they write; a task runs its writer and its lookups in one thread
    private static final ThreadLocal<Map<String, List<SinkWriter<?>>>> READ_YOUR_WRITES =
            ThreadLocal.withInitial(HashMap::new);

    private static final Map<String, Map<String, Table>> STORES = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> LOOKUPS = new ConcurrentHashMap<>();
    // the rows written into sink tables, by store and table, keyed by the primary key
    private static final Map<String, Map<String, Map<Row, Row>>> SINKS = new ConcurrentHashMap<>();

    /** Per store and epoch domain, when epoch 0 started: every source counts epochs from it. */
    private static final Map<String, Long> EPOCH_ZERO = new ConcurrentHashMap<>();

    /** The length of an epoch. */
    private static final long EPOCH_MILLIS = 10;

    /** Lookups read in more than one page. */
    private static final Map<String, AtomicLong> PAGED_LOOKUPS = new ConcurrentHashMap<>();

    /** The number of lookups of a store read in more than one page. */
    public static long pagedLookups(String store) {
        return PAGED_LOOKUPS.computeIfAbsent(store, s -> new AtomicLong()).get();
    }

    /** The number of lookups served by the tables of a store. */
    public static long lookups(String store) {
        return LOOKUPS.computeIfAbsent(store, s -> new AtomicLong()).get();
    }

    /**
     * Registers the script of a table: rows to upsert, or deletes of keys. The first field is the
     * primary key.
     */
    public static void register(String store, String table, List<Change> script) {
        Table t = new Table(script);
        Map<Object, GenericRowData> state = new HashMap<>();
        for (Change change : script) {
            Object key = change.row.getField(0);
            GenericRowData old = state.get(key);
            List<RowData> records = new ArrayList<>(2);
            if (change.delete) {
                if (old != null) {
                    state.remove(key);
                    records.add(withKind(old, RowKind.DELETE));
                }
            } else {
                state.put(key, change.row);
                if (old == null) {
                    records.add(withKind(change.row, RowKind.INSERT));
                } else {
                    records.add(withKind(old, RowKind.UPDATE_BEFORE));
                    records.add(withKind(change.row, RowKind.UPDATE_AFTER));
                }
            }
            t.changelog.add(records);
        }
        STORES.computeIfAbsent(store, s -> new ConcurrentHashMap<>()).put(table, t);
    }

    private static GenericRowData withKind(GenericRowData row, RowKind kind) {
        GenericRowData copy = new GenericRowData(kind, row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            copy.setField(i, row.getField(i));
        }
        return copy;
    }

    /** The current rows of a table. */
    public static Collection<GenericRowData> currentRows(String store, String table) {
        Table t = STORES.get(store).get(table);
        synchronized (t) {
            return new ArrayList<>(t.current.values());
        }
    }

    /**
     * Holds the changes of a table from a position on until the sources of another table have
     * applied its whole script, so that a script can change rows that the other table refers to.
     */
    public static void holdUntilApplied(
            String store, String table, int fromPosition, String otherTable) {
        Table t = table(store, table);
        synchronized (t) {
            t.holdFrom = fromPosition;
            t.holdUntilTable = otherTable;
        }
    }

    /**
     * Pauses the sources of a table before the change at the given position, until {@link #resume},
     * so that a job can be stopped with a savepoint between two parts of a script.
     */
    public static void pauseAt(String store, String table, int position) {
        Table t = table(store, table);
        synchronized (t) {
            t.pauseAt = position;
        }
    }

    public static void resume(String store, String table) {
        Table t = table(store, table);
        synchronized (t) {
            t.pauseAt = -1;
        }
    }

    /** The number of changes of the table's script that were applied to the store. */
    public static int applied(String store, String table) {
        Table t = table(store, table);
        synchronized (t) {
            return t.applied;
        }
    }

    private static boolean fullyApplied(String store, String table) {
        Table t = table(store, table);
        synchronized (t) {
            return t.applied >= t.script.size();
        }
    }

    /** Whether a source of the table resumed from a checkpoint. */
    public static boolean restoredFromCheckpoint(String store, String table) {
        Table t = table(store, table);
        synchronized (t) {
            return t.restoredFromCheckpoint;
        }
    }

    public static void clear(String store) {
        EPOCH_ZERO.keySet().removeIf(key -> key.startsWith(store + "/"));
        STORES.remove(store);
        LOOKUPS.remove(store);
        PAGED_LOOKUPS.remove(store);
        SINKS.remove(store);
    }

    /** The rows a sink table holds, printed like the rows of a query result. */
    public static List<String> sinkRows(String store, String table) {
        Map<Row, Row> rows = sinkTable(store, table);
        synchronized (rows) {
            List<String> result = new ArrayList<>();
            for (Row row : rows.values()) {
                result.add(row.toString());
            }
            return result;
        }
    }

    private static Map<Row, Row> sinkTable(String store, String table) {
        return SINKS.computeIfAbsent(store, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(table, t -> new LinkedHashMap<>());
    }

    private static Table table(String store, String table) {
        return Objects.requireNonNull(
                STORES.getOrDefault(store, Collections.emptyMap()).get(table),
                "No table " + table + " in store " + store);
    }

    /** An upsert of a full row, or a delete of the row with the key of {@code row}. */
    public static final class Change implements java.io.Serializable {
        public final boolean delete;
        public final GenericRowData row;

        private Change(boolean delete, GenericRowData row) {
            this.delete = delete;
            this.row = row;
        }

        public static Change upsert(Object... fields) {
            return new Change(false, GenericRowData.of(fields));
        }

        public static Change delete(Object... fields) {
            return new Change(true, GenericRowData.of(fields));
        }
    }

    private static final class Table {
        final List<Change> script;
        final Map<Object, GenericRowData> current = new LinkedHashMap<>();
        // the changelog of every change, computed when the table is registered, so that a source
        // that restarts from a checkpoint emits the same records again, like a replayed log
        final List<List<RowData>> changelog = new ArrayList<>();
        // the number of changes applied to the store; several scans of a table share it
        int applied;
        // whether the source has already failed once (see fail-after)
        boolean failed;
        // whether a source resumed from a checkpointed position
        boolean restoredFromCheckpoint;
        // the changes from this position on wait until the table holdUntilTable is fully applied
        int holdFrom = -1;
        String holdUntilTable;
        // the changes from this position on wait until the table is resumed
        int pauseAt = -1;
        // per epoch domain, the position after the last change of every epoch a source of the
        // table ended; a restored source ends them at the same positions again
        final Map<String, List<Integer>> epochEnds = new HashMap<>();
        // the epoch domains whose source emitted the whole script and ended its last epoch
        final Set<String> epochsDone = new HashSet<>();

        Table(List<Change> script) {
            this.script = script;
        }

        /** The state after the first {@code changes} changes, for stale reads. */
        Map<Object, GenericRowData> stateAfter(int changes) {
            Map<Object, GenericRowData> state = new HashMap<>();
            for (Change change : script.subList(0, changes)) {
                if (change.delete) {
                    state.remove(change.row.getField(0));
                } else {
                    state.put(change.row.getField(0), change.row);
                }
            }
            return state;
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STORE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MAX_DELAY_MS);
        options.add(STALE_READS);
        options.add(FAIL_AFTER);
        options.add(INSERT_ONLY);
        options.add(SINK_BUFFER_UNTIL_FLUSH);
        options.add(SINK_READ_YOUR_WRITES);
        options.add(SINK_FULL_DELETES);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        int[] primaryKey =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(
                                columns ->
                                        columns.stream()
                                                .mapToInt(schema.getColumnNames()::indexOf)
                                                .toArray())
                        .orElseThrow(
                                () -> new IllegalArgumentException("A primary key is needed."));
        return new StoreTableSource(
                helper.getOptions().get(STORE),
                context.getObjectIdentifier().getObjectName(),
                primaryKey,
                helper.getOptions().get(MAX_DELAY_MS),
                helper.getOptions().get(STALE_READS),
                helper.getOptions().get(FAIL_AFTER),
                helper.getOptions().get(INSERT_ONLY));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        int[] primaryKey =
                schema.getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(
                                columns ->
                                        columns.stream()
                                                .mapToInt(schema.getColumnNames()::indexOf)
                                                .toArray())
                        .orElseThrow(
                                () -> new IllegalArgumentException("A primary key is needed."));
        LogicalType[] types =
                schema.getColumnDataTypes().stream()
                        .map(type -> type.getLogicalType())
                        .toArray(LogicalType[]::new);
        return new StoreTableSink(
                helper.getOptions().get(STORE),
                context.getObjectIdentifier().getObjectName(),
                primaryKey,
                types,
                helper.getOptions().get(SINK_BUFFER_UNTIL_FLUSH),
                helper.getOptions().get(SINK_READ_YOUR_WRITES),
                helper.getOptions().get(SINK_FULL_DELETES));
    }

    /**
     * An upsert sink into the store, like a key-value store: it is not rolled back when the job
     * restores from a checkpoint, and a delete of a key it does not hold changes nothing. The rows
     * of a sink table can be looked up; rows written before a checkpoint barrier are applied when
     * the sink is flushed, before the checkpoint completes.
     */
    private static final class StoreTableSink
            implements DynamicTableSink, SupportsReadBackAfterCheckpoint, SupportsReadYourWrites {

        private final String store;
        private final String table;
        private final int[] primaryKey;
        private final LogicalType[] types;
        private final boolean bufferUntilFlush;
        private final boolean offersReadYourWrites;
        private final boolean fullDeletes;
        private boolean readYourWrites;

        private StoreTableSink(
                String store,
                String table,
                int[] primaryKey,
                LogicalType[] types,
                boolean bufferUntilFlush,
                boolean offersReadYourWrites,
                boolean fullDeletes) {
            this.store = store;
            this.table = table;
            this.primaryKey = primaryKey;
            this.types = types;
            this.bufferUntilFlush = bufferUntilFlush;
            this.offersReadYourWrites = offersReadYourWrites;
            this.fullDeletes = fullDeletes;
        }

        @Override
        public boolean applyReadYourWrites() {
            readYourWrites = offersReadYourWrites;
            return readYourWrites;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.upsert(!fullDeletes);
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkV2Provider.of(
                    new StoreSink(
                            store, table, primaryKey, types, bufferUntilFlush, readYourWrites));
        }

        @Override
        public DynamicTableSink copy() {
            StoreTableSink copy =
                    new StoreTableSink(
                            store,
                            table,
                            primaryKey,
                            types,
                            bufferUntilFlush,
                            offersReadYourWrites,
                            fullDeletes);
            copy.readYourWrites = readYourWrites;
            return copy;
        }

        @Override
        public String asSummaryString() {
            return IDENTIFIER;
        }
    }

    private static final class StoreSink implements Sink<RowData> {

        private static final long serialVersionUID = 1L;

        private final String store;
        private final String table;
        private final int[] primaryKey;
        private final RowData.FieldGetter[] getters;
        private final boolean bufferUntilFlush;
        private final boolean readYourWrites;

        private StoreSink(
                String store,
                String table,
                int[] primaryKey,
                LogicalType[] types,
                boolean bufferUntilFlush,
                boolean readYourWrites) {
            this.store = store;
            this.table = table;
            this.primaryKey = primaryKey;
            this.bufferUntilFlush = bufferUntilFlush;
            this.readYourWrites = readYourWrites;
            this.getters = new RowData.FieldGetter[types.length];
            for (int i = 0; i < types.length; i++) {
                getters[i] = RowData.createFieldGetter(types[i], i);
            }
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            Map<Row, Row> rows = sinkTable(store, table);
            // key and row of each buffered upsert; the row is null for a delete
            List<Row[]> buffer = new ArrayList<>();
            SinkWriter<RowData> writer =
                    new SinkWriter<RowData>() {
                        @Override
                        public void write(RowData element, Context context) {
                            Object[] key = new Object[primaryKey.length];
                            for (int i = 0; i < key.length; i++) {
                                key[i] = getters[primaryKey[i]].getFieldOrNull(element);
                            }
                            Row row = null;
                            if (element.getRowKind() == RowKind.INSERT
                                    || element.getRowKind() == RowKind.UPDATE_AFTER) {
                                Object[] fields = new Object[getters.length];
                                for (int i = 0; i < fields.length; i++) {
                                    fields[i] = getters[i].getFieldOrNull(element);
                                }
                                row = Row.of(fields);
                            }
                            buffer.add(new Row[] {Row.of(key), row});
                            if (!bufferUntilFlush) {
                                flush(false);
                            }
                        }

                        @Override
                        public void flush(boolean endOfInput) {
                            synchronized (rows) {
                                for (Row[] write : buffer) {
                                    if (write[1] != null) {
                                        rows.put(write[0], write[1]);
                                    } else {
                                        rows.remove(write[0]);
                                    }
                                }
                            }
                            buffer.clear();
                        }

                        @Override
                        public void close() {
                            if (readYourWrites) {
                                READ_YOUR_WRITES.get().get(store + "/" + table).remove(this);
                            }
                        }
                    };
            if (readYourWrites) {
                // created in the thread of the task, where its lookups run
                READ_YOUR_WRITES
                        .get()
                        .computeIfAbsent(store + "/" + table, key -> new ArrayList<>())
                        .add(writer);
            }
            return writer;
        }
    }

    private static final class StoreTableSource
            implements ScanTableSource, LookupTableSource, SupportsEpochs {

        private final String store;
        private final String table;
        private final int[] primaryKey;
        private final int maxDelayMs;
        private final boolean staleReads;
        private final int failAfter;
        private final boolean insertOnly;
        private @Nullable String epochDomain;

        private StoreTableSource(
                String store,
                String table,
                int[] primaryKey,
                int maxDelayMs,
                boolean staleReads,
                int failAfter,
                boolean insertOnly) {
            this.store = store;
            this.table = table;
            this.primaryKey = primaryKey;
            this.maxDelayMs = maxDelayMs;
            this.staleReads = staleReads;
            this.failAfter = failAfter;
            this.insertOnly = insertOnly;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return insertOnly ? ChangelogMode.insertOnly() : ChangelogMode.all();
        }

        @Override
        public boolean applyEpochs(String domain) {
            epochDomain = domain;
            return true;
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            return SourceProvider.of(
                    new StoreSource(store, table, maxDelayMs, failAfter, epochDomain), 1);
        }

        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
            int[] keyFields = new int[context.getKeys().length];
            for (int i = 0; i < keyFields.length; i++) {
                keyFields[i] = context.getKeys()[i][0];
            }
            return AsyncLookupFunctionProvider.of(
                    new StoreLookup(
                            store,
                            table,
                            keyFields,
                            maxDelayMs,
                            staleReads,
                            context.requiresReadYourWrites(),
                            epochDomain));
        }

        @Override
        public DynamicTableSource copy() {
            StoreTableSource copy =
                    new StoreTableSource(
                            store,
                            table,
                            primaryKey,
                            maxDelayMs,
                            staleReads,
                            failAfter,
                            insertOnly);
            copy.epochDomain = epochDomain;
            return copy;
        }

        @Override
        public String asSummaryString() {
            return IDENTIFIER;
        }
    }

    /**
     * Applies the script of a table to the store and emits the changelog of every change after the
     * change was applied. The table is one split whose position is checkpointed; after a restore
     * the source emits the changelog again from there without applying the changes a second time,
     * like a replayed log.
     */
    private static final class StoreSource implements Source<RowData, StoreSplit, Integer> {

        private static final long serialVersionUID = 1L;

        private final String store;
        private final String table;
        private final int maxDelayMs;
        private final int failAfter;
        private final @Nullable String epochDomain;

        private StoreSource(
                String store,
                String table,
                int maxDelayMs,
                int failAfter,
                @Nullable String epochDomain) {
            this.store = store;
            this.table = table;
            this.maxDelayMs = maxDelayMs;
            this.failAfter = failAfter;
            this.epochDomain = epochDomain;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SplitEnumerator<StoreSplit, Integer> createEnumerator(
                SplitEnumeratorContext<StoreSplit> context) {
            return new StoreEnumerator(context, table, false);
        }

        @Override
        public SplitEnumerator<StoreSplit, Integer> restoreEnumerator(
                SplitEnumeratorContext<StoreSplit> context, Integer assigned) {
            return new StoreEnumerator(context, table, assigned == 1);
        }

        @Override
        public SimpleVersionedSerializer<StoreSplit> getSplitSerializer() {
            return new StoreSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new AssignedSerializer();
        }

        @Override
        public SourceReader<RowData, StoreSplit> createReader(SourceReaderContext context) {
            return new StoreReader(context, store, table, maxDelayMs, failAfter, epochDomain);
        }
    }

    /** The changelog of one table from a position on. */
    private static final class StoreSplit implements SourceSplit {
        final String table;
        final int position;

        StoreSplit(String table, int position) {
            this.table = table;
            this.position = position;
        }

        @Override
        public String splitId() {
            return table;
        }
    }

    private static final class StoreSplitSerializer
            implements SimpleVersionedSerializer<StoreSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(StoreSplit split) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(32);
            out.writeUTF(split.table);
            out.writeInt(split.position);
            return out.getCopyOfBuffer();
        }

        @Override
        public StoreSplit deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new StoreSplit(in.readUTF(), in.readInt());
        }
    }

    private static final class AssignedSerializer implements SimpleVersionedSerializer<Integer> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Integer assigned) {
            return new byte[] {assigned.byteValue()};
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) {
            return (int) serialized[0];
        }
    }

    /** Hands out the single split of the table, again if a reader returns it. */
    private static final class StoreEnumerator implements SplitEnumerator<StoreSplit, Integer> {
        private final SplitEnumeratorContext<StoreSplit> context;
        private final String table;
        private final List<StoreSplit> returned = new ArrayList<>();
        private boolean assigned;

        StoreEnumerator(
                SplitEnumeratorContext<StoreSplit> context, String table, boolean assigned) {
            this.context = context;
            this.table = table;
            this.assigned = assigned;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (!returned.isEmpty()) {
                context.assignSplit(returned.remove(0), subtaskId);
            } else if (!assigned) {
                assigned = true;
                context.assignSplit(new StoreSplit(table, 0), subtaskId);
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<StoreSplit> splits, int subtaskId) {
            returned.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Integer snapshotState(long checkpointId) {
            return assigned ? 1 : 0;
        }

        @Override
        public void close() {}
    }

    /** Emits one change per call, with a random pause between changes. */
    private static final class StoreReader implements SourceReader<RowData, StoreSplit> {

        private final SourceReaderContext context;
        private final String store;
        private final String table;
        private final int maxDelayMs;
        private final int failAfter;
        private final Random random;
        private final ScheduledExecutorService timer;
        private final @Nullable String epochDomain;

        /** With epochs: the open epoch, or -1 until it is known after a restore. */
        private int epoch = -1;

        /** With epochs: the position the open epoch started at. */
        private int epochStart;

        private @Nullable StoreSplit split;
        private int position;
        private boolean noMoreSplits;
        private boolean checkpointCompleted;
        private CompletableFuture<Void> available = new CompletableFuture<>();

        StoreReader(
                SourceReaderContext context,
                String store,
                String table,
                int maxDelayMs,
                int failAfter,
                @Nullable String epochDomain) {
            this.context = context;
            this.epochDomain = epochDomain;
            this.store = store;
            this.table = table;
            this.maxDelayMs = maxDelayMs;
            this.failAfter = failAfter;
            this.random = new Random(Objects.hash(store, table));
            this.timer =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread thread = new Thread(r, "store-source-" + table);
                                thread.setDaemon(true);
                                return thread;
                            });
        }

        @Override
        public void start() {
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) {
            if (split == null) {
                return noMoreSplits ? InputStatus.END_OF_INPUT : waitFor(-1);
            }
            Table t = table(store, table);
            if (epochDomain != null) {
                InputStatus status = pollEpochs(output, t);
                if (status != null) {
                    return status;
                }
            } else if (position >= t.script.size()) {
                return InputStatus.END_OF_INPUT;
            }
            int holdFrom;
            String holdUntilTable;
            int pauseAt;
            synchronized (t) {
                holdFrom = t.holdFrom;
                holdUntilTable = t.holdUntilTable;
                pauseAt = t.pauseAt;
            }
            if (pauseAt >= 0 && position >= pauseAt) {
                return stall(output, t);
            }
            if (holdFrom >= 0 && position >= holdFrom && !fullyApplied(store, holdUntilTable)) {
                return stall(output, t);
            }
            synchronized (t) {
                if (failAfter >= 0 && position >= failAfter && !t.failed) {
                    if (!checkpointCompleted) {
                        // fail only after a checkpoint, so that the restore uses its state
                        return stall(output, t);
                    }
                    t.failed = true;
                    throw new RuntimeException("Injected failure of the source of " + table);
                }
                if (t.applied == position) {
                    Change change = t.script.get(position);
                    Object key = change.row.getField(0);
                    if (change.delete) {
                        t.current.remove(key);
                    } else {
                        t.current.put(key, change.row);
                    }
                    t.applied++;
                }
            }
            for (RowData row : t.changelog.get(position)) {
                if (epochDomain != null) {
                    // the epoch of a change is the timestamp of its records
                    output.collect(row, epoch);
                } else {
                    output.collect(row);
                }
            }
            position++;
            return waitFor(maxDelayMs > 0 ? random.nextInt(maxDelayMs + 1) : 0);
        }

        /**
         * Ends epochs: an epoch recorded before ends where it ended, and the open one ends at a
         * random position or at the end of the script, with a watermark of its number. Returns null
         * if the change at the position belongs to the open epoch.
         */
        private @Nullable InputStatus pollEpochs(ReaderOutput<RowData> output, Table t) {
            int size = t.script.size();
            synchronized (t) {
                List<Integer> ends =
                        t.epochEnds.computeIfAbsent(epochDomain, d -> new ArrayList<>());
                if (epoch < 0) {
                    // at the start or after a restore: the first epoch that did not end before
                    // the position, announced by a watermark of the epoch before it
                    epoch = 0;
                    while (epoch < ends.size() && ends.get(epoch) < position) {
                        epoch++;
                    }
                    if (epoch == ends.size()) {
                        // the epochs the clock passed before the source started hold no change
                        while (ends.size() < clockEpoch()) {
                            ends.add(position);
                        }
                        epoch = ends.size();
                    }
                    epochStart = position;
                    output.emitWatermark(new Watermark(epoch - 1));
                    return InputStatus.MORE_AVAILABLE;
                }
                if (epoch < ends.size()) {
                    if (position < ends.get(epoch)) {
                        return null;
                    }
                    output.emitWatermark(new Watermark(epoch));
                    epoch++;
                    epochStart = position;
                    return InputStatus.MORE_AVAILABLE;
                }
                boolean atEnd = position >= size;
                if (atEnd || clockEpoch() > epoch) {
                    endEpochsUntil(output, ends, Math.max(epoch, clockEpoch() - 1));
                    if (atEnd) {
                        t.epochsDone.add(epochDomain);
                        return InputStatus.END_OF_INPUT;
                    }
                    return InputStatus.MORE_AVAILABLE;
                }
                return null;
            }
        }

        /** The epoch of the shared clock: every source of the domain counts the same epochs. */
        private long clockEpoch() {
            long zero =
                    EPOCH_ZERO.computeIfAbsent(
                            store + "/" + epochDomain, d -> System.currentTimeMillis());
            return (System.currentTimeMillis() - zero) / EPOCH_MILLIS;
        }

        /** Ends the open epoch at the position, and the epochs up to the last, which are empty. */
        private void endEpochsUntil(ReaderOutput<RowData> output, List<Integer> ends, long last) {
            while (ends.size() <= last) {
                ends.add(position);
            }
            output.emitWatermark(new Watermark(last));
            epoch = (int) last + 1;
            epochStart = position;
        }

        /**
         * Waits a little before the change at the position. With epochs, the open epoch ends first:
         * lookups that read as of it must not wait for a source that stalls.
         */
        private InputStatus stall(ReaderOutput<RowData> output, Table t) {
            if (epochDomain != null && epoch >= 0) {
                synchronized (t) {
                    List<Integer> ends = t.epochEnds.get(epochDomain);
                    if (epoch == ends.size() && clockEpoch() > epoch) {
                        endEpochsUntil(output, ends, clockEpoch() - 1);
                    }
                }
            }
            return waitFor(5);
        }

        /** Returns NOTHING_AVAILABLE until the delay passed, or until notified if negative. */
        private InputStatus waitFor(int delayMs) {
            available = new CompletableFuture<>();
            if (delayMs >= 0) {
                CompletableFuture<Void> future = available;
                timer.schedule(() -> future.complete(null), delayMs, TimeUnit.MILLISECONDS);
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<StoreSplit> snapshotState(long checkpointId) {
            return split == null
                    ? Collections.emptyList()
                    : Collections.singletonList(new StoreSplit(table, position));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return available;
        }

        @Override
        public void addSplits(List<StoreSplit> splits) {
            split = splits.get(0);
            position = split.position;
            if (position > 0) {
                Table t = table(store, table);
                synchronized (t) {
                    t.restoredFromCheckpoint = true;
                }
            }
            available.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
            available.complete(null);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointCompleted = true;
        }

        @Override
        public void close() {
            timer.shutdownNow();
        }
    }

    /** Reads the current state of the table, after a random delay. */
    /**
     * Looks up the rows of a key in the store. It also reads them in pages: the first page reads
     * the current rows of the key, as a lookup does, and the pages hand them over.
     */
    public static final class StoreLookup extends AsyncLookupFunction
            implements SupportsPagedLookup, SupportsVersionedLookup {

        private static final long serialVersionUID = 1L;
        private static final AtomicInteger THREADS = new AtomicInteger();

        private final String store;
        private final String table;
        private final int[] keyFields;
        private final int maxDelayMs;
        private final boolean staleReads;
        private final boolean requiresWriter;
        private final @Nullable String epochDomain;

        private transient ExecutorService executor;
        private transient Random random;

        public StoreLookup(
                String store,
                String table,
                int[] keyFields,
                int maxDelayMs,
                boolean staleReads,
                boolean requiresWriter,
                @Nullable String epochDomain) {
            this.epochDomain = epochDomain;
            this.store = store;
            this.table = table;
            this.keyFields = keyFields;
            this.maxDelayMs = maxDelayMs;
            this.staleReads = staleReads;
            this.requiresWriter = requiresWriter;
        }

        @Override
        public void open(FunctionContext context) {
            // an instance shared by two callers would be opened twice and leak its resources
            Preconditions.checkState(executor == null, "The lookup of %s is opened twice.", table);
            executor =
                    Executors.newFixedThreadPool(
                            3,
                            r -> {
                                Thread thread =
                                        new Thread(r, "store-lookup-" + THREADS.incrementAndGet());
                                thread.setDaemon(true);
                                return thread;
                            });
            // seeded, so that a failing run repeats its delays
            random = new Random((store + "/" + table).hashCode());
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            // a writer of this task that writes the table: what it wrote is read first
            List<SinkWriter<?>> writers = READ_YOUR_WRITES.get().get(store + "/" + table);
            if (requiresWriter && (writers == null || writers.isEmpty())) {
                throw new IllegalStateException(
                        "No writer of table " + table + " runs in the task of its lookup.");
            }
            if (writers != null) {
                for (SinkWriter<?> writer : writers) {
                    try {
                        writer.flush(false);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
            LOOKUPS.computeIfAbsent(store, s -> new AtomicLong()).incrementAndGet();
            Object[] key = new Object[keyFields.length];
            for (int i = 0; i < key.length; i++) {
                key[i] = ((GenericRowData) keyRow).getField(i);
            }
            int delay;
            int lag;
            synchronized (this) {
                delay = maxDelayMs > 0 ? random.nextInt(maxDelayMs + 1) : 0;
                lag = random.nextInt(4);
            }
            return CompletableFuture.supplyAsync(
                    () -> {
                        if (delay > 0) {
                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        Map<String, Table> tables =
                                STORES.getOrDefault(store, Collections.emptyMap());
                        if (!tables.containsKey(table)) {
                            return lookupSink(key);
                        }
                        Table t = table(store, table);
                        Collection<GenericRowData> rows;
                        synchronized (t) {
                            rows =
                                    staleReads
                                            ? t.stateAfter(Math.max(0, t.applied - lag)).values()
                                            : t.current.values();
                            List<RowData> matches = new ArrayList<>();
                            for (GenericRowData row : rows) {
                                boolean match = true;
                                for (int i = 0; i < key.length && match; i++) {
                                    match = Objects.equals(row.getField(keyFields[i]), key[i]);
                                }
                                if (match) {
                                    matches.add(row);
                                }
                            }
                            return matches;
                        }
                    },
                    executor);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookupAsOf(RowData keyRow, long epoch) {
            Preconditions.checkState(
                    epochDomain != null, "The lookup of %s applied no epochs.", table);
            LOOKUPS.computeIfAbsent(store, s -> new AtomicLong()).incrementAndGet();
            Object[] key = new Object[keyFields.length];
            for (int i = 0; i < key.length; i++) {
                key[i] = ((GenericRowData) keyRow).getField(i);
            }
            CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> readAsOf(key, epoch, result), executor);
            return result;
        }

        /** Reads the rows of the key as of the end of the epoch, once a source ended it. */
        private void readAsOf(
                Object[] key, long epoch, CompletableFuture<Collection<RowData>> result) {
            Table t = table(store, table);
            synchronized (t) {
                List<Integer> ends = t.epochEnds.getOrDefault(epochDomain, Collections.emptyList());
                int end = -1;
                if (epoch < 0) {
                    end = 0;
                } else if (epoch < ends.size()) {
                    end = ends.get((int) epoch);
                } else if (t.epochsDone.contains(epochDomain)) {
                    end = t.script.size();
                }
                if (end >= 0) {
                    List<RowData> matches = new ArrayList<>();
                    for (GenericRowData row : t.stateAfter(end).values()) {
                        boolean match = true;
                        for (int i = 0; i < key.length && match; i++) {
                            match = Objects.equals(row.getField(keyFields[i]), key[i]);
                        }
                        if (match) {
                            matches.add(row);
                        }
                    }
                    result.complete(matches);
                    return;
                }
            }
            // the epoch did not end yet: read again soon
            CompletableFuture.runAsync(
                    () -> readAsOf(key, epoch, result),
                    CompletableFuture.delayedExecutor(2, TimeUnit.MILLISECONDS, executor));
        }

        @Override
        public LookupCursor openCursor(RowData keyRow, int pageSize) {
            return new LookupCursor() {
                private @Nullable List<RowData> rows;
                private int next;

                @Override
                public CompletableFuture<LookupPage> nextPage() {
                    CompletableFuture<Collection<RowData>> all =
                            rows == null
                                    ? asyncLookup(keyRow)
                                    : CompletableFuture.completedFuture(rows);
                    return all.thenApply(
                            found -> {
                                if (rows == null) {
                                    rows = new ArrayList<>(found);
                                }
                                int end = Math.min(rows.size(), next + pageSize);
                                List<RowData> page = new ArrayList<>(rows.subList(next, end));
                                if (next > 0 && next == pageSize) {
                                    PAGED_LOOKUPS
                                            .computeIfAbsent(store, s -> new AtomicLong())
                                            .incrementAndGet();
                                }
                                next = end;
                                return LookupPage.of(page, next >= rows.size());
                            });
                }

                @Override
                public void close() {}
            };
        }

        /** The rows of a sink table with the given key. */
        private Collection<RowData> lookupSink(Object[] key) {
            Map<Row, Row> rows = sinkTable(store, table);
            List<RowData> matches = new ArrayList<>();
            synchronized (rows) {
                for (Row row : rows.values()) {
                    boolean match = true;
                    for (int i = 0; i < key.length && match; i++) {
                        match = Objects.equals(row.getField(keyFields[i]), key[i]);
                    }
                    if (match) {
                        Object[] fields = new Object[row.getArity()];
                        for (int i = 0; i < fields.length; i++) {
                            fields[i] = row.getField(i);
                        }
                        matches.add(GenericRowData.of(fields));
                    }
                }
            }
            return matches;
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }
}
