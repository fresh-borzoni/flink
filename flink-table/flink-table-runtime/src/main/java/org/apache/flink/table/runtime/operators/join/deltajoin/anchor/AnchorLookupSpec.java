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
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.connector.source.lookup.SupportsPagedLookup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A lookup into one input table, keyed by fields of a row of another table (or of an anchor key
 * row).
 *
 * <p>The fetcher calls the table's async lookup function with the key fields of the given row. The
 * optional calc applies the table's projection and filter, so that the result has the same type as
 * the table's rows in the input stream.
 */
public final class AnchorLookupSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<AsyncFunction<RowData, Object>> fetcher;
    private final DataType fetcherResultType;
    private final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> calc;
    private final RowType resultType;
    private final int[] sourceKeyFields;
    private final LogicalType[] sourceKeyTypes;
    private final String description;

    /** Whether the lookup function of the fetcher also returns the rows of a key in pages. */
    private final boolean paged;

    /**
     * @param sourceKeyFields the fields of the source row that the fetcher uses as lookup keys; a
     *     row with a NULL in any of them matches nothing and is not looked up
     * @param sourceKeyTypes the types of those fields
     */
    public AnchorLookupSpec(
            GeneratedFunction<AsyncFunction<RowData, Object>> fetcher,
            DataType fetcherResultType,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> calc,
            RowType resultType,
            int[] sourceKeyFields,
            LogicalType[] sourceKeyTypes,
            String description) {
        this(
                fetcher,
                fetcherResultType,
                calc,
                resultType,
                sourceKeyFields,
                sourceKeyTypes,
                description,
                false);
    }

    /**
     * @param paged whether the lookup function of the fetcher implements {@link
     *     SupportsPagedLookup}; it takes the source key fields as its key row
     */
    public AnchorLookupSpec(
            GeneratedFunction<AsyncFunction<RowData, Object>> fetcher,
            DataType fetcherResultType,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> calc,
            RowType resultType,
            int[] sourceKeyFields,
            LogicalType[] sourceKeyTypes,
            String description,
            boolean paged) {
        this.paged = paged;
        this.fetcher = fetcher;
        this.fetcherResultType = fetcherResultType;
        this.calc = calc;
        this.resultType = resultType;
        this.sourceKeyFields = sourceKeyFields;
        this.sourceKeyTypes = sourceKeyTypes;
        this.description = description;
    }

    public GeneratedFunction<AsyncFunction<RowData, Object>> getFetcher() {
        return fetcher;
    }

    public DataType getFetcherResultType() {
        return fetcherResultType;
    }

    @Nullable
    public GeneratedFunction<FlatMapFunction<RowData, RowData>> getCalc() {
        return calc;
    }

    public RowType getResultType() {
        return resultType;
    }

    public int[] getSourceKeyFields() {
        return sourceKeyFields;
    }

    public LogicalType[] getSourceKeyTypes() {
        return sourceKeyTypes;
    }

    public String getDescription() {
        return description;
    }

    /** Whether the rows of a key can be read in pages. */
    public boolean isPaged() {
        return paged;
    }

    @Override
    public String toString() {
        return description;
    }
}
