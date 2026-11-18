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

package org.apache.flink.table.connector.source.lookup;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A lookup function that can also return the rows of one key in pages, for keys with more rows than
 * a caller wants to hold at once. An {@link AsyncLookupFunction} implements it in addition to its
 * lookups; callers that do not page keep using {@link AsyncLookupFunction#asyncLookup}.
 *
 * <p>The pages of one cursor together return every row that matches the key, each once. Whether
 * they read one consistent state of the table is up to the connector: rows that change while a
 * cursor is open may be missed or returned in two versions, unless the connector reads all pages
 * from the state the cursor opened on.
 */
@PublicEvolving
public interface SupportsPagedLookup {

    /**
     * Opens a cursor over the rows that match the key row, which holds the lookup keys in the order
     * of {@link LookupTableSource.LookupContext#getKeys()}, like the key row of {@link
     * AsyncLookupFunction#asyncLookup}.
     *
     * @param pageSize the most rows a page should hold
     */
    LookupCursor openCursor(RowData keyRow, int pageSize);

    /** The pages of the rows of one key. Not thread safe. */
    @PublicEvolving
    interface LookupCursor extends AutoCloseable {

        /**
         * The next page. A caller asks for the next page only after the previous one completed, and
         * not after the last one.
         */
        CompletableFuture<LookupPage> nextPage();

        /** Releases the resources of the cursor. May be called before the last page was read. */
        @Override
        void close();
    }

    /** One page of rows. */
    @PublicEvolving
    final class LookupPage {

        private final Collection<RowData> rows;
        private final boolean last;

        private LookupPage(Collection<RowData> rows, boolean last) {
            this.rows = rows;
            this.last = last;
        }

        /**
         * @param rows the rows of the page, about the page size of them
         * @param last whether no rows follow
         */
        public static LookupPage of(Collection<RowData> rows, boolean last) {
            return new LookupPage(rows, last);
        }

        public Collection<RowData> getRows() {
            return rows;
        }

        public boolean isLast() {
            return last;
        }
    }
}
