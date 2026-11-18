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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeltaJoinCache}. */
class DeltaJoinCacheTest {

    @Test
    void testDeleteCacheRemovesOneRowOfACachedKey() {
        DeltaJoinCache cache = new DeltaJoinCache(10, 10);
        LinkedHashMap<RowData, RowData> rows = new LinkedHashMap<>();
        rows.put(GenericRowData.of(1), GenericRowData.of(1, 10));
        rows.put(GenericRowData.of(2), GenericRowData.of(2, 10));
        cache.buildCache(GenericRowData.of(10), rows, true);

        cache.deleteCache(GenericRowData.of(10), GenericRowData.of(1), true);
        // a row that is not cached, and a key that is not cached
        cache.deleteCache(GenericRowData.of(10), GenericRowData.of(3), true);
        cache.deleteCache(GenericRowData.of(11), GenericRowData.of(1), true);

        assertThat(cache.getData(GenericRowData.of(10), true))
                .containsOnlyKeys(GenericRowData.of(2));
        assertThat(cache.getData(GenericRowData.of(11), true)).isNull();
        assertThat(cache.getRightTotalSize().get()).isOne();
        assertThat(cache.getLeftCache().size()).isZero();
    }
}
