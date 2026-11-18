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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshScheduler}. */
class RefreshSchedulerTest {

    private final List<String> started = new ArrayList<>();
    private final List<String> watermarks = new ArrayList<>();

    private RefreshScheduler<String, String> scheduler(int capacity) {
        return new RefreshScheduler<>(capacity, started::add, watermarks::add);
    }

    @Test
    void testChangesDuringARefreshMergeIntoOneMore() {
        RefreshScheduler<String, String> scheduler = scheduler(4);
        scheduler.note("a");
        scheduler.note("a");
        scheduler.note("a");
        assertThat(started).containsExactly("a");

        scheduler.finished("a");
        assertThat(started).containsExactly("a", "a");
        scheduler.finished("a");
        assertThat(started).containsExactly("a", "a");
        assertThat(scheduler.isIdle()).isTrue();
    }

    @Test
    void testKeysWaitForCapacityOnceEach() {
        RefreshScheduler<String, String> scheduler = scheduler(1);
        scheduler.note("a");
        scheduler.note("b");
        scheduler.note("b");
        scheduler.note("c");
        assertThat(started).containsExactly("a");
        assertThat(scheduler.numWaiting()).isEqualTo(2);

        scheduler.finished("a");
        scheduler.finished("b");
        scheduler.finished("c");
        assertThat(started).containsExactly("a", "b", "c");
        assertThat(scheduler.isIdle()).isTrue();
    }

    @Test
    void testBusyKeyDoesNotStarveWaitingKeys() {
        RefreshScheduler<String, String> scheduler = scheduler(1);
        scheduler.note("hot");
        scheduler.note("cold");
        scheduler.note("hot");
        scheduler.finished("hot");
        assertThat(started).containsExactly("hot", "cold");
        scheduler.finished("cold");
        assertThat(started).containsExactly("hot", "cold", "hot");
    }

    @Test
    void testWatermarkWaitsForRefreshesOfEarlierChanges() {
        RefreshScheduler<String, String> scheduler = scheduler(1);
        scheduler.note("a");
        scheduler.note("b");
        scheduler.watermark("w1");
        // later changes do not hold the watermark back
        scheduler.note("c");
        assertThat(watermarks).isEmpty();

        scheduler.finished("a");
        assertThat(watermarks).isEmpty();
        scheduler.finished("b");
        assertThat(watermarks).containsExactly("w1");
        scheduler.finished("c");
    }

    @Test
    void testWatermarkWaitsForTheRerunOfADirtyKey() {
        RefreshScheduler<String, String> scheduler = scheduler(2);
        scheduler.note("a");
        scheduler.note("a");
        scheduler.watermark("w1");
        scheduler.finished("a");
        // the change before the watermark arrived during the first refresh
        assertThat(watermarks).isEmpty();
        scheduler.finished("a");
        assertThat(watermarks).containsExactly("w1");
    }

    @Test
    void testWatermarkOfAnIdleSchedulerPassesAtOnce() {
        RefreshScheduler<String, String> scheduler = scheduler(2);
        scheduler.watermark("w1");
        scheduler.note("a");
        scheduler.watermark("w2");
        assertThat(watermarks).containsExactly("w1");
        scheduler.finished("a");
        assertThat(watermarks).containsExactly("w1", "w2");
    }

    @Test
    void testFinishingAKeyThatDoesNotRunFails() {
        RefreshScheduler<String, String> scheduler = scheduler(1);
        scheduler.note("a");
        scheduler.note("b");
        assertThatThrownBy(() -> scheduler.finished("b")).isInstanceOf(IllegalStateException.class);
    }
}
