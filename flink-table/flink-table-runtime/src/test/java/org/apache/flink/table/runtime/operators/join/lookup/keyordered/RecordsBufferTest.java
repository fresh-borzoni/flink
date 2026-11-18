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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.junit.jupiter.api.Test;

import java.util.Deque;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordsBuffer}. */
class RecordsBufferTest {

    @Test
    void testPendingElementsKeepArrivalOrder() {
        RecordsBuffer<String, String> buffer = new RecordsBuffer<>();
        // v1 finished but is not emitted yet, v2 is in flight, v3 waits behind v2
        runAndFinish(buffer, "k", "v1");
        buffer.enqueueRecord("k", "v2");
        buffer.pop("k");
        buffer.enqueueRecord("k", "v3");

        assertThat(buffer.pendingElements()).containsOnlyKeys("k");
        assertThat(buffer.pendingElements().get("k")).containsExactly("v1", "v2", "v3");
    }

    @Test
    void testPendingElementsOfEachBufferAlone() {
        RecordsBuffer<String, String> buffer = new RecordsBuffer<>();
        runAndFinish(buffer, "finished", "f1");
        runAndFinish(buffer, "finished", "f2");
        buffer.enqueueRecord("active", "a1");
        buffer.pop("active");
        buffer.enqueueRecord("blocking", "b1");
        buffer.enqueueRecord("blocking", "b2");

        Map<String, Deque<String>> pending = buffer.pendingElements();
        assertThat(pending).containsOnlyKeys("finished", "active", "blocking");
        assertThat(pending.get("finished")).containsExactly("f1", "f2");
        assertThat(pending.get("active")).containsExactly("a1");
        assertThat(pending.get("blocking")).containsExactly("b1", "b2");
    }

    @Test
    void testPendingElementsDoNotChangeTheBuffer() {
        RecordsBuffer<String, String> buffer = new RecordsBuffer<>();
        runAndFinish(buffer, "k", "v1");
        buffer.enqueueRecord("k", "v2");
        buffer.pop("k");
        buffer.enqueueRecord("k", "v3");

        Map<String, Deque<String>> pending = buffer.pendingElements();
        pending.get("k").clear();

        assertThat(buffer.getFinishedBuffer().get("k")).containsExactly("v1");
        assertThat(buffer.getActiveBuffer()).containsEntry("k", "v2");
        assertThat(buffer.getBlockingBuffer().get("k")).containsExactly("v3");

        // the remaining records are processed and emitted after the snapshot
        buffer.finish("k", "v2");
        buffer.output("k", "v1");
        buffer.output("k", "v2");
        buffer.pop("k");
        buffer.finish("k", "v3");
        buffer.output("k", "v3");

        assertThat(buffer.pendingElements()).isEmpty();
        assertThat(buffer.getFinishedBuffer()).isEmpty();
        assertThat(buffer.getFinishSize()).isZero();
    }

    private static void runAndFinish(RecordsBuffer<String, String> buffer, String key, String v) {
        buffer.enqueueRecord(key, v);
        assertThat(buffer.pop(key)).hasValue(v);
        buffer.finish(key, v);
    }
}
