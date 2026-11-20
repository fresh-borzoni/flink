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

import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Schedules group refreshes per key: a refresh of a key never overlaps with another refresh of the
 * same key, and changes that arrive while a key is waiting or being refreshed are merged into one
 * further refresh.
 *
 * <ul>
 *   <li>A change for an idle key starts a refresh if fewer than {@code capacity} refreshes run,
 *       otherwise the key waits. A key waits at most once, however many changes it receives.
 *   <li>A change for a running key marks it dirty. When the refresh ends, a dirty key is refreshed
 *       again: that refresh starts after the change arrived, so it reads the change.
 *   <li>No refresh starts while the gate set by {@link #setGate} is closed.
 *   <li>A watermark is held back until every key that had work when the watermark arrived has
 *       finished a refresh that started after its last change before the watermark.
 * </ul>
 *
 * <p>This class is not thread safe. It must be used from the task thread only.
 *
 * @param <K> the key type
 * @param <W> the watermark type
 */
public final class RefreshScheduler<K, W> {

    private final int capacity;
    private final Consumer<K> startRefresh;
    private final Consumer<W> emitWatermark;

    /** Keys with work: running (dirty or not) or waiting for a slot. */
    private final Map<K, KeyState> keys = new HashMap<>();

    /** Keys waiting for a slot, in the order they started to wait. */
    private final LinkedHashSet<K> waiting = new LinkedHashSet<>();

    /** Watermarks that wait for refreshes, oldest first. */
    private final Deque<HeldWatermark<K, W>> heldWatermarks = new ArrayDeque<>();

    private int running;
    private long coalesced;

    /** While closed, running refreshes end but no refresh starts. */
    private BooleanSupplier gate = () -> true;

    public RefreshScheduler(int capacity, Consumer<K> startRefresh, Consumer<W> emitWatermark) {
        Preconditions.checkArgument(capacity > 0, "capacity must be positive");
        this.capacity = capacity;
        this.startRefresh = startRefresh;
        this.emitWatermark = emitWatermark;
    }

    /** A change for the given key arrived. */
    public void note(K key) {
        KeyState state = keys.get(key);
        if (state == null) {
            state = new KeyState();
            keys.put(key, state);
            if (running < capacity && gate.getAsBoolean()) {
                start(key, state);
            } else {
                waiting.add(key);
            }
        } else {
            coalesced++;
            if (state.running) {
                state.dirty = true;
            }
            // a waiting key already starts after this change
        }
    }

    /** The refresh of the given key ended: its result was emitted. */
    public void finished(K key) {
        KeyState state = keys.get(key);
        Preconditions.checkState(
                state != null && state.running, "No refresh of key %s is running.", key);
        state.running = false;
        state.finishedGeneration = state.startedGeneration;
        running--;
        releaseWatermarks(key, state);
        if (state.dirty) {
            state.dirty = false;
            // behind the keys that already wait, so that a busy key cannot starve them
            waiting.add(key);
        } else {
            keys.remove(key);
        }
        startWaiting();
    }

    /** A watermark arrived. It is emitted once the refreshes it depends on ended. */
    public void watermark(W watermark) {
        Map<K, Long> required = new HashMap<>();
        for (Map.Entry<K, KeyState> entry : keys.entrySet()) {
            KeyState state = entry.getValue();
            // a clean running refresh started after every change of its key; otherwise the next
            // refresh of the key is the first one that starts after all changes before now
            boolean coveredByRunning = state.running && !state.dirty;
            required.put(
                    entry.getKey(),
                    coveredByRunning ? state.startedGeneration : state.startedGeneration + 1);
        }
        heldWatermarks.addLast(new HeldWatermark<>(watermark, required));
        emitReleasedWatermarks();
    }

    /**
     * Sets what must hold for a refresh to start. While it does not, running refreshes still end
     * and changes still mark keys, but keys only wait. Call {@link #gateOpened()} when it may hold
     * again.
     */
    public void setGate(BooleanSupplier gate) {
        this.gate = gate;
    }

    /** The gate may hold again: starts as many waiting refreshes as there are free slots. */
    public void gateOpened() {
        startWaiting();
    }

    public boolean isGateClosed() {
        return !gate.getAsBoolean();
    }

    /** Keys that have work: running, dirty or waiting. */
    public Set<K> keysWithWork() {
        return keys.keySet();
    }

    public boolean isIdle() {
        return keys.isEmpty();
    }

    public int numRunning() {
        return running;
    }

    public int numWaiting() {
        return waiting.size();
    }

    /** The changes that were merged into a refresh that was running or waiting already. */
    public long numCoalesced() {
        return coalesced;
    }

    private void start(K key, KeyState state) {
        state.running = true;
        state.startedGeneration++;
        running++;
        startRefresh.accept(key);
    }

    private void startWaiting() {
        // one at a time: a refresh that ends at once may close the gate
        Iterator<K> it = waiting.iterator();
        while (running < capacity && it.hasNext() && gate.getAsBoolean()) {
            K key = it.next();
            it.remove();
            start(key, keys.get(key));
            it = waiting.iterator();
        }
    }

    private void releaseWatermarks(K key, KeyState state) {
        for (HeldWatermark<K, W> held : heldWatermarks) {
            Long required = held.required.get(key);
            if (required != null && state.finishedGeneration >= required) {
                held.required.remove(key);
            }
        }
        emitReleasedWatermarks();
    }

    private void emitReleasedWatermarks() {
        while (!heldWatermarks.isEmpty() && heldWatermarks.peekFirst().required.isEmpty()) {
            emitWatermark.accept(heldWatermarks.pollFirst().watermark);
        }
    }

    private static final class KeyState {
        private boolean running;
        private boolean dirty;
        private long startedGeneration;
        private long finishedGeneration;
    }

    private static final class HeldWatermark<K, W> {
        private final W watermark;
        private final Map<K, Long> required;

        private HeldWatermark(W watermark, Map<K, Long> required) {
            this.watermark = watermark;
            this.required = required;
        }
    }
}
