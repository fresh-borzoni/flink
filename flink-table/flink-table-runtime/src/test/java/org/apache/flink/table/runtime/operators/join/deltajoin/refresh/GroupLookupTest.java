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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the timeout of a {@link GroupLookup}. */
class GroupLookupTest {

    private static final RowType ROW_TYPE = RowType.of(new IntType(), new IntType());

    private final List<ResultFuture<Object>> pending = new ArrayList<>();
    private final List<List<RowData>> results = new ArrayList<>();
    private final List<Throwable> errors = new ArrayList<>();

    private TaskMailbox mailbox;
    private TestProcessingTimeService timers;
    private GroupLookup lookup;

    @BeforeEach
    void setup() throws Exception {
        mailbox = new TaskMailboxImpl();
        MailboxProcessor processor =
                new MailboxProcessor(c -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);
        MailboxExecutor executor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, processor);
        timers = new TestProcessingTimeService();
        timers.setCurrentTime(0);
        lookup =
                new GroupLookup(
                        fetcher(),
                        DataTypes.ROW(DataTypes.INT(), DataTypes.INT()).bridgedTo(RowData.class),
                        null,
                        new RowDataSerializer(ROW_TYPE),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {0}, ROW_TYPE.getChildren().toArray(new LogicalType[0])));
        lookup.open(new MockStreamingRuntimeContext(1, 0), executor, timers, 1000);
    }

    @Test
    void testLookupThatDoesNotCompleteFails() throws Exception {
        lookup.read(key(1), results::add, errors::add);
        timers.setCurrentTime(999);
        assertThat(errors).isEmpty();

        timers.setCurrentTime(1000);
        assertThat(errors).singleElement().isInstanceOf(TimeoutException.class);

        // a result that arrives late is dropped
        pending.get(0).complete(Collections.singletonList(GenericRowData.of(1, 1)));
        runMails();
        assertThat(results).isEmpty();
    }

    @Test
    void testCompletedLookupDoesNotTimeOut() throws Exception {
        lookup.read(key(1), results::add, errors::add);
        pending.get(0).complete(Collections.singletonList(GenericRowData.of(1, 1)));
        runMails();
        assertThat(results).singleElement().asList().hasSize(1);

        timers.setCurrentTime(5000);
        assertThat(errors).isEmpty();
    }

    /** A key in binary form, as the operator's key selectors produce it. */
    private static RowData key(int k) {
        return new RowDataSerializer(RowType.of(new IntType())).toBinaryRow(GenericRowData.of(k));
    }

    private void runMails() throws Exception {
        Optional<Mail> mail;
        while ((mail = mailbox.tryTake(TaskMailbox.MIN_PRIORITY)).isPresent()) {
            mail.get().run();
        }
    }

    private GeneratedFunction<AsyncFunction<RowData, Object>> fetcher() {
        return new GeneratedFunction<AsyncFunction<RowData, Object>>("", "", new Object[0]) {
            @Override
            public AsyncFunction<RowData, Object> newInstance(ClassLoader classLoader) {
                return new RichAsyncFunction<RowData, Object>() {
                    @Override
                    public void open(OpenContext openContext) {}

                    @Override
                    public void asyncInvoke(RowData key, ResultFuture<Object> future) {
                        pending.add(future);
                    }
                };
            }
        };
    }
}
