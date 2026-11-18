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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.table.data.RowData;

/**
 * Creates a {@link JoinKeyGroupRefreshOperator}.
 *
 * <p>It is a {@link YieldingOperatorFactory} so that the operator, which yields to the mailbox
 * while it waits for lookups, is never chained to a legacy source that runs in its own thread.
 */
public class JoinKeyGroupRefreshOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements TwoInputStreamOperatorFactory<RowData, RowData, RowData>,
                YieldingOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private final GroupRefreshSpec spec;
    private final JoinKeyGroupSpec groups;

    public JoinKeyGroupRefreshOperatorFactory(GroupRefreshSpec spec, JoinKeyGroupSpec groups) {
        this.spec = spec;
        this.groups = groups;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        return (T) new JoinKeyGroupRefreshOperator(parameters, spec, groups);
    }

    @Override
    public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader classLoader) {
        return JoinKeyGroupRefreshOperator.class;
    }
}
