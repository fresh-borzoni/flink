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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshSpec;

import javax.annotation.Nullable;

/**
 * Factory of {@link AnchorGroupRefreshOperator}.
 *
 * <p>It is a {@link YieldingOperatorFactory} so that the operator, which yields to the mailbox
 * while it waits for lookups, is never chained to a legacy source that runs in its own thread.
 */
public class AnchorGroupRefreshOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements OneInputStreamOperatorFactory<RowData, RowData>,
                YieldingOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private final GroupRefreshSpec spec;
    private final AnchorJoinTree tree;
    private final AnchorLookupSpec[] lookupSpecs;
    private final @Nullable GeneratedJoinCondition[] conditions;
    private final GeneratedFilterCondition[][] filters;
    private final AnchorNotes notes;

    public AnchorGroupRefreshOperatorFactory(
            GroupRefreshSpec spec,
            AnchorJoinTree tree,
            AnchorLookupSpec[] lookupSpecs,
            @Nullable GeneratedJoinCondition[] conditions,
            GeneratedFilterCondition[][] filters,
            AnchorNotes notes) {
        this.spec = spec;
        this.tree = tree;
        this.lookupSpecs = lookupSpecs;
        this.conditions = conditions;
        this.filters = filters;
        this.notes = notes;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        return (T)
                new AnchorGroupRefreshOperator(
                        parameters, spec, tree, lookupSpecs, conditions, filters, notes);
    }

    @Override
    public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader classLoader) {
        return AnchorGroupRefreshOperator.class;
    }
}
