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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupRefreshDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.utils.GroupRefreshDeltaJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Converts a tree of joins into a {@link StreamPhysicalGroupRefreshDeltaJoin} when the incremental
 * delta join cannot handle it (outer joins, inputs with deletes) and every input supports the
 * lookups the group refresh delta join needs.
 *
 * <p>A tree of three or more tables is grouped by an anchor, a table whose row is in every output
 * row. A join of two tables is grouped as {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING} says. By default it is
 * grouped by an anchor if it has one, so that a change of a row re-reads only the output rows of
 * its anchor rows, and the join grouped by the join key is kept as an alternative: {@link
 * DeltaJoinReadBackRule} switches to it if the output table can be read back by the join key but
 * not by the anchor.
 *
 * <p>The rule runs top-down before changelog-mode inference, so that it converts the largest
 * possible tree and the inference can request old images from the inputs that need them.
 */
@Value.Enclosing
public class GroupRefreshDeltaJoinRule extends RelRule<GroupRefreshDeltaJoinRule.Config> {

    public static final GroupRefreshDeltaJoinRule INSTANCE =
            GroupRefreshDeltaJoinRule.Config.DEFAULT.toRule();

    private GroupRefreshDeltaJoinRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        StreamPhysicalJoin join = call.rel(0);
        // the job option enables it for every join, the hint for the joins of one query block
        return (tableConfig.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_ENABLED)
                        || FlinkHints.hasGroupRefreshHint(join.getHints()))
                && tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY)
                        != OptimizerConfigOptions.DeltaJoinStrategy.NONE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalJoin join = call.rel(0);
        OptimizerConfigOptions.DeltaJoinGrouping grouping =
                unwrapTableConfig(call)
                        .get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_GROUPING);
        // the analysis groups a tree of three or more tables by an anchor in any case
        Optional<GroupRefreshDeltaJoinUtil.JoinTree> byJoinKey =
                grouping == OptimizerConfigOptions.DeltaJoinGrouping.ANCHOR
                        ? Optional.empty()
                        : GroupRefreshDeltaJoinUtil.analyze(join, true);
        if (byJoinKey.isPresent()
                && byJoinKey.get().grouping == GroupRefreshDeltaJoinSpec.Grouping.ANCHOR) {
            call.transformTo(node(join, byJoinKey.get()));
            return;
        }
        // a FULL join has no anchor
        Optional<GroupRefreshDeltaJoinUtil.JoinTree> anchored =
                grouping == OptimizerConfigOptions.DeltaJoinGrouping.JOIN_KEY
                                || join.getJoinType() == JoinRelType.FULL
                        ? Optional.empty()
                        : GroupRefreshDeltaJoinUtil.analyze(join, false);
        if (anchored.isPresent()) {
            StreamPhysicalGroupRefreshDeltaJoin node = node(join, anchored.get());
            call.transformTo(
                    byJoinKey.isPresent()
                            ? node.withAlternative(node(join, byJoinKey.get()))
                            : node);
        } else {
            byJoinKey.ifPresent(tree -> call.transformTo(node(join, tree)));
        }
    }

    private static StreamPhysicalGroupRefreshDeltaJoin node(
            StreamPhysicalJoin join, GroupRefreshDeltaJoinUtil.JoinTree tree) {
        if (tree.grouping == GroupRefreshDeltaJoinSpec.Grouping.JOIN_KEY) {
            Set<ImmutableBitSet> keys = GroupRefreshDeltaJoinUtil.joinKeyGroupOutputKeys(tree);
            return new StreamPhysicalGroupRefreshDeltaJoin(
                    join.getCluster(),
                    join.getTraitSet(),
                    tree.leafInputs,
                    join.getRowType(),
                    tree,
                    keys,
                    keys);
        }

        // All changes of an output row whose key contains the anchor key come from recomputes of
        // that anchor key, in order. With one row per group the operator may upsert by the anchor
        // key without retracting the previous row, so only the anchor key itself is an upsert key
        // then: a longer key, e.g. (anchor, dimension), would keep the old row next to the new one.
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(join.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> joinKeys = mq.getUniqueKeys(join);
        Set<ImmutableBitSet> upsertKeys = Collections.emptySet();
        Set<ImmutableBitSet> uniqueKeys = Collections.emptySet();
        if (tree.outputAnchorKeyPositions != null) {
            ImmutableBitSet anchorKey = ImmutableBitSet.of(tree.outputAnchorKeyPositions);
            if (joinKeys != null) {
                uniqueKeys =
                        joinKeys.stream()
                                .filter(key -> key.contains(anchorKey))
                                .collect(Collectors.toSet());
            }
            upsertKeys = tree.singleRowGroups ? Collections.singleton(anchorKey) : uniqueKeys;
        }
        return new StreamPhysicalGroupRefreshDeltaJoin(
                join.getCluster(),
                join.getTraitSet(),
                tree.leafInputs,
                join.getRowType(),
                tree,
                upsertKeys,
                uniqueKeys);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        GroupRefreshDeltaJoinRule.Config DEFAULT =
                ImmutableGroupRefreshDeltaJoinRule.Config.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalJoin.class).anyInputs())
                        .withDescription("GroupRefreshDeltaJoinRule");

        @Override
        default GroupRefreshDeltaJoinRule toRule() {
            return new GroupRefreshDeltaJoinRule(this);
        }
    }
}
