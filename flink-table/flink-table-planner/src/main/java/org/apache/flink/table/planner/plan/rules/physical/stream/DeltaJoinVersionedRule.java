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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinVersioned;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsEpochs;
import org.apache.flink.table.planner.plan.abilities.source.EpochsSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.GroupRefreshDeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupRefreshDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Lets a group refresh delta join grouped by an anchor read its tables as of epochs of its sources,
 * see {@link
 * org.apache.flink.table.runtime.operators.join.deltajoin.refresh.GroupRefreshOutputMode#VERSIONED}:
 * the scans of its inputs cut their changes into epochs, and its lookups read as of them.
 *
 * <p>It applies if every table supports epochs ({@link SupportsEpochs}) and, with {@link
 * DeltaJoinVersioned#AUTO}, if the join does not read its output back, which {@link
 * DeltaJoinReadBackRule} decides first.
 */
@Value.Enclosing
public class DeltaJoinVersionedRule extends RelRule<DeltaJoinVersionedRule.Config> {

    public static final DeltaJoinVersionedRule INSTANCE =
            DeltaJoinVersionedRule.Config.DEFAULT.toRule();

    private DeltaJoinVersionedRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamPhysicalGroupRefreshDeltaJoin join = call.rel(0);
        return versioned(join) != DeltaJoinVersioned.NONE
                && join.getVersionedDomain() == null
                && join.getReadBack() == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalGroupRefreshDeltaJoin join = call.rel(0);
        String domain = domainOf(join, versioned(join) == DeltaJoinVersioned.FORCE);
        if (domain != null) {
            call.transformTo(join.withVersioned(domain, inputsWithEpochs(join, domain), null));
        }
    }

    /**
     * The epoch domain of the join if it can read its tables as of epochs: it is grouped by an
     * anchor and every table supports epochs. Otherwise null, or a failure if {@code force}.
     */
    static @Nullable String domainOf(StreamPhysicalGroupRefreshDeltaJoin join, boolean force) {
        if (join.getTree().grouping != GroupRefreshDeltaJoinSpec.Grouping.ANCHOR) {
            if (force) {
                throw new ValidationException(
                        "Only a group refresh delta join grouped by an anchor can read its tables "
                                + "as of epochs.");
            }
            return null;
        }
        List<String> tables = new ArrayList<>();
        for (StreamPhysicalTableSourceScan scan : join.getTree().leafScans) {
            TableSourceTable table = scan.tableSourceTable();
            String name = String.join(".", table.getQualifiedName());
            DynamicTableSource source = table.tableSource().copy();
            if (!(source instanceof SupportsEpochs)
                    || !((SupportsEpochs) source).applyEpochs(name)) {
                if (force) {
                    throw new ValidationException(
                            String.format(
                                    "The delta join cannot read %s as of epochs: its source does "
                                            + "not support them. Set '%s' to AUTO or NONE.",
                                    name,
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_VERSIONED
                                            .key()));
                }
                return null;
            }
            tables.add(name);
        }
        // one domain per join: its scans and lookups count the same epochs
        return String.join(",", tables);
    }

    /** The inputs of the join with their scans cutting their changes into epochs. */
    static List<RelNode> inputsWithEpochs(StreamPhysicalGroupRefreshDeltaJoin join, String domain) {
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            inputs.add(withEpochs(input, domain));
        }
        return inputs;
    }

    static DeltaJoinVersioned versionedOption(RelNode node) {
        return versioned(node);
    }

    private static DeltaJoinVersioned versioned(RelNode node) {
        return unwrapTableConfig(node)
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_GROUP_REFRESH_VERSIONED);
    }

    /** The input with its scan cutting its changes into the epochs of the domain. */
    private static RelNode withEpochs(RelNode input, String domain) {
        RelNode rel =
                input instanceof HepRelVertex ? ((HepRelVertex) input).getCurrentRel() : input;
        if (rel instanceof StreamPhysicalTableSourceScan) {
            StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) rel;
            TableSourceTable table = scan.tableSourceTable();
            DynamicTableSource source = table.tableSource().copy();
            EpochsSpec epochs = new EpochsSpec(domain);
            epochs.apply(source, null);
            return scan.copy(
                    table.copy(source, table.getStatistic(), new SourceAbilitySpec[] {epochs}));
        }
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode child : rel.getInputs()) {
            inputs.add(withEpochs(child, domain));
        }
        return rel.copy(rel.getTraitSet(), inputs);
    }

    /** Configuration for {@link DeltaJoinVersionedRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        DeltaJoinVersionedRule.Config DEFAULT =
                ImmutableDeltaJoinVersionedRule.Config.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(StreamPhysicalGroupRefreshDeltaJoin.class)
                                                .anyInputs())
                        .withDescription("DeltaJoinVersionedRule");

        @Override
        default DeltaJoinVersionedRule toRule() {
            return new DeltaJoinVersionedRule(this);
        }
    }
}
