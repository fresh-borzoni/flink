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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;

/**
 * How a delta join that refreshes groups writes the rows of its output table and reads them back,
 * instead of keeping the rows it emitted in state.
 *
 * <p>The join turns its joined rows into rows of the output table with {@link #projection} and
 * {@link #condition}. The rows of one join key are the rows of the output table whose {@link
 * #groupColumns} equal that key; they are found by a lookup on the output table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeltaJoinReadBackSpec {

    public static final String FIELD_NAME_PROJECTION_INPUT = "projectionInput";
    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";
    public static final String FIELD_NAME_OUTPUT_TABLE = "outputTable";
    public static final String FIELD_NAME_GROUP_COLUMNS = "groupColumns";
    public static final String FIELD_NAME_OUTPUT_KEY = "outputKey";
    public static final String FIELD_NAME_READ_YOUR_WRITES = "readYourWrites";

    /** The joined row that the projection reads. */
    @JsonProperty(FIELD_NAME_PROJECTION_INPUT)
    private final RowType projectionInput;

    /** The columns of the output table, computed on the joined row. */
    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    /** The joined rows that are written, if not all. */
    @JsonProperty(FIELD_NAME_CONDITION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final @Nullable RexNode condition;

    /** The output table, as a table to look up. */
    @JsonProperty(FIELD_NAME_OUTPUT_TABLE)
    private final TemporalTableSourceSpec outputTable;

    /** The columns of the output table that hold the join keys, in the order of the join keys. */
    @JsonProperty(FIELD_NAME_GROUP_COLUMNS)
    private final int[] groupColumns;

    /** The primary key of the output table. */
    @JsonProperty(FIELD_NAME_OUTPUT_KEY)
    private final int[] outputKey;

    /**
     * Whether a lookup of the output table sees every row the join emitted before it; otherwise
     * only the rows emitted before a completed checkpoint.
     */
    @JsonProperty(FIELD_NAME_READ_YOUR_WRITES)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean readYourWrites;

    @JsonCreator
    public DeltaJoinReadBackSpec(
            @JsonProperty(FIELD_NAME_PROJECTION_INPUT) RowType projectionInput,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
            @JsonProperty(FIELD_NAME_OUTPUT_TABLE) TemporalTableSourceSpec outputTable,
            @JsonProperty(FIELD_NAME_GROUP_COLUMNS) int[] groupColumns,
            @JsonProperty(FIELD_NAME_OUTPUT_KEY) int[] outputKey,
            @JsonProperty(FIELD_NAME_READ_YOUR_WRITES) boolean readYourWrites) {
        this.projectionInput = projectionInput;
        this.projection = projection;
        this.condition = condition;
        this.outputTable = outputTable;
        this.groupColumns = groupColumns;
        this.outputKey = outputKey;
        this.readYourWrites = readYourWrites;
    }

    @JsonIgnore
    public RowType getProjectionInput() {
        return projectionInput;
    }

    @JsonIgnore
    public List<RexNode> getProjection() {
        return projection;
    }

    @JsonIgnore
    public @Nullable RexNode getCondition() {
        return condition;
    }

    @JsonIgnore
    public TemporalTableSourceSpec getOutputTable() {
        return outputTable;
    }

    @JsonIgnore
    public int[] getGroupColumns() {
        return groupColumns;
    }

    @JsonIgnore
    public int[] getOutputKey() {
        return outputKey;
    }

    @JsonIgnore
    public boolean isReadYourWrites() {
        return readYourWrites;
    }
}
