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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsEpochs;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

/**
 * A {@link SourceAbilitySpec} that cuts the changes of a scan into the epochs of a domain, see
 * {@link SupportsEpochs}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("Epochs")
public final class EpochsSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_DOMAIN = "domain";

    @JsonProperty(FIELD_NAME_DOMAIN)
    private final String domain;

    @JsonCreator
    public EpochsSpec(@JsonProperty(FIELD_NAME_DOMAIN) String domain) {
        this.domain = domain;
    }

    public String getDomain() {
        return domain;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (!(tableSource instanceof SupportsEpochs)
                || !((SupportsEpochs) tableSource).applyEpochs(domain)) {
            throw new TableException(
                    String.format(
                            "%s cannot cut its changes into epochs.",
                            tableSource.getClass().getName()));
        }
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return false;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        return "epochs=[" + domain + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return domain.equals(((EpochsSpec) o).domain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), domain);
    }
}
