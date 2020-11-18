/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.memory.MemoryPoolInfo;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MemoryInfo
{
    private final int availableProcessors;
    private final DataSize totalNodeMemory;
    private final Map<MemoryPoolId, MemoryPoolInfo> pools;

    @JsonCreator
    public MemoryInfo(
            @JsonProperty("availableProcessors") int availableProcessors,
            @JsonProperty("totalNodeMemory") DataSize totalNodeMemory,
            @JsonProperty("pools") Map<MemoryPoolId, MemoryPoolInfo> pools)
    {
        this.totalNodeMemory = requireNonNull(totalNodeMemory, "totalNodeMemory is null");
        this.pools = ImmutableMap.copyOf(requireNonNull(pools, "pools is null"));
        this.availableProcessors = availableProcessors;
    }

    /**
     * Gets the number of available processors for the node.
     *
     * @return the number of available processors for the node
     */
    @JsonProperty
    public int getAvailableProcessors()
    {
        return availableProcessors;
    }

    /**
     * Gets the total node's available memory in bytes.
     *
     * @return the total node's available memory in bytes
     */
    @JsonProperty
    public DataSize getTotalNodeMemory()
    {
        return totalNodeMemory;
    }

    /**
     * Gets a map of memory pool identifier and its metadata.
     *
     * @return a map of memory pool identifier and respective metadata
     */
    @JsonProperty
    public Map<MemoryPoolId, MemoryPoolInfo> getPools()
    {
        return pools;
    }

    /**
     * Overrides the {@link Object#toString()} method.
     *
     * @return the object's string representation
     */
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("availableProcessors", availableProcessors)
                .add("totalNodeMemory", totalNodeMemory)
                .add("pools", pools)
                .toString();
    }
}
