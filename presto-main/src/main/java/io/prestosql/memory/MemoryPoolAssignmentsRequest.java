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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MemoryPoolAssignmentsRequest
{
    private final String coordinatorId;
    private final long version;
    private final List<MemoryPoolAssignment> assignments;

    @JsonCreator
    public MemoryPoolAssignmentsRequest(@JsonProperty("coordinatorId") String coordinatorId, @JsonProperty("version") long version, @JsonProperty("assignments") List<MemoryPoolAssignment> assignments)
    {
        this.coordinatorId = requireNonNull(coordinatorId, "coordinatorId is null");
        this.version = version;
        this.assignments = ImmutableList.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    /**
     * Gets the coordinator identifier.
     * <p>
     * Used to discover which machine (node) is the
     * coordinator inside the cluster.
     *
     * @return the coordinator identifier
     */
    @JsonProperty
    public String getCoordinatorId()
    {
        return coordinatorId;
    }

    /**
     * Gets the version of the request.
     * <p>
     * It is used to ensure the consistency if requests
     * come out of order.
     *
     * @return the version of the request.
     */
    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public List<MemoryPoolAssignment> getAssignments()
    {
        return assignments;
    }

    /**
     * Overrides the {@link Object#toString()} method.
     * <p>
     * The string representation contains the assignment's version and
     * the list of memory pool assignments.
     *
     * @return the object's string representation
     */
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", version)
                .add("assignments", assignments)
                .toString();
    }
}
