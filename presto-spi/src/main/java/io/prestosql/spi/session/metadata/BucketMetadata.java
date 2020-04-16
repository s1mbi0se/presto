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
package io.prestosql.spi.session.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BucketMetadata
{
    private final List<String> by;
    private final Integer version;
    private final Integer count;
    private final Optional<List<SortMetadata>> sortedBy;

    @JsonCreator
    public BucketMetadata(
            @JsonProperty List<String> by,
            @JsonProperty Integer version,
            @JsonProperty Integer count,
            @JsonProperty("sorted_by") Optional<List<SortMetadata>> sortedBy)
    {
        this.by = by;
        this.version = version;
        this.count = count;
        this.sortedBy = sortedBy;
    }

    @JsonProperty
    public List<String> getBy()
    {
        return by;
    }

    @JsonProperty
    public Integer getVersion()
    {
        return version;
    }

    @JsonProperty
    public Integer getCount()
    {
        return count;
    }

    @JsonProperty("sorted_by")
    public Optional<List<SortMetadata>> getSortedBy()
    {
        return sortedBy;
    }
}
