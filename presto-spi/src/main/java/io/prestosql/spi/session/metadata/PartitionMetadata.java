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
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PartitionMetadata
{
    private final String name;
    private final List<ColumnMetadata> columns;
    private final Optional<StorageMetadata> storage;
    private final List<String> values;

    @JsonCreator
    public PartitionMetadata(
            @JsonProperty String name,
            @JsonProperty List<ColumnMetadata> columns,
            @JsonProperty Optional<StorageMetadata> storage,
            @JsonProperty List<String> values)
    {
        this.name = name;
        this.columns = columns;
        this.storage = storage;
        this.values = values;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<StorageMetadata> getStorage()
    {
        return storage;
    }

    @JsonProperty
    public List<String> getValues()
    {
        return values;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionMetadata that = (PartitionMetadata) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
