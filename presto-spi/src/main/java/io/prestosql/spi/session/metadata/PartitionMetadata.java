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

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PartitionMetadata
{
    private final ColumnMetadata column;
    private final PartitionInfo info;

    @JsonCreator
    public PartitionMetadata(
            @JsonProperty ColumnMetadata column,
            @JsonProperty PartitionInfo info)
    {
        this.column = column;
        this.info = info;
    }

    @JsonProperty
    public ColumnMetadata getColumn()
    {
        return column;
    }

    @JsonProperty
    public PartitionInfo getInfo()
    {
        return info;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PartitionMetadata that = (PartitionMetadata) o;
        return Objects.equals(column, that.column) &&
                Objects.equals(info, that.info);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, info);
    }
}
