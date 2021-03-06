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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnMetadata
{
    private final String name;
    private final String dataType;
    private final Optional<String> columnType;
    private final Optional<String> comment;
    private final Optional<String> extraInfo;
    private final Optional<Boolean> hidden;
    private final Optional<Map<String, String>> properties;
    private final Optional<StatisticsMetadata> statistics;

    @JsonCreator
    public ColumnMetadata(
            @JsonProperty String name,
            @JsonProperty("data_type") String dataType,
            @JsonProperty("column_type") Optional<String> columnType,
            @JsonProperty Optional<String> comment,
            @JsonProperty("extra_info") Optional<String> extraInfo,
            @JsonProperty Optional<Boolean> hidden,
            @JsonProperty Optional<Map<String, String>> properties,
            @JsonProperty Optional<StatisticsMetadata> statistics)
    {
        this.name = name;
        this.dataType = dataType;
        this.columnType = columnType;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
        this.properties = properties;
        this.statistics = statistics;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("data_type")
    public String getDataType()
    {
        return dataType;
    }

    @JsonProperty("column_type")
    public Optional<String> getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty("extra_info")
    public Optional<String> getExtraInfo()
    {
        return extraInfo;
    }

    @JsonProperty
    public Optional<Boolean> isHidden()
    {
        return hidden;
    }

    @JsonProperty
    public Optional<Map<String, String>> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public Optional<StatisticsMetadata> getStatistics()
    {
        return statistics;
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
        ColumnMetadata that = (ColumnMetadata) o;
        return name.equals(that.name) &&
                dataType.equals(that.dataType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, dataType);
    }
}
