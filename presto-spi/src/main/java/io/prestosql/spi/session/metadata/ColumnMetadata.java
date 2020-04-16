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

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnMetadata
{
    private final String name;
    private final String dataType;
    private final String columnType;
    private final String comment;
    private final String extraInfo;
    private final boolean hidden;
    private final Map<String, Object> properties;

    @JsonCreator
    public ColumnMetadata(
            @JsonProperty String name,
            @JsonProperty("data_type") String dataType,
            @JsonProperty("column_type") String columnType,
            @JsonProperty String comment,
            @JsonProperty("extra_info") String extraInfo,
            @JsonProperty boolean hidden,
            @JsonProperty("properties") Map<String, Object> properties)
    {
        this.name = name;
        this.dataType = dataType;
        this.columnType = columnType;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
        this.properties = properties;
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
    public String getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty("extra_info")
    public String getExtraInfo()
    {
        return extraInfo;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @JsonProperty
    public Map<String, Object> getProperties()
    {
        return properties;
    }
}
