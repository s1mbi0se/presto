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
package io.prestosql.spi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryRequestTableMetadata
{
    private String tableName;
    private List<Map<String, String>> columnsMetadata;

    @JsonProperty("table_name")
    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    @JsonProperty("columns_metadata")
    public List<Map<String, String>> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public void setColumnsMetadata(List<Map<String, String>> columnsMetadata)
    {
        this.columnsMetadata = columnsMetadata;
    }

    public QueryRequestTableMetadata()
    {
    }

    public QueryRequestTableMetadata(String tableName, List<Map<String, String>> columnsMetadata)
    {
        this.tableName = tableName;
        this.columnsMetadata = columnsMetadata;
    }

    public static final class Builder
    {
        private String tableName;
        private List<Map<String, String>> columnsMetadata;

        public Builder()
        {
        }

        public Builder withTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder withColumnsMetadata(List<Map<String, String>> columnsMetadata)
        {
            this.columnsMetadata = columnsMetadata;
            return this;
        }

        public QueryRequestTableMetadata build()
        {
            return new QueryRequestTableMetadata(tableName, columnsMetadata);
        }
    }
}