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
import java.util.Map;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableMetadata
{
    private final String name;
    private final Optional<String> type;
    private final Optional<StorageMetadata> storage;
    private final Optional<List<ColumnMetadata>> partitions;
    private final List<ColumnMetadata> dataColumns;
    private final Optional<String> comment;
    private final Optional<Map<String, Object>> additionalProperties;

    @JsonCreator
    public TableMetadata(
            @JsonProperty String name,
            @JsonProperty Optional<String> type,
            @JsonProperty Optional<StorageMetadata> storage,
            @JsonProperty Optional<List<ColumnMetadata>> partitions,
            @JsonProperty("data_columns") List<ColumnMetadata> dataColumns,
            @JsonProperty Optional<String> comment,
            @JsonProperty("additional_properties") Optional<Map<String, Object>> additionalProperties)
    {
        this.name = name;
        this.type = type;
        this.storage = storage;
        this.partitions = partitions;
        this.dataColumns = dataColumns;
        this.comment = comment;
        this.additionalProperties = additionalProperties;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<StorageMetadata> getStorage()
    {
        return storage;
    }

    @JsonProperty
    public Optional<List<ColumnMetadata>> getPartitions()
    {
        return partitions;
    }

    @JsonProperty("data_columns")
    public List<ColumnMetadata> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty("additional_properties")
    public Optional<Map<String, Object>> getAdditionalProperties()
    {
        return additionalProperties;
    }

    public static final class Builder
    {
        private String name;
        private Optional<String> type;
        private Optional<StorageMetadata> storage;
        private Optional<List<ColumnMetadata>> partitions;
        private List<ColumnMetadata> dataColumns;
        private Optional<String> comment;
        private Optional<Map<String, Object>> additionalProperties;

        public Builder()
        {
        }

        public Builder withName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder withType(Optional<String> type)
        {
            this.type = type;
            return this;
        }

        public Builder withStorage(Optional<StorageMetadata> storage)
        {
            this.storage = storage;
            return this;
        }

        public Builder withPartitions(Optional<List<ColumnMetadata>> partitions)
        {
            this.partitions = partitions;
            return this;
        }

        public Builder withDataColumns(List<ColumnMetadata> dataColumns)
        {
            this.dataColumns = dataColumns;
            return this;
        }

        public Builder withComment(Optional<String> comment)
        {
            this.comment = comment;
            return this;
        }

        public Builder withAdditionalProperties(Optional<Map<String, Object>> additionalProperties)
        {
            this.additionalProperties = additionalProperties;
            return this;
        }

        public TableMetadata build()
        {
            return new TableMetadata(name, type, storage, partitions, dataColumns, comment, additionalProperties);
        }
    }
}
