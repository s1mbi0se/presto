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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueryRequestMetadata
{
    private final List<TableMetadata> metadata;

    @JsonProperty("metadata")
    public List<TableMetadata> getMetadata()
    {
        return this.metadata;
    }

    @JsonCreator
    public QueryRequestMetadata(@JsonProperty List<TableMetadata> metadata)
    {
        this.metadata = metadata;
    }

    public static final class Builder
    {
        private List<TableMetadata> metadata;

        public Builder()
        {
        }

        public Builder withMetadata(List<TableMetadata> metadata)
        {
            this.metadata = metadata;
            return this;
        }

        public QueryRequestMetadata build()
        {
            return new QueryRequestMetadata(metadata);
        }
    }
}
