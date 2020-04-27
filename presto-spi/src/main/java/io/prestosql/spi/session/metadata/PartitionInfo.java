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
public class PartitionInfo
{
    private final String value;
    private final StorageMetadata storage;

    @JsonCreator
    public PartitionInfo(
            @JsonProperty String value,
            @JsonProperty StorageMetadata storage)
    {
        this.value = value;
        this.storage = storage;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @JsonProperty
    public StorageMetadata getStorage()
    {
        return storage;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PartitionInfo that = (PartitionInfo) o;
        return storage.equals(that.storage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(storage);
    }
}
