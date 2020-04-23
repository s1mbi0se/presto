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
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StorageMetadata
{
    private final String format;
    private final String location;
    private final boolean skewed;
    private final Optional<BucketMetadata> bucket;
    private final Optional<Map<String, String>> serdeProperties;

    @JsonCreator
    public StorageMetadata(
            @JsonProperty String format,
            @JsonProperty String location,
            @JsonProperty boolean skewed,
            @JsonProperty Optional<BucketMetadata> bucket,
            @JsonProperty("serde_properties") Optional<Map<String, String>> serdeProperties)
    {
        this.format = format;
        this.location = location;
        this.skewed = skewed;
        this.bucket = bucket;
        this.serdeProperties = serdeProperties;
    }

    @JsonProperty
    public String getFormat()
    {
        return format;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public boolean isSkewed()
    {
        return skewed;
    }

    @JsonProperty
    public Optional<BucketMetadata> getBucket()
    {
        return bucket;
    }

    @JsonProperty("serde_properties")
    public Optional<Map<String, String>> getSerdeProperties()
    {
        return serdeProperties;
    }
}
