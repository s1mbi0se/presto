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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataConnectionResponse
{
    private final Double responseTime;
    private final List<DataConnection> content;

    @JsonCreator
    public DataConnectionResponse(
            @JsonProperty("response-time") Double responseTime,
            @JsonProperty("content") List<DataConnection> content)
    {
        this.responseTime = responseTime;
        this.content = content;
    }

    public Double getResponseTime()
    {
        return responseTime;
    }

    public List<DataConnection> getContent()
    {
        return content;
    }
}
