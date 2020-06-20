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
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataConnection
{
    private final BigInteger id;
    private final String name;
    //    private final String friendlyName;
    private final Integer typeId;
    //    private final String availability;
//    private final String role;
//    private final BigInteger organizationId;
//    private final String createdByType;
//    private final BigInteger createdByUserId;
    @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss")
    private final LocalDateTime createdAt;
    @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss")
    private final LocalDateTime updatedAt;
    @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss")
    private final LocalDateTime deletedAt;
    private final String status;
    private final Map<String, String> settings;

    @JsonCreator
    public DataConnection(
            @JsonProperty("id") BigInteger id,
            @JsonProperty("name") String name,
//            @JsonProperty("friendly-name") String friendlyName,
            @JsonProperty("type-id") Integer typeId,
//            @JsonProperty("availability") String availability,
//            @JsonProperty("role") String role,
//            @JsonProperty("organization-id") BigInteger organizationId,
//            @JsonProperty("created-by-type") String createdByType,
//            @JsonProperty("created-by-user-id") BigInteger createdByUserId,
            @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss") @JsonProperty("created-at") LocalDateTime createdAt,
            @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss") @JsonProperty("updated-at") LocalDateTime updatedAt,
            @JsonFormat(pattern = "MM/dd/yyyy, HH:mm:ss") @JsonProperty("deleted-at") LocalDateTime deletedAt,
            @JsonProperty("status") String status,
            @JsonProperty("settings") Map<String, String> settings)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
//        this.friendlyName = friendlyName;
        this.typeId = requireNonNull(typeId, "typeId is null");
//        this.availability = requireNonNull(availability, "availability is null");
//        this.role = requireNonNull(role, "role is null");
//        this.organizationId = requireNonNull(organizationId, "organizationId is null");
//        this.createdByType = requireNonNull(createdByType, "createdByType is null");
//        this.createdByUserId = requireNonNull(createdByUserId, "createdByUserId is null");
        this.createdAt = requireNonNull(createdAt, "createdAt is null");
        this.updatedAt = updatedAt;
        this.deletedAt = deletedAt;
        this.status = requireNonNull(status, "status is null");
        this.settings = Optional.ofNullable(settings).orElse(ImmutableMap.of());
    }

    public BigInteger getId()
    {
        return id;
    }

    public String getName()
    {
        return name.toLowerCase(ENGLISH);
    }

//    public String getFriendlyName()
//    {
//        return friendlyName;
//    }

    public Integer getTypeId()
    {
        return typeId;
    }

    //    public String getAvailability()
//    {
//        return availability;
//    }
//
//    public String getRole()
//    {
//        return role;
//    }
//
//    public BigInteger getOrganizationId()
//    {
//        return organizationId;
//    }
//
//    public String getCreatedByType()
//    {
//        return createdByType;
//    }
//
//    public BigInteger getCreatedByUserId()
//    {
//        return createdByUserId;
//    }
//
    public LocalDateTime getCreatedAt()
    {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt()
    {
        return updatedAt;
    }

    public LocalDateTime getDeletedAt()
    {
        return deletedAt;
    }

    public String getStatus()
    {
        return status;
    }

    public Map<String, String> getSettings()
    {
        return settings;
    }

    public static DataConnection from(String json)
    {
        JsonCodec<DataConnection> jsonCodec = jsonCodec(DataConnection.class);
        return jsonCodec.fromJson(json);
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
        DataConnection that = (DataConnection) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public String toString()
    {
        return "DataConnection{" +
                "name='" + name + '\'' +
                '}';
    }
}
