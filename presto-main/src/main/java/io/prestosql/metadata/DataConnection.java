package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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
//    private final DateTime createdAt;
//    private final String status;
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
//            @JsonProperty("created-at") DateTime createdAt,
//            @JsonProperty("status") String status,
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
//        this.createdAt = requireNonNull(createdAt, "createdAt is null");
//        this.status = requireNonNull(status, "status is null");
        this.settings = ImmutableMap.copyOf(requireNonNull(settings, "properties is null"));
    }

    public BigInteger getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
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
//    public DateTime getCreatedAt()
//    {
//        return createdAt;
//    }
//
//    public String getStatus()
//    {
//        return status;
//    }

    public Map<String, String> getSettings()
    {
        return settings;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
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
