package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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
