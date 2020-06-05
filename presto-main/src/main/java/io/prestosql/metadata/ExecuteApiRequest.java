package io.prestosql.metadata;

import java.util.List;

public interface ExecuteApiRequest
{
    public List<DataConnection> executeApiRequest(String dataConnectionEndpoint, String queryParameters);
}

