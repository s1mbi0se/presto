package io.prestosql.metadata;

import java.util.List;

public interface ApiService
{
    List<DataConnection> executeApiRequest(String dataConnectionEndpoint, String queryParameters);
}

