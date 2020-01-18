package io.prestosql.metadata;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class DataConnectionParser
{
    public static Map<String, String> getCatalogProperties(Map<String, String> dataConnectionsProperties)
    {
        return ImmutableMap.copyOf(dataConnectionsProperties);
    }

}
