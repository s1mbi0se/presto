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

import java.util.HashMap;
import java.util.Map;

public enum DataConnectionType
{
    SHANNONDB(0, "shannondb"),
    MYSQL(1, "mysql"),
    POSTGRESQL(2, "postgresql"),
    MONGODB(3, "mongosb"),
    ORACLE(4, "oracle"),
    SQLSERVER(5, "sqlserver"),
    REDSHIFT(6, "redshift"),
    SAP(7, "sap"),
    SNOWFLAKE(8, "snow"),
    TERADATA(9, "teradata"),
    IBM_DB2(10, "db2"),
    HIVE(11, "hive-hadoop2"),
    MARIADB(12, "mysql"),
    COCKROACHDB(13, "cockroachdb"),
    BIGQUERY(14, "bigquery"),
    ELASTICSEARCH(15, "elasticsearch"),
    SPARKSQL(16, "spark"),
    MEMSQL(17, "memsql"),
    CSV(18, "csv"),
    S3(19, "hive-hadoop2"),
    WASABI(20, "hive-hadoop2"),
    B2(21, "hive-hadoop2");

    private int value;
    private String name;
    private static Map map = new HashMap<>();

    private DataConnectionType(int value, String name)
    {
        this.value = value;
        this.name = name;
    }

    static {
        for (DataConnectionType dataConnectionType : DataConnectionType.values()) {
            map.put(dataConnectionType.value, dataConnectionType);
        }
    }

    public static DataConnectionType valueOf(int dataConnectionType)
    {
        return (DataConnectionType) map.get(dataConnectionType);
    }

    public int getValue()
    {
        return value;
    }

    public String getName()
    {
        return name;
    }
}
