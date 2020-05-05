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

import com.google.common.collect.ImmutableMap;
import com.macasaet.fernet.Key;
import com.macasaet.fernet.StringValidator;
import com.macasaet.fernet.Token;
import com.macasaet.fernet.Validator;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class DataConnectionParser
{
    private static final Logger log = Logger.get(DataConnectionParser.class);

    public static Map<String, String> getCatalogProperties(String connectorName, Map<String, String> dataConnectionsProperties, LocalDateTime createdAt, Integer typeId,
            String dataConnectionCryptoKey)
    {
        ImmutableMap.Builder<String, String> catalog = ImmutableMap.builder();

        switch (DataConnectionType.valueOf(typeId)) {
            case SHANNONDB:
                catalog.put(DynamicCatalogStoreConfig.ShannonDbConfigProperties.HOST.getConfigName(),
                        dataConnectionsProperties.get(DynamicCatalogStoreConfig.ShannonDbConfigProperties.HOST.getConfigName()));
                catalog.put(DynamicCatalogStoreConfig.ShannonDbConfigProperties.PORT.getConfigName(),
                        dataConnectionsProperties.get(DynamicCatalogStoreConfig.ShannonDbConfigProperties.PORT.getConfigName()));
                break;
            case HIVE:
                catalog.put("hive.metastore.uri", getMetastoreUri(connectorName, dataConnectionsProperties));
                break;
            case S3:
            case WASABI:
            case B2:
                catalog.put("hive.metastore", "provided");
                break;
            case MYSQL:
            case MARIADB:
            case SQLSERVER:
            case ORACLE:
            case REDSHIFT:
            case IBM_DB2:
            case POSTGRESQL:
                catalog.put("connection-url", getJdbcConnectionString(connectorName, dataConnectionsProperties));
                catalog.put("connection-user", dataConnectionsProperties.get("username"));
                catalog.put("connection-password", dataConnectionCryptoKey == null ?
                        dataConnectionsProperties.get("password") : decrypt(dataConnectionsProperties.get("password"), createdAt, dataConnectionCryptoKey));
                break;
            default:
                throw new PrestoException(null, "Data source not supported.");
        }

        return catalog.build();
    }

    private static String decrypt(String password, LocalDateTime createdAt, String dataConnectionCryptoKey)
    {
        try {
            final Key key = new Key(dataConnectionCryptoKey);

            final Token token = Token.fromString(password);

            final Validator<String> validator = new StringValidator()
            {
                public TemporalAmount getTimeToLive()
                {
                    return Duration.between(createdAt,
                            LocalDateTime.now(ZoneOffset.UTC).plus(Duration.ofSeconds(60L)));
                }
            };

            final String payload = token.validateAndDecrypt(key, validator);

            return payload;
        }
        catch (IllegalArgumentException e) {
            log.error(e.getMessage());
            return password;
        }
    }

    private static String getMetastoreUri(String connectorName, Map<String, String> dataConnectionsProperties)
    {
        return String.format("thrift://%s:%s", dataConnectionsProperties.get("host"), dataConnectionsProperties.get("host-port"));
    }

    private DataConnectionParser() {}

    private static String getJdbcConnectionString(String connectorName, Map<String, String> dataConnectionsProperties)
    {
        String jdbc = null;
        switch (connectorName) {
            case "postgresql":
                jdbc = String.format("jdbc:%s://%s:%s/%s", connectorName, dataConnectionsProperties.get("host"), dataConnectionsProperties.get("host-port"),
                        dataConnectionsProperties.get("database-name"));
                break;
            case "mysql":
            default:
                jdbc = String.format("jdbc:%s://%s:%s", connectorName, dataConnectionsProperties.get("host"), dataConnectionsProperties.get("host-port"));
        }

        checkState(jdbc != null, "Catalog configuration %s cannot infer connector-url", connectorName);

        return jdbc;
    }
}
