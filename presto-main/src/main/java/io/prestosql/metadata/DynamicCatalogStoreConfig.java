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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Maps.fromProperties;

public class DynamicCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final String API_CONFIG_FILE = "api-config.properties";
    private static final String SHANNONDB_CONFIG_FILE = "shannondb.properties";
    protected static final String SHANNONDB_CONNECTOR_NAME = "shannondb";
    protected static DataConnection shannondbDataConnection;
    private File catalogConfigurationDir = new File("etc/catalog/");
    private String dataConnectionsEndpoint = "/v1/data_connections/all";
    private String dataConnectionsUrl = "localhost";
    private String dataConnectionsApiKey = "apikey";
    private List<String> disabledCatalogs;
    private String cryptoKey = "";

    public String getDataConnectionsEndpoint()
    {
        return dataConnectionsEndpoint;
    }

    public String getDataConnectionsUrl()
    {
        return dataConnectionsUrl;
    }

    public String getDataConnectionsApiKey()
    {
        return dataConnectionsApiKey;
    }

    public DynamicCatalogStoreConfig()
    {
        Map<String, String> properties = null;
        try {
            properties = this.readConfigFile(API_CONFIG_FILE);

            if (properties.size() > 0) {
                Map<String, String> env = System.getenv();

                if (env.containsKey("API_DATA_CONNECTIONS_ENDPOINT")) {
                    this.dataConnectionsEndpoint = env.get("API_DATA_CONNECTIONS_ENDPOINT");
                }
                else {
                    this.dataConnectionsEndpoint = properties.get("data-connections-endpoint");
                }

                if (env.containsKey("API_DATA_CONNECTIONS_URL")) {
                    this.dataConnectionsUrl = env.get("API_DATA_CONNECTIONS_URL");
                }
                else {
                    this.dataConnectionsUrl = properties.get("data-connections-url");
                }

                if (env.containsKey("API_USER_KEY")) {
                    this.dataConnectionsApiKey = env.get("API_USER_KEY");
                }
                else {
                    this.dataConnectionsApiKey = properties.get("data-connections-api-key");
                }

                if (env.containsKey("API_CRYPTO_KEY")) {
                    this.cryptoKey = env.get("API_CRYPTO_KEY");
                }
                else {
                    this.cryptoKey = properties.get("data-connections-crypto-key");
                }
            }

            Map<String, String> shannonDbConfig = this.readConfigFile(SHANNONDB_CONFIG_FILE);

            if (shannonDbConfig.size() > 0) {
                shannondbDataConnection = new DataConnection(
                        BigInteger.ONE,
                        SHANNONDB_CONNECTOR_NAME,
                        0,
                        LocalDateTime.now(ZoneOffset.UTC),
                        null,
                        null,
                        "active",
                        shannonDbConfig);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getDisabledCatalogs()
    {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public DynamicCatalogStoreConfig setDisabledCatalogs(String catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : SPLITTER.splitToList(catalogs);
        return this;
    }

    public String getCryptoKey()
    {
        return cryptoKey;
    }

    public DynamicCatalogStoreConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }

    /**
     * Gets the directory where catalog files are stored.
     *
     * @return the directory where catalog files are stored
     */
    public File getCatalogConfigurationDir()
    {
        return this.catalogConfigurationDir;
    }

    private Map<String, String> readConfigFile(String propertyFileName)
            throws IOException
    {
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().equals(propertyFileName)) {
                return loadApiConfigFile(file);
            }
        }
        return ImmutableMap.of();
    }

    private List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private Map<String, String> loadApiConfigFile(File file)
            throws IOException
    {
        return new HashMap<>(loadProperties(file));
    }

    public static enum ShannonDbConfigProperties
    {
        HOST("connection-host"),
        PORT("connection-port");

        private String configName;

        ShannonDbConfigProperties(String configName)
        {
            this.configName = configName;
        }

        public String getConfigName()
        {
            return configName;
        }
    }

    public Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
