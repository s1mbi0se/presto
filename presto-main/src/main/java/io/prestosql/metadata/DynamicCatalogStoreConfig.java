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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.util.PropertiesUtil.loadProperties;

public class DynamicCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private File catalogConfigurationDir = new File("etc/catalog/");
    private String dataConnectionsEndpoint;
    private String dataConnectionsUrl;
    private String dataConnectionsApiKey;
    private List<String> disabledCatalogs;

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
            properties = this.readApiConfigFile();
            this.dataConnectionsEndpoint = properties.get("data-connections-endpoint");
            this.dataConnectionsUrl = properties.get("data-connections-url");
            this.dataConnectionsApiKey = properties.get("data-connections-api-key");
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

    public DynamicCatalogStoreConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }

    private Map<String, String> readApiConfigFile()
            throws IOException
    {
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().equals("api-config.properties")) {
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
}
