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
import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

public class DynamicCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String catalogDataConnectionEndpoint = "/v1/data_connections";
    private List<String> disabledCatalogs;

    @NotNull
    public String getCatalogDataConnectionEndpoint()
    {
        return catalogDataConnectionEndpoint;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-dir")
    public DynamicCatalogStoreConfig setCatalogDataConnectionEndpoint(String uri)
    {
        this.catalogDataConnectionEndpoint = uri;
        return this;
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
}
