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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Locale.ENGLISH;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final String dataConnectionEndpoint;
    private final String dataConnectionUrl;
    private final String dataConnectionApiKey;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final HttpClient httpClient;
    private final JsonCodec<DataConnectionResponse> jsonCodec = jsonCodec(DataConnectionResponse.class);

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogStoreConfig config)
    {
        this(connectorManager,
                config.getDataConnectionsEndpoint(),
                config.getDataConnectionsUrl(),
                config.getDataConnectionsApiKey(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
    }

    public DynamicCatalogStore(
            ConnectorManager connectorManager,
            String dataConnectionEndpoint,
            String dataConnectionUrl,
            String dataConnectionApiKey,
            List<String> disabledCatalogs)
    {
        this.connectorManager = connectorManager;
        this.dataConnectionEndpoint = dataConnectionEndpoint;
        this.dataConnectionUrl = dataConnectionUrl;
        this.dataConnectionApiKey = dataConnectionApiKey;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.httpClient = new JettyHttpClient();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (DataConnection dataConnection : listDataConnections()) {
            loadCatalog(dataConnection);
        }
    }

    private void loadCatalog(DataConnection dataConnection)
            throws Exception
    {
        String catalogName = dataConnection.getName().toLowerCase(ENGLISH);
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        String connectorName = DataConnectionType.valueOf(dataConnection.getTypeId()).toString();
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", dataConnection.getName());
        connectorName = connectorName.toLowerCase(ENGLISH);

        log.info("-- Loading catalog %s --", dataConnection);
        Map<String, String> properties = DataConnectionParser.getCatalogProperties(connectorName, dataConnection.getSettings());

        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private List<DataConnection> listDataConnections()
            throws URISyntaxException, IOException, InterruptedException, ExecutionException, TimeoutException
    {
        DataConnectionResponse response = httpClient.execute(
                prepareGet().setUri(uriFor(dataConnectionUrl, dataConnectionEndpoint))
                        .setHeader(AUTHORIZATION, dataConnectionApiKey)
                        .build(),
                createJsonResponseHandler(jsonCodec));

        if (response.getContent() != null && response.getContent().size() > 0) {
            return ImmutableList.copyOf(response.getContent());
        }

        return ImmutableList.of();
    }

    private URI uriFor(String dataConnectionUrl, String dataConnectionEndpoint)
    {
        try {
            URI uri = new URI("http", null, dataConnectionUrl, 8080, null, null, null);
            return uri.resolve(dataConnectionEndpoint);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private List<DataConnection> parseDataConnectionRequest(InputStream responseContent)
            throws IOException
    {
        String data = CharStreams.toString(new InputStreamReader(responseContent, StandardCharsets.UTF_8));
        System.out.println(data);

        if (data != null && !"".isEmpty()) {
            DataConnectionResponse response = jsonCodec.fromJson(data);

            if (response.getContent() != null && response.getContent().size() > 0) {
                return response.getContent();
            }
        }
        return ImmutableList.of();
    }
}
