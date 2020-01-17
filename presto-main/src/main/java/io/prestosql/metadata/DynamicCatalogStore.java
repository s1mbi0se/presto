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
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpStatus;
import org.weakref.jmx.internal.guava.base.Charsets;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final String catalogDataConnectionEndpoint;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final JsonCodec<Map<String, Object>> mapCodec = mapJsonCodec(String.class, Object.class);
    private final HttpClient httpClient;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogStoreConfig config)
    {
        this(connectorManager,
                config.getCatalogDataConnectionEndpoint(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
    }

    public DynamicCatalogStore(ConnectorManager connectorManager, String catalogDataConnectionEndpoint, List<String> disabledCatalogs)
    {
        this.connectorManager = connectorManager;
        this.catalogDataConnectionEndpoint = catalogDataConnectionEndpoint;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.httpClient = new HttpClient();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (DataConnection dataConnection : listDataConnections(catalogDataConnectionEndpoint)) {
            loadCatalog(dataConnection);
        }
    }

    private void loadCatalog(DataConnection dataConnection)
            throws Exception
    {
        String catalogName = dataConnection.getName();
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", dataConnection);
        Map<String, String> properties = dataConnection.getSettings();

        String connectorName = properties.remove("connector.name");
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", dataConnection.getName());

        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private List<DataConnection> listDataConnections(String catalogDataConnectionEndpoint)
            throws URISyntaxException, IOException, InterruptedException, ExecutionException, TimeoutException
    {
//        URI uri = HttpUriBuilder.uriBuilderFrom(new URI(catalogDataConnectionEndpoint)).build();
//        Request request = prepareGet()
//                .setUri(uri)
//                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
//                .setHeader(AUTHORIZATION, "4")
//                .build();
//
//        HttpClient.HttpResponseFuture<HttpPageBufferClient.PagesResponse> resultFuture = httpClient.executeAsync(
//                request,
//                new HttpPageBufferClient.PageResponseHandler());

        InputStreamResponseListener listener = new InputStreamResponseListener();
        httpClient.newRequest(catalogDataConnectionEndpoint)
                .header(CONTENT_TYPE, JSON_UTF_8.toString())
                .header(AUTHORIZATION, "4")
                .send(listener);

        // Wait for the response headers to arrive
        Response response = listener.get(5, TimeUnit.SECONDS);

        // Look at the response
        if (response.getStatus() == HttpStatus.OK_200)
        {
            // Use try-with-resources to close input stream.
            try (InputStream responseContent = listener.getInputStream())
            {
                // Your logic here
                return parseDataConnectionRequest(responseContent);
            }
        }

//        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
//            File[] files = installedPluginsDir.listFiles();
//            if (files != null) {
//                return ImmutableList.copyOf(files);
//            }
//        }
        return ImmutableList.of();
    }

    private List<DataConnection> parseDataConnectionRequest(InputStream responseContent)
            throws IOException
    {
        String data = CharStreams.toString(new InputStreamReader(responseContent, Charsets.UTF_8));
        System.out.println(data);

        return ImmutableList.of();
    }
}
