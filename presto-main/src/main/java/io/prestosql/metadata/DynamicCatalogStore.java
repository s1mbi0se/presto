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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.spi.PrestoException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.inject.Inject;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.metadata.DynamicCatalogStoreErrorCode.DATA_CONNECTION_REQUEST_FAILED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.joda.time.DateTimeZone.UTC;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private static DateTime lastCatalogDeltaDateTime;
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    private final String baseDeltaQueryParameters = "?created-after=%s&updated-after=%s";
    private final ConnectorManager connectorManager;
    private final CatalogDeltaRetrieverScheduler scheduler;
    private final String dataConnectionEndpoint;
    private final DynamicCatalogStoreRoundRobin dataConnectionUrls;
    private final String dataConnectionApiKey;
    private final String dataConnectionCryptoKey;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final HttpClient httpClient;
    private final JsonCodec<DataConnectionResponse> jsonCodec = jsonCodec(DataConnectionResponse.class);
    private final Announcer announcer;
    private final CatalogManager catalogManager;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogStoreConfig config,
            CatalogDeltaRetrieverScheduler scheduler,
            Announcer announcer,
            CatalogManager catalogManager)
    {
        this(connectorManager,
                config.getDataConnectionsEndpoint(),
                config.getDataConnectionsUrl(),
                config.getDataConnectionsApiKey(),
                config.getCryptoKey(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()), scheduler, announcer, catalogManager);
    }

    public DynamicCatalogStore(
            ConnectorManager connectorManager,
            String dataConnectionEndpoint,
            String dataConnectionUrl,
            String dataConnectionApiKey,
            String dataConnectionCryptoKey,
            List<String> disabledCatalogs,
            CatalogDeltaRetrieverScheduler scheduler,
            Announcer announcer,
            CatalogManager catalogManager)
    {
        this.connectorManager = connectorManager;
        this.dataConnectionEndpoint = requireNonNull(dataConnectionEndpoint, "dataConnectionEndpoint is null.");
        this.dataConnectionApiKey = requireNonNull(dataConnectionApiKey, "dataConnectionApiKey is null.");
        this.dataConnectionCryptoKey = requireNonNull(dataConnectionCryptoKey, "dataConnectionCryptoKey is null");
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.dataConnectionUrls = new DynamicCatalogStoreRoundRobin(requireNonNull(dataConnectionUrl, "dataConnectionUrl is null."));
        this.scheduler = scheduler;
        this.httpClient = new JettyHttpClient();
        this.announcer = announcer;
        this.catalogManager = catalogManager;
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (DataConnection dataConnection : listAllDataConnections()) {
            loadCatalog(dataConnection);
        }

        if (!connectorManager.getCatalogManager().getCatalog(DynamicCatalogStoreConfig.SHANNONDB_CONNECTOR_NAME).isPresent()) {
            loadCatalog(DynamicCatalogStoreConfig.shannondbDataConnection);
        }

        scheduler.schedule(() -> {
            try {
                updateCatalogDelta();
            }
            catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }, 5);
    }

    public void loadCatalog(DataConnection dataConnection)
            throws Exception
    {
        String catalogName = dataConnection.getName().toLowerCase(ENGLISH);
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        String connectorName = DataConnectionType.valueOf(dataConnection.getTypeId()).getName();
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", dataConnection.getName());

        log.info("-- Loading catalog %s --", dataConnection);
        Map<String, String> properties = DataConnectionParser.getCatalogProperties(connectorName, dataConnection.getSettings(), dataConnection.getCreatedAt(),
                dataConnection.getTypeId(), dataConnectionCryptoKey);

        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private void updateCatalogDelta()
            throws Exception
    {
        log.info("updating catalogs");
        List<DataConnection> delta = listCatalogDelta();
        if (delta.size() > 0) {
            for (DataConnection dataConnection : delta) {
                log.debug(dataConnection.toString());
                if (!dataConnection.getStatus().equals("active")) {
                    if (connectorManager.getCatalogManager().getCatalog(dataConnection.getName()).isPresent()) {
                        log.info(String.format("Decommissioning data connection %s.", dataConnection.getName()));
                        connectorManager.dropConnection(dataConnection.getName());
                    }
                }
                else {
                    if (!connectorManager.getCatalogManager().getCatalog(dataConnection.getName()).isPresent()) {
                        log.info(String.format("Found new data connection %s. Loading...", dataConnection.getName()));
                        loadCatalog(dataConnection);
                    }
                }
            }
        }
        updateConnectorIds(announcer, catalogManager);
    }

    private static void updateConnectorIds(Announcer announcer, CatalogManager metadata)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // automatically build connectorIds if not configured
        Set<String> connectorIds = metadata.getCatalogs().stream()
                .map(Catalog::getConnectorCatalogName)
                .map(Object::toString)
                .collect(toImmutableSet());

        if (!announcement.getProperties().containsKey("connectorIds")
                || !Joiner.on(',').join(connectorIds).equals(announcement.getProperties().get("connectorIds"))) {
            // build announcement with updated sources
            ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
            builder.addProperties(announcement.getProperties().entrySet().stream().filter(p ->
                    !p.getKey().equals("connectorIds")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

            // update announcement
            announcer.removeServiceAnnouncement(announcement.getId());
            announcer.addServiceAnnouncement(builder.build());
        }
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    private List<DataConnection> listAllDataConnections()
            throws URISyntaxException, IOException, InterruptedException, ExecutionException, TimeoutException
    {
        return getDataConnections(dataConnectionEndpoint, "?status=active");
    }

    private List<DataConnection> listCatalogDelta()
    {
        return getDataConnections(dataConnectionEndpoint, resolveDeltaQueryParameter());
    }

    private String resolveDeltaQueryParameter()
    {
        if (lastCatalogDeltaDateTime == null) {
            lastCatalogDeltaDateTime = DateTime.now(UTC);
        }

        try {
            String result = String.format(baseDeltaQueryParameters, dateTimeFormatter.print(lastCatalogDeltaDateTime), dateTimeFormatter.print(lastCatalogDeltaDateTime));
            lastCatalogDeltaDateTime = DateTime.now(UTC);
            return result;
        }
        catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return "";
        }
    }

    private List<DataConnection> getDataConnections(String dataConnectionEndpoint, String queryParameters)
    {
        DataConnectionResponse response = null;
        try {
            response = httpClient.execute(
                    prepareGet().setUri(uriFor(dataConnectionUrls.getServer(), dataConnectionEndpoint + queryParameters))
                            .setHeader(AUTHORIZATION, dataConnectionApiKey)
                            .build(),
                    createJsonResponseHandler(jsonCodec));
        }
        catch (Exception e) {
            if (e.getCause() instanceof ConnectException) {
                log.error("Unable to connect to API");
                log.error(e.getMessage());
            }
            else {
                throw new PrestoException(DATA_CONNECTION_REQUEST_FAILED, e);
            }
        }

        return getDataConnectionsFromResponse(response);
    }

    private List<DataConnection> getDataConnectionsFromResponse(DataConnectionResponse response)
    {
        if (response != null && response.getContent() != null && response.getContent().size() > 0) {
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

    public ConnectorManager getConnectorManager()
    {
        return connectorManager;
    }
}
