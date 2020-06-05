package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class ApiService
{
    private DynamicCatalogStoreRoundRobin dataConnectionUrls;
    private String queryParameters;
    private final HttpClient httpClient;
    private String dataConnectionApiKey;
    private JsonCodec<DataConnectionResponse> jsonCodec = jsonCodec(DataConnectionResponse.class);
    private static final io.airlift.log.Logger log = io.airlift.log.Logger.get(ApiService.class);

    private ExecuteApiRequest executeApiRequest;

    public ApiService(DynamicCatalogStoreRoundRobin dataConnectionUrls, String queryParameters, String dataConnectionApiKey)
    {
        this.dataConnectionUrls = dataConnectionUrls;
        this.queryParameters = queryParameters;
        this.httpClient = new JettyHttpClient();
        this.dataConnectionApiKey = dataConnectionApiKey;
    }

    protected List<DataConnection> executeApiRequest(String dataConnectionEndpoint, String queryParameters)
    {
        DataConnectionResponse response = null;
        Integer poolSize = dataConnectionUrls.getPoolSize();
        for (int i = 0; i < poolSize; i++) {
            String apiServer = null;
            try {
                apiServer = dataConnectionUrls.getServer();
                response = doExecute(dataConnectionEndpoint, queryParameters, apiServer);
                log.debug(String.format("API server [%s] - ok - request %s", apiServer, (queryParameters.contains("delete") ? "delete delta" : "delta")));
                log.debug(dataConnectionEndpoint + queryParameters);
                break;
            }
            catch (Exception e) {
                log.error(String.format("API server [%s] - unable to connect - request %s", apiServer, (queryParameters.contains("delete") ? "delete delta" : "delta")));
                log.error(dataConnectionEndpoint + queryParameters);
                log.error(e.getMessage());
            }
        }

        return getDataConnectionsFromResponse(response);
    }

    protected DataConnectionResponse doExecute(String dataConnectionEndpoint, String queryParameters, String apiServer)
    {
        return httpClient.execute(
                prepareGet().setUri(uriFor(apiServer, dataConnectionEndpoint + queryParameters))
                        .setHeader(AUTHORIZATION, dataConnectionApiKey)
                        .build(),
                createJsonResponseHandler(jsonCodec));
    }

    protected URI uriFor(String dataConnectionUrl, String dataConnectionEndpoint)
    {
        try {
            URI uri = new URI("http", null, dataConnectionUrl, 8080, null, null, null);
            return uri.resolve(dataConnectionEndpoint);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private List<DataConnection> getDataConnectionsFromResponse(DataConnectionResponse response)
    {
        if (response != null && response.getContent() != null && response.getContent().size() > 0) {
            return ImmutableList.copyOf(response.getContent());
        }

        return ImmutableList.of();
    }

    public void setExecuteApiRequest(ExecuteApiRequest executeApiRequest) {
        this.executeApiRequest = executeApiRequest;
    }
}


