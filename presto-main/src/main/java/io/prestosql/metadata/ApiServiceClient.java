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

public class ApiServiceClient
    implements ApiService
{
    private DynamicCatalogStoreRoundRobin dataConnectionUrls;
    private String queryParameters;
    private final HttpClient httpClient;
    private String dataConnectionApiKey;
    private JsonCodec<DataConnectionResponse> jsonCodec = jsonCodec(DataConnectionResponse.class);
    private static final io.airlift.log.Logger log = io.airlift.log.Logger.get(ApiServiceClient.class);

    public ApiServiceClient(DynamicCatalogStoreRoundRobin dataConnectionUrls, String queryParameters, String dataConnectionApiKey)
    {
        this.dataConnectionUrls = dataConnectionUrls;
        this.queryParameters = queryParameters;
        this.httpClient = new JettyHttpClient();
        this.dataConnectionApiKey = dataConnectionApiKey;
    }

    /**
     * Executes the API's request to list all data_connections.
     * <p>
     * This method takes the String endpoint and the desired String query parameters.
     * Then it will go through all the available IPs to connect to the API. If not, it will throw an exception that will be
     * handled in the try/catch block.
     * </p>
     *
     * @param dataConnectionEndpoint a String that represents the endpoint responsible for listing all data_connections.
     * @param queryParameters a String that is responsible for querying/filtering.
     * @return a list of all data_connections.
     **/
    public List<DataConnection> executeApiRequest(String dataConnectionEndpoint, String queryParameters)
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

    /**
     * Responsible for getting the API address.
     * <p>
     *  This method takes dataConnectionEndpoint, queryParameters and apiServer as parameters and, therefore, returns the address where
     *  it is possible to execute the query.
     * </p>
     *
     * @param dataConnectionEndpoint  a String that represents the endpoint responsible for listing all data_connections.
     * @param queryParameters  a String that is responsible for querying/filtering.
     * @param apiServer  a String that represents the API's server.
     * **/
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
}


