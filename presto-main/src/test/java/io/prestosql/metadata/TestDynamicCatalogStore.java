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

import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.testing.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestDynamicCatalogStore
{

    private HttpClient client;
    private TestingHttpServer server;
    private final JsonCodec<List<Object>> listCodec = listJsonCodec(Object.class);
    private LifeCycleManager liceCycleManager;

    @BeforeMethod
    public void setup()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new InMemoryEventModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule()
//                new MainModule()
        );

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        liceCycleManager = injector.getInstance(LifeCycleManager.class);

        client = new JettyHttpClient();
    }

    @Test
    public void shouldRetrieveDataConnectionResponse()
            throws IOException
    {
        List<Object> expected = listCodec.fromJson(Resources.toString(Resources.getResource("data_connection_list.json"), UTF_8));

        List<Object> response = client.execute(
                prepareGet().setUri(uriFor("/data_connections")).build(),
                createJsonResponseHandler(listCodec));

        Assertions.assertEqualsIgnoreOrder(expected, response);
    }

    private URI uriFor(String path)
    {
        try {
            URI uri = new URI("http", null, "localhost", 8080, null, null, null);
            return uri.resolve(path);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
