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
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicCatalogStore
{
    private final JsonCodec<DataConnectionResponse> jsonCodec = jsonCodec(DataConnectionResponse.class);
//    private final DynamicCatalogStore store;
//
//    public TestDynamicCatalogStore(DynamicCatalogStore store)
//    {
//        this.store = store;
//    }

    @BeforeMethod
    public void setup()
    {
    }

    @Test
    public void shouldRetrieveDataConnectionResponse()
            throws IOException
    {
//        List<DataConnection> expected = jsonCodec.fromJson(Resources.toString(Resources.getResource("data_connection_list.json"), UTF_8)).getContent();
//
//        DataConnectionResponse response = client.execute(
//                prepareGet().setUri(uriFor("/v1/data_connections/all"))
//                        .setHeader("authorization", "")
//                        .build(),
//                createJsonResponseHandler(jsonCodec));
//
//        Assertions.assertEqualsIgnoreOrder(expected.getContent(), response.getContent());
    }

    @Test
    public void shouldPopulateCatalogsFromDataConnections()
    {
    }

    @Test
    public void shouldLoadNewCatalog()
            throws Exception
    {
    }

    @Test
    public void shouldDecommissionCatalog()
    {
    }

    @Test
    public void shouldGetCatalogPropertiesFromDataConnectionSettings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "host", "0.0.0.0",
                "host-port", "1234",
                "database-name", "db",
                "username", "myuser",
                "password", "mypass");

        Map<String, String> actual = DataConnectionParser.getCatalogProperties("mysql", properties, LocalDateTime.now(), null);

        Map<String, String> expected = ImmutableMap.of(
                "connection-url", "jdbc:mysql://0.0.0.0:1234",
                "connection-user", "myuser",
                "connection-password", "mypass");

        assertThat(actual).containsAllEntriesOf(expected);
    }

    @Test
    public void shouldGetDataConnectionTypeString()
    {
        DataConnection dataConnection = new DataConnection(
                BigInteger.ONE,
                "sample",
                1,
                LocalDateTime.now(),
                "active",
                ImmutableMap.of());

        String connectorName = DataConnectionType.valueOf(dataConnection.getTypeId()).toString();

        String expected = "MYSQL";

        assertThat(connectorName).isEqualTo(expected);
    }

    @Test
    public void shouldRunOnSchedule()
    {
        CatalogDeltaRetrieverScheduler scheduler = new CatalogDeltaRetrieverScheduler();

        int a = 1;
        AtomicInteger b = new AtomicInteger(0);
        scheduler.schedule(() -> { b.set(a); }, 1);

        try {
            Thread.sleep(1100);
            assertThat(b.get()).isEqualTo(1);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
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
