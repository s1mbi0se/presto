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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.testing.TestingResponse;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestDynamicCatalogStore
{

    @Test
    public void shouldGetCatalogPropertiesFromDataConnectionSettings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "host", "0.0.0.0",
                "host-port", "1234",
                "database-name", "db",
                "username", "myuser",
                "password", "mypass");

        Map<String, String> actual = DataConnectionParser.getCatalogProperties("mysql", properties, LocalDateTime.now(),
                DataConnectionType.MYSQL.getValue(), null);

        Map<String, String> expected = ImmutableMap.of(
                "connection-url", "jdbc:mysql://0.0.0.0:1234",
                "connection-user", "myuser",
                "connection-password", "mypass");

        assertThat(actual).containsAllEntriesOf(expected);
    }

    @Test
    public void shouldGetDataConnectionTypeString()
    {
        DataConnection mysql = new DataConnection(BigInteger.ONE, "sample", 1, LocalDateTime.now(), null, null, "active", ImmutableMap.of());
        DataConnection postgres = new DataConnection(BigInteger.ONE, "sample", 2, LocalDateTime.now(), null, null, "active", ImmutableMap.of());
        DataConnection shannondb = new DataConnection(BigInteger.ONE, "sample", 0, LocalDateTime.now(), null, null, "active", ImmutableMap.of());
        DataConnection hive = new DataConnection(BigInteger.ONE, "sample", 11, LocalDateTime.now(), null, null, "active", ImmutableMap.of());

        String connectorNameMysql = DataConnectionType.valueOf(mysql.getTypeId()).toString();
        String connectorNamePostgres = DataConnectionType.valueOf(postgres.getTypeId()).toString();
        String connectorNameShannondb = DataConnectionType.valueOf(shannondb.getTypeId()).toString();
        String connectorNameHive = DataConnectionType.valueOf(hive.getTypeId()).toString();

        String expectedMysql = "MYSQL";
        String expectedPostgres = "POSTGRESQL";
        String expectedShannondb = "SHANNONDB";
        String expectedHive = "HIVE";

        assertThat(connectorNameMysql).isEqualTo(expectedMysql);
        assertThat(connectorNamePostgres).isEqualTo(expectedPostgres);
        assertThat(connectorNameShannondb).isEqualTo(expectedShannondb);
        assertThat(connectorNameHive).isEqualTo(expectedHive);
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

    @Test
    public void shouldGetCatalogName()
    {
        DataConnection dataConnection = new DataConnection(BigInteger.valueOf(1), "sample1", 1, LocalDateTime.now(), null, null, "active", ImmutableMap.of());

        assertThat(dataConnection.getId()).isEqualTo(BigInteger.valueOf(1));
        final String expected = "sample1_1";
        assertThat(DynamicCatalogStore.getCatalogName(dataConnection)).isEqualTo(expected);
    }

    @Test
    public void shouldThrowsExceptionFromError500()
            throws URISyntaxException
    {
        String ipSet = "localhost;157.1.1.1;server3;server4;server5";
        DynamicCatalogStoreRoundRobin rr = DynamicCatalogStoreRoundRobin.getInstance(ipSet);
        final ApiServiceClient apiService = PowerMockito.spy(new ApiServiceClient(rr, "", ""));
        TestingResponse testingResponse = new TestingResponse(HttpStatus.INTERNAL_SERVER_ERROR, ImmutableListMultimap.of(), new byte[] {});

        PowerMockito.when(apiService.uriFor("localhost", "8080")).thenReturn(new URI("http://host:80800"));
        PowerMockito.when(apiService.executeApiRequest("/data_connection/", "id?3")).thenThrow(new UnexpectedResponseException("Error 500", null, testingResponse));

        verify(apiService, times(1)).doExecute("/data_connection/", "id?3", "localhost");
        verify(apiService, times(1)).doExecute("/data_connection/", "id?3", "157.1.1.1");
        verify(apiService, times(1)).doExecute("/data_connection/", "id?3", "server3");
        verify(apiService, times(1)).doExecute("/data_connection/", "id?3", "server4");
        verify(apiService, times(1)).doExecute("/data_connection/", "id?3", "server5");
    }
}
