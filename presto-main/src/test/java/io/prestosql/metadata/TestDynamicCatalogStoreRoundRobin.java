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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicCatalogStoreRoundRobin
{
    private DynamicCatalogStoreRoundRobin rr;
    private String ipSet = "localhost;127.0.0.1;server3;server4";

    @BeforeTest
    public void setup()
    {
        rr = DynamicCatalogStoreRoundRobin.getInstance(ipSet);
    }

    @Test(priority = 1)
    public void shouldGetDataConnectionTypeString()
    {
        assertThat(rr.getServer()).isEqualTo("localhost");
        assertThat(rr.getServer()).isEqualTo("127.0.0.1");
        assertThat(rr.getServer()).isEqualTo("server3");
        assertThat(rr.getServer()).isEqualTo("server4");
        assertThat(rr.getServer()).isEqualTo("localhost");
        assertThat(rr.getServer()).isEqualTo("127.0.0.1");
        assertThat(rr.getServer()).isEqualTo("server3");
        assertThat(rr.getServer()).isEqualTo("server4");
    }

    @Test(priority = 1)
    public void shouldGetPoolSize()
    {
        String ipSet = "localhost;127.0.0.1;server3;server4";
        DynamicCatalogStoreRoundRobin rr = DynamicCatalogStoreRoundRobin.getInstance(ipSet);
        assertThat(rr.getPoolSize()).isEqualTo(4);
    }
}
