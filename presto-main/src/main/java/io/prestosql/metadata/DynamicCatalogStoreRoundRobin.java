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

public class DynamicCatalogStoreRoundRobin
{
    public static String[] ipSet;

    public DynamicCatalogStoreRoundRobin(String ipSet)
    {
        this.ipSet = ipSet.split(";");
    }

    private static Integer position = 0;

    public String getServer()
    {
        String target = null;

        synchronized (position) {
            if (position > ipSet.length - 1) {
                position = 0;
            }
            target = ipSet[position];
            position++;
        }
        return target;
    }
}