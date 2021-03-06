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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class CatalogDeltaRetrieverScheduler
{
    private static final ScheduledExecutorService updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("catalog-list-update-scheduler-%s"));

    public CatalogDeltaRetrieverScheduler() {}

    public void schedule(Runnable runnable, final int delay)
    {
        updateScheduledExecutor.scheduleWithFixedDelay(runnable, delay, delay, TimeUnit.SECONDS);
    }
}
