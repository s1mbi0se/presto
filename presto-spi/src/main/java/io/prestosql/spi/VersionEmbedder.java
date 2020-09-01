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
package io.prestosql.spi;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public interface VersionEmbedder
{
    /**
     * Encodes Presto server version information in the class name.
     *
     * @param runnable the object that will be modified
     * @return an object with embedded version information in its class name
     */
    Runnable embedVersion(Runnable runnable);

    /**
     * Encodes Presto server version information in the class name.
     *
     * @param runnable the object that will be modified
     * @param <T> the type of returned parameter when the callable is executed
     * @return an object with embedded version information in its class name
     */
    <T> Callable<T> embedVersion(Callable<T> runnable);

    /**
     * Gets an executor that automatically embeds the server version when executes a runnable.
     *
     * @param delegate the executor that will execute the runnable
     * @return an executor that automatically embeds the server version when executes a runnable.
     */
    default Executor embedVersion(Executor delegate)
    {
        requireNonNull(delegate, "delegate is null");
        return runnable -> delegate.execute(embedVersion(runnable));
    }
}
