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
package io.prestosql.dispatcher;

import io.airlift.units.Duration;
import io.prestosql.execution.ExecutionFailureInfo;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatchInfo
{
    private final Optional<CoordinatorLocation> coordinatorLocation;
    private final Optional<ExecutionFailureInfo> failureInfo;
    private final Duration elapsedTime;
    private final Duration queuedTime;

    /**
     * Creates a new DispatchInfo object with information about a query waiting
     * to be processed.
     *
     * @param elapsedTime time elapsed until the query response
     * @param queuedTime time the query is waiting to be executed
     * @return a new DispatchInfo instance with information about a query waiting to be processed
     */
    public static DispatchInfo queued(Duration elapsedTime, Duration queuedTime)
    {
        return new DispatchInfo(Optional.empty(), Optional.empty(), elapsedTime, queuedTime);
    }

    /**
     * Creates a new DispatchInfo object with information about a query that
     * is being processed.
     *
     * @param coordinatorLocation an object that contains information about the URI of the coordinator
     * @param elapsedTime time elapsed until the query response
     * @param queuedTime time the query waited to be executed
     * @return a new DispatchInfo instance with information about a query that is being processed
     */
    public static DispatchInfo dispatched(CoordinatorLocation coordinatorLocation, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        return new DispatchInfo(Optional.of(coordinatorLocation), Optional.empty(), elapsedTime, queuedTime);
    }

    /**
     * Creates a new DispatchInfo object with information about a query that failed during
     * its execution.
     *
     * @param failureInfo an object with details about failures that occurred during query
     * execution
     * @param elapsedTime time elapsed until the query response
     * @param queuedTime time the query waited to be executed
     * @return a query that failed during its execution
     */
    public static DispatchInfo failed(ExecutionFailureInfo failureInfo, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(failureInfo, "failureInfo is null");
        return new DispatchInfo(Optional.empty(), Optional.of(failureInfo), elapsedTime, queuedTime);
    }

    private DispatchInfo(Optional<CoordinatorLocation> coordinatorLocation, Optional<ExecutionFailureInfo> failureInfo, Duration elapsedTime, Duration queuedTime)
    {
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
    }

    /**
     * Returns an object with information about coordinator's URI.
     *
     * @return an Optional object with information about coordinator's URI
     * @see java.net.URI
     */
    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return coordinatorLocation;
    }

    /**
     * Returns detailed information about a failure during query execution, such as its type,
     * message and cause.
     *
     * @return an Optional object with detailed information about a failure during query execution
     */
    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    /**
     * Returns the elapsed time of the query.
     *
     * @return the elapsed time of the query
     */
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    /**
     * Returns the time the query waited to be processed.
     *
     * @return the time the query waited to be processed
     */
    public Duration getQueuedTime()
    {
        return queuedTime;
    }
}
