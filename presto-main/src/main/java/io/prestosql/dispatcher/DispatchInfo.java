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
     * Takes the waiting time and the elapsed time.
     * <p>
     * Receives elapsed time and waiting time and return
     * an object of type {@link DispatchInfo}.
     *
     * @param elapsedTime time elapsed until the query response
     * @param queuedTime queue waiting time
     *
     * @return a {@link DispatchInfo} object
     */
    public static DispatchInfo queued(Duration elapsedTime, Duration queuedTime)
    {
        return new DispatchInfo(Optional.empty(), Optional.empty(), elapsedTime, queuedTime);
    }

    public static DispatchInfo dispatched(CoordinatorLocation coordinatorLocation, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        return new DispatchInfo(Optional.of(coordinatorLocation), Optional.empty(), elapsedTime, queuedTime);
    }

    public static DispatchInfo failed(ExecutionFailureInfo failureInfo, Duration elapsedTime, Duration queuedTime)
    {
        requireNonNull(failureInfo, "coordinatorLocation is null");
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
     * Returns the coordinator's uri.
     *
     * @return an Option {@link CoordinatorLocation}.
     */
    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return coordinatorLocation;
    }

    /**
     * If there was a failure, it returns detailed information such as the type, the message
     * and cause.
     *
     * @return an Optional {@link ExecutionFailureInfo}.
     */
    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    /**
     * Returns the elapsed time of the query.
     *
     * @return a {@link Duration} object.
     */
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    /**
     * Returns the queued time.
     *
     * @return a {@link Duration} object.
     */
    public Duration getQueuedTime()
    {
        return queuedTime;
    }
}
