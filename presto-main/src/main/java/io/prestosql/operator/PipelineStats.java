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
package io.prestosql.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class PipelineStats
{
    private final int pipelineId;

    private final DateTime firstStartTime;
    private final DateTime lastStartTime;
    private final DateTime lastEndTime;

    private final boolean inputPipeline;
    private final boolean outputPipeline;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;
    private final DataSize systemMemoryReservation;

    private final DistributionSnapshot queuedTime;
    private final DistributionSnapshot elapsedTime;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;
    private final Duration physicalInputReadTime;

    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final DataSize physicalWrittenDataSize;

    private final List<OperatorStats> operatorSummaries;
    private final List<DriverStats> drivers;

    @JsonCreator
    public PipelineStats(
            @JsonProperty("pipelineId") int pipelineId,

            @JsonProperty("firstStartTime") DateTime firstStartTime,
            @JsonProperty("lastStartTime") DateTime lastStartTime,
            @JsonProperty("lastEndTime") DateTime lastEndTime,

            @JsonProperty("inputPipeline") boolean inputPipeline,
            @JsonProperty("outputPipeline") boolean outputPipeline,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") DataSize systemMemoryReservation,

            @JsonProperty("queuedTime") DistributionSnapshot queuedTime,
            @JsonProperty("elapsedTime") DistributionSnapshot elapsedTime,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("physicalInputReadTime") Duration physicalInputReadTime,

            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,
            @JsonProperty("drivers") List<DriverStats> drivers)
    {
        this.pipelineId = pipelineId;

        this.firstStartTime = firstStartTime;
        this.lastStartTime = lastStartTime;
        this.lastEndTime = lastEndTime;

        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        this.systemMemoryReservation = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null");

        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");

        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        checkArgument(physicalInputPositions >= 0, "physicalInputPositions is negative");
        this.physicalInputPositions = physicalInputPositions;
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");

        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        checkArgument(internalNetworkInputPositions >= 0, "internalNetworkInputPositions is negative");
        this.internalNetworkInputPositions = internalNetworkInputPositions;

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null");

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
        this.drivers = ImmutableList.copyOf(requireNonNull(drivers, "drivers is null"));
    }

    @JsonProperty
    public int getPipelineId()
    {
        return pipelineId;
    }

    @Nullable
    @JsonProperty
    public DateTime getFirstStartTime()
    {
        return firstStartTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getLastStartTime()
    {
        return lastStartTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getLastEndTime()
    {
        return lastEndTime;
    }

    @JsonProperty
    public boolean isInputPipeline()
    {
        return inputPipeline;
    }

    @JsonProperty
    public boolean isOutputPipeline()
    {
        return outputPipeline;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getSystemMemoryReservation()
    {
        return systemMemoryReservation;
    }

    @JsonProperty
    public DistributionSnapshot getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public DistributionSnapshot getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the physical input data size in bytes.
     *
     * @return the size of the input data in bytes.
     */
    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    /**
     * Returns a property that represents the physical input positions of the PipelineStats.
     *
     * @return the physical input positions.
     */
    @JsonProperty
    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the internal network input data size in bytes.
     *
     * @return the internal network input data size in bytes.
     */
    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    @JsonProperty
    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the raw input data size in bytes.
     *
     * @return the raw input data size in bytes.
     */
    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    /**
     * Returns a property that represents the raw input positions of the PipelineStats.
     *
     * @return the raw input positions.
     */
    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the processed input data size in bytes.
     *
     * @return the processed input data size in bytes.
     */
    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    /**
     * Returns a property that represents the processed input positions of the PipelineStats.
     *
     * @return the processed input positions.
     */
    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    /**
     * Returns an object of type {@link Duration} that contains the physical input read time.
     *
     * @return the physical input read time.
     */
    @JsonProperty
    public Duration getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the output data size in bytes.
     *
     * @return the output data size.
     */
    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    /**
     * Returns a property that represents the output positions of the PipelineStats.
     *
     * @return the output positions.
     */
    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    /**
     * Returns an object of type {@link DataSize} that contains the physical written data size in bytes.
     *
     * @return the physical written data size.
     */
    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public List<DriverStats> getDrivers()
    {
        return drivers;
    }

    public PipelineStats summarize()
    {
        return new PipelineStats(
                pipelineId,
                firstStartTime,
                lastStartTime,
                lastEndTime,
                inputPipeline,
                outputPipeline,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                runningDrivers,
                runningPartitionedDrivers,
                blockedDrivers,
                completedDrivers,
                userMemoryReservation,
                revocableMemoryReservation,
                systemMemoryReservation,
                queuedTime,
                elapsedTime,
                totalScheduledTime,
                totalCpuTime,
                totalBlockedTime,
                fullyBlocked,
                blockedReasons,
                physicalInputDataSize,
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                rawInputDataSize,
                rawInputPositions,
                processedInputDataSize,
                processedInputPositions,
                outputDataSize,
                outputPositions,
                physicalWrittenDataSize,
                operatorSummaries.stream()
                        .map(OperatorStats::summarize)
                        .collect(Collectors.toList()),
                ImmutableList.of());
    }
}
