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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.LazyOutputBuffer;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.QueryContext;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.PipelineStatus;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskStats;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.prestosql.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTER_DOMAINS;
import static io.prestosql.execution.TaskState.ABORTED;
import static io.prestosql.execution.TaskState.FAILED;
import static io.prestosql.execution.TaskState.RUNNING;
import static io.prestosql.util.Failures.toFailures;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final String taskInstanceId;
    private final URI location;
    private final String nodeId;
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;
    private final QueryContext queryContext;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private final Executor taskNotificationExecutor;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong taskStatusVersion = new AtomicLong(TaskStatus.STARTING_VERSION);
    private final FutureStateChange<?> taskStatusVersionChange = new FutureStateChange<>();

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);

    /**
     * Creates a sql task.
     * <p>
     * Each task belongs to a stage and is processed
     * inside the workers, not the coordinator.
     *
     * @param taskId the task's identifier
     * @param location the URI where the metadata about the task is retrieved
     * @param nodeId the node of the worker where task is executed
     * @param queryContext an object with metadata about the environment where query
     * is executed
     * @param sqlTaskExecutionFactory the factory used to create the tasks
     * @param taskNotificationExecutor the executor used to process a task
     * @param onDone the function to be applied over task when they are done
     * @param maxBufferSize the max size of the exchange buffer
     * @param failedTasks an object that count the number of failed queries
     * @return
     */
    public static SqlTask createSqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            Function<SqlTask, ?> onDone,
            DataSize maxBufferSize,
            DataSize maxBroadcastBufferSize,
            CounterStat failedTasks)
    {
        SqlTask sqlTask = new SqlTask(taskId, location, nodeId, queryContext, sqlTaskExecutionFactory, taskNotificationExecutor, maxBufferSize, maxBroadcastBufferSize);
        sqlTask.initialize(onDone, failedTasks);
        return sqlTask;
    }

    private SqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            DataSize maxBufferSize,
            DataSize maxBroadcastBufferSize)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = UUID.randomUUID().toString();
        this.location = requireNonNull(location, "location is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");

        outputBuffer = new LazyOutputBuffer(
                taskId,
                taskInstanceId,
                taskNotificationExecutor,
                maxBufferSize,
                maxBroadcastBufferSize,
                // Pass a memory context supplier instead of a memory context to the output buffer,
                // because we haven't created the task context that holds the the memory context yet.
                () -> queryContext.getTaskContextByTaskId(taskId).localSystemMemoryContext(),
                () -> notifyStatusChanged());
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
    }

    /**
     * Initializes a task.
     *
     * @param onDone a function to be executed over task when it is finished
     * @param failedTasks an object that counts the number of failed tasks
     */
    private void initialize(Function<SqlTask, ?> onDone, CounterStat failedTasks)
    {
        requireNonNull(onDone, "onDone is null");
        requireNonNull(failedTasks, "failedTasks is null");
        taskStateMachine.addStateChangeListener(newState -> {
            if (!newState.isDone()) {
                if (newState != RUNNING) {
                    // notify that task state changed (apart from initial RUNNING state notification)
                    notifyStatusChanged();
                }
                return;
            }

            // Update failed tasks counter
            if (newState == FAILED) {
                failedTasks.update(1);
            }

            // store final task info
            while (true) {
                TaskHolder taskHolder = taskHolderReference.get();
                if (taskHolder.isFinished()) {
                    // another concurrent worker already set the final state
                    return;
                }

                if (taskHolderReference.compareAndSet(taskHolder, new TaskHolder(createTaskInfo(taskHolder), taskHolder.getIoStats()))) {
                    break;
                }
            }

            // make sure buffers are cleaned up
            if (newState == FAILED || newState == ABORTED) {
                // don't close buffers for a failed query
                // closed buffers signal to upstream tasks that everything finished cleanly
                outputBuffer.fail();
            }
            else {
                outputBuffer.destroy();
            }

            try {
                onDone.apply(SqlTask.this);
            }
            catch (Exception e) {
                log.warn(e, "Error running task cleanup callback %s", SqlTask.this.taskId);
            }

            // notify that task is finished
            notifyStatusChanged();
        });
    }

    /**
     * Gets a flag which indicates that output buffer is full.
     * <p>
     * If a buffer is over utilized, it becomes blocked until
     * it has available memory again.
     *
     * @return a flag which indicates that output buffer is full
     */
    public boolean isOutputBufferOverutilized()
    {
        return outputBuffer.isOverutilized();
    }

    public SqlTaskIoStats getIoStats()
    {
        return taskHolderReference.get().getIoStats();
    }

    /**
     * Gets the object with metadata about the task's identifier.
     *
     * @return the object with metadata about the task's identifier
     */
    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    /**
     * Gets the task unique identifier.
     *
     * @return the task unique identifier
     */
    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    /**
     * Records the last time that Presto act on the query.
     * <p>
     * As an example: every time that a request is executed to know the query status in Presto,
     * the last heartbeat increases.
     * If the time passed from last heartbeat is greater than the defined timeout, the query is
     * abandoned by the sever.
     */
    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    public TaskInfo getTaskInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskInfo(taskHolderReference.get());
        }
    }

    /**
     * Gets the metadata about an executed task.
     * <p>
     * The metadata contains the information about:
     * - The drivers that are being executed by the task;
     * - The amount of data processed by the task;
     * - The time that the task was stopped by full GC.
     *
     * @return the metadata about an executed task
     */
    public TaskStatus getTaskStatus()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskStatus(taskHolderReference.get());
        }
    }

    public VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(long callersDynamicFiltersVersion)
    {
        TaskHolder taskHolder = taskHolderReference.get();
        if (taskHolder.getTaskExecution() == null) {
            // Dynamic filters are only available during task execution.
            // Dynamic filters are collected by same tasks that run corresponding
            // join operators. When task (and implicitly join operator) is done it
            // means that all potential consumers (that belong to probe side of join)
            // of dynamic filters are either finished or cancelled. Therefore dynamic
            // filters are no longer required.
            return INITIAL_DYNAMIC_FILTER_DOMAINS;
        }

        return taskHolder.getTaskExecution().getTaskContext().acknowledgeAndGetNewDynamicFilterDomains(callersDynamicFiltersVersion);
    }

    private synchronized void notifyStatusChanged()
    {
        taskStatusVersion.incrementAndGet();
        taskStatusVersionChange.complete(null, taskNotificationExecutor);
    }

    /**
     * Gets an object with task metadata.
     * <p>
     * Checks if the holder contains any executing task and will
     * loads the information from the previous task. If the holder is empty
     * it will create a task with empty parameters.
     *
     * @param taskHolder an object that holds a task
     * @return an object with the task metadata
     */
    private TaskStatus createTaskStatus(TaskHolder taskHolder)
    {
        // Obtain task status version before building actual TaskStatus object.
        // This way any task updates won't be lost since all updates happen
        // before version number is increased.
        long versionNumber = taskStatusVersion.get();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == FAILED) {
            failures = toFailures(taskStateMachine.getFailureCauses());
        }

        int queuedPartitionedDrivers = 0;
        int runningPartitionedDrivers = 0;
        DataSize physicalWrittenDataSize = DataSize.ofBytes(0);
        DataSize userMemoryReservation = DataSize.ofBytes(0);
        DataSize systemMemoryReservation = DataSize.ofBytes(0);
        DataSize revocableMemoryReservation = DataSize.ofBytes(0);
        // TODO: add a mechanism to avoid sending the whole completedDriverGroups set over the wire for every task status reply
        Set<Lifespan> completedDriverGroups = ImmutableSet.of();
        long fullGcCount = 0;
        Duration fullGcTime = new Duration(0, MILLISECONDS);
        long dynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
        if (taskHolder.getFinalTaskInfo() != null) {
            TaskInfo taskInfo = taskHolder.getFinalTaskInfo();
            TaskStats taskStats = taskInfo.getStats();
            queuedPartitionedDrivers = taskStats.getQueuedPartitionedDrivers();
            runningPartitionedDrivers = taskStats.getRunningPartitionedDrivers();
            physicalWrittenDataSize = taskStats.getPhysicalWrittenDataSize();
            userMemoryReservation = taskStats.getUserMemoryReservation();
            systemMemoryReservation = taskStats.getSystemMemoryReservation();
            revocableMemoryReservation = taskStats.getRevocableMemoryReservation();
            fullGcCount = taskStats.getFullGcCount();
            fullGcTime = taskStats.getFullGcTime();
        }
        else if (taskHolder.getTaskExecution() != null) {
            long physicalWrittenBytes = 0;
            TaskContext taskContext = taskHolder.getTaskExecution().getTaskContext();
            for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                PipelineStatus pipelineStatus = pipelineContext.getPipelineStatus();
                queuedPartitionedDrivers += pipelineStatus.getQueuedPartitionedDrivers();
                runningPartitionedDrivers += pipelineStatus.getRunningPartitionedDrivers();
                physicalWrittenBytes += pipelineContext.getPhysicalWrittenDataSize();
            }
            physicalWrittenDataSize = succinctBytes(physicalWrittenBytes);
            userMemoryReservation = taskContext.getMemoryReservation();
            systemMemoryReservation = taskContext.getSystemMemoryReservation();
            revocableMemoryReservation = taskContext.getRevocableMemoryReservation();
            completedDriverGroups = taskContext.getCompletedDriverGroups();
            fullGcCount = taskContext.getFullGcCount();
            fullGcTime = taskContext.getFullGcTime();
            dynamicFiltersVersion = taskContext.getDynamicFiltersVersion();
        }

        return new TaskStatus(taskStateMachine.getTaskId(),
                taskInstanceId,
                versionNumber,
                state,
                location,
                nodeId,
                completedDriverGroups,
                failures,
                queuedPartitionedDrivers,
                runningPartitionedDrivers,
                isOutputBufferOverutilized(),
                physicalWrittenDataSize,
                userMemoryReservation,
                systemMemoryReservation,
                revocableMemoryReservation,
                fullGcCount,
                fullGcTime,
                dynamicFiltersVersion);
    }

    private TaskStats getTaskStats(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getStats();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getTaskContext().getTaskStats();
        }
        // if the task completed without creation, set end time
        DateTime endTime = taskStateMachine.getState().isDone() ? DateTime.now() : null;
        return new TaskStats(taskStateMachine.getCreatedTime(), endTime);
    }

    private static Set<PlanNodeId> getNoMoreSplits(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getNoMoreSplits();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getNoMoreSplits();
        }
        return ImmutableSet.of();
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder)
    {
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<PlanNodeId> noMoreSplits = getNoMoreSplits(taskHolder);

        TaskStatus taskStatus = createTaskStatus(taskHolder);
        return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get());
    }

    public synchronized ListenableFuture<TaskStatus> getTaskStatus(long callersCurrentVersion)
    {
        if (callersCurrentVersion < taskStatusVersion.get() || taskHolderReference.get().isFinished()) {
            // return immediately if caller has older task status version or final task info is available
            return immediateFuture(getTaskStatus());
        }

        // At this point taskHolderReference.get().isFinished() might become true. However notifyStatusChanged()
        // is synchronized therefore notification for new listener won't be lost.
        return Futures.transform(taskStatusVersionChange.createNewListener(), input -> getTaskStatus(), directExecutor());
    }

    public TaskInfo updateTask(Session session, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions)
    {
        try {
            // The LazyOutput buffer does not support write methods, so the actual
            // output buffer must be established before drivers are created (e.g.
            // a VALUES query).
            outputBuffer.setOutputBuffers(outputBuffers);

            // assure the task execution is only created once
            SqlTaskExecution taskExecution;
            synchronized (this) {
                // is task already complete?
                TaskHolder taskHolder = taskHolderReference.get();
                if (taskHolder.isFinished()) {
                    return taskHolder.getFinalTaskInfo();
                }
                taskExecution = taskHolder.getTaskExecution();
                if (taskExecution == null) {
                    checkState(fragment.isPresent(), "fragment must be present");
                    taskExecution = sqlTaskExecutionFactory.create(
                            session,
                            queryContext,
                            taskStateMachine,
                            outputBuffer,
                            fragment.get(),
                            sources,
                            this::notifyStatusChanged,
                            totalPartitions);
                    taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                    needsPlan.set(false);
                }
            }

            if (taskExecution != null) {
                taskExecution.addSources(sources);
            }
        }
        catch (Error e) {
            failed(e);
            throw e;
        }
        catch (RuntimeException e) {
            failed(e);
        }

        return getTaskInfo();
    }

    /**
     * Retrieves the list with the task response buffers.
     *
     * @param bufferId the id of the buffer that contains the response data
     * @param startingSequenceId the user request identifier
     * @param maxSize the max size of the returned data
     * @return a list with the response's buffers
     */
    public ListenableFuture<BufferResult> getTaskResults(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return outputBuffer.get(bufferId, startingSequenceId, maxSize);
    }

    public void acknowledgeTaskResults(OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(bufferId, "bufferId is null");

        outputBuffer.acknowledge(bufferId, sequenceId);
    }

    public TaskInfo abortTaskResults(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        log.debug("Aborting task %s output %s", taskId, bufferId);
        outputBuffer.abort(bufferId);

        return getTaskInfo();
    }

    public void failed(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        taskStateMachine.failed(cause);
    }

    public TaskInfo cancel()
    {
        taskStateMachine.cancel();
        return getTaskInfo();
    }

    public TaskInfo abort()
    {
        taskStateMachine.abort();
        return getTaskInfo();
    }

    @Override
    public String toString()
    {
        return taskId.toString();
    }

    private static final class TaskHolder
    {
        private final SqlTaskExecution taskExecution;
        private final TaskInfo finalTaskInfo;
        private final SqlTaskIoStats finalIoStats;

        private TaskHolder()
        {
            this.taskExecution = null;
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(SqlTaskExecution taskExecution)
        {
            this.taskExecution = requireNonNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats)
        {
            this.taskExecution = null;
            this.finalTaskInfo = requireNonNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = requireNonNull(finalIoStats, "finalIoStats is null");
        }

        public boolean isFinished()
        {
            return finalTaskInfo != null;
        }

        /**
         * Gets the metadata about the executed task.
         * <p>
         * The metadata contains:
         * - The {@link io.prestosql.operator.Driver} set that compounds
         * the task.
         * - The buffer that stores the output result.
         * - The metadata about the {@link io.prestosql.metadata.Split} where
         * query is executed.
         *
         * @return the metadata about the executed task.
         */
        @Nullable
        public SqlTaskExecution getTaskExecution()
        {
            return taskExecution;
        }

        /**
         * Gets the metadata about the running task.
         * <p>
         * The metadata contains information about:
         * - The processed plan tree nodes
         * - The used buffers
         * - Information about memory usage
         *
         * @return the metadata about the task
         */
        @Nullable
        public TaskInfo getFinalTaskInfo()
        {
            return finalTaskInfo;
        }

        public SqlTaskIoStats getIoStats()
        {
            // if we are finished, return the final IoStats
            if (finalIoStats != null) {
                return finalIoStats;
            }
            // if we haven't started yet, return an empty IoStats
            if (taskExecution == null) {
                return new SqlTaskIoStats();
            }
            // get IoStats from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return new SqlTaskIoStats(taskContext.getProcessedInputDataSize(), taskContext.getInputPositions(), taskContext.getOutputDataSize(), taskContext.getOutputPositions());
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }
}
