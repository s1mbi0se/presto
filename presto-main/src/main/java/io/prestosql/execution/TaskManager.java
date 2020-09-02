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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.MemoryPoolAssignmentsRequest;
import io.prestosql.sql.planner.PlanFragment;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public interface TaskManager
{
    /**
     * Gets all of the currently tracked tasks.  This will included
     * uninitialized, running, and completed tasks.
     */
    List<TaskInfo> getAllTaskInfo();

    /**
     * Gets the info for the specified task.  If the task has not been created
     * yet, an uninitialized task is created and the info is returned.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    TaskInfo getTaskInfo(TaskId taskId);

    /**
     * Gets the status for the specified task.
     */
    TaskStatus getTaskStatus(TaskId taskId);

    /**
     * Gets future info for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the info is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState);

    /**
     * Gets the unique instance id of a task.  This can be used to detect a task
     * that was destroyed and recreated.
     */
    String getTaskInstanceId(TaskId taskId);

    /**
     * Gets future status for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the status is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState);

    /**
     * Changes the {@link io.prestosql.memory.MemoryPool} which a query must be assigned.
     * <p>
     * Presto has two memory pools:
     * - General pool: It is used by default.
     * - Reserved pool: It is used to run the heaviest query when
     * general pool is full.
     *
     * @param assignments a list of queries and the memory pool they must use
     */
    void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments);

    /**
     * Updates the task plan, sources and output buffers.  If the task does not
     * already exist, is is created and then updated.
     */
    TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions);

    /**
     * Cancels a task.  If the task does not already exist, is is created and then
     * canceled.
     */
    TaskInfo cancelTask(TaskId taskId);

    /**
     * Aborts a task.  If the task does not already exist, is is created and then
     * aborted.
     */
    TaskInfo abortTask(TaskId taskId);

    /**
     * Gets results from a task either immediately or in the future.  If the
     * task or buffer has not been created yet, an uninitialized task is
     * created and a future is returned.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize);

    /**
     * Acknowledges previously received results.
     */
    void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId);

    /**
     * Aborts a result buffer for a task.  If the task or buffer has not been
     * created yet, an uninitialized task is created and a the buffer is
     * aborted.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    TaskInfo abortTaskResults(TaskId taskId, OutputBufferId bufferId);

    /**
     * Adds a state change listener to the specified task.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener);
}
