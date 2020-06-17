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
package io.prestosql.plugin.logs;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
/**
 * This class is responsible to execute all debug logs inside ColumnCreationFlux.
 *
 * <p>For each method that contains StartCreateSchemaFlowLoggable, FinishCreateSchemaFlowLoggable,
 * or CreateSchemaFlowLoggable this class uses the {@link
 * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} to writes inside the log file, a
 * set of important information about methods execution.
 */

@Aspect
public class SchemaCreationFlowLogger
        extends BaseAspect
{
    private static final String FLOW_NAME = "CreateSchemaFlow";

    protected final Map<Long, Integer> threadIdToStep = new ConcurrentHashMap<>();
    protected final Map<Long, Long> threadIdToDebugLogId = new ConcurrentHashMap<>();

    /**
     * This method is executed when the Thread starts execution of creation schema command inside
     * worker.
     *
     * <p>It is necessary to create the step and debug log id variable for each thread that will
     * execute the flux. So it will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *     obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(
            "execution(* *(..)) && "
                    + "@annotation(io.prestosql.plugin.annotations.StartCreateSchemaFlowLoggable)")
    public Object startFlux(final ProceedingJoinPoint point) throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * This method just retrieves the thread id and delegate the processing to method inside {@link
     * BaseAspect}
     *
     * <p>It will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *     obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(
            "execution(* *(..)) &&"
                    + " @annotation(io.prestosql.plugin.annotations.CreateSchemaFlowLoggable)")
    public Object around(final ProceedingJoinPoint point) throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * This method is executed when the Thread starts execution when finishes the command of create
     * schema inside the worker.
     *
     * <p>It is necessary to remove the step and debug log id variable for each thread that already
     * executed the flux. It will first to delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed than it will reset information
     * about step and debug log id inside maps.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *     obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(
            "execution(* *(..)) && "
                    + "@annotation(io.prestosql.plugin.annotations.FinishCreateSchemaFlowLoggable)")
    public Object finishFlux(final ProceedingJoinPoint point) throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        final Object resultFromMethod = printDebugLogForMethod(point, threadId);

        threadIdToStep.remove(threadId);
        threadIdToDebugLogId.remove(threadId);

        return resultFromMethod;
    }

    @Around(
            "execution(* *(..)) &&"
                    + " @annotation(io.prestosql.plugin.annotations.GenericFlowLoggable)")
    public Object generic(final ProceedingJoinPoint point) throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        final Object resultFromMethod = printDebugLogForMethod(point, threadId);

        threadIdToStep.remove(threadId);
        threadIdToDebugLogId.remove(threadId);

        return resultFromMethod;
    }

    /**
     * Defines a name for a general flux inside ShannonDB
     *
     * <p>The flow name is used inside logs because there are methods that are used in more than one
     * flux inside the code, so it is necessary to know the specific flux that call it.
     *
     * @return the name of a general flux inside ShannonDB.
     */
    @Override
    protected String getFlowName()
    {
        return FLOW_NAME;
    }

    /**
     * Defines a map of a thread to a step(or sequence) for a general flux inside ShannonDB
     *
     * <p>The step is a number that shows the sequence that methods are executed inside a flux.
     *
     * @return a map with a thread and and the order that a method is executed inside a flux.
     */
    @Override
    protected Map<Long, Integer> getThreadIdToStep()
    {
        return this.threadIdToStep;
    }

    /**
     * Defines a map of a thread to an identifier of flux's execution
     *
     * <p>Each time that a Thread execute a flux, a new debug log id is created and represents that
     * execution cycle.
     *
     * @return .
     */
    @Override
    protected Map<Long, Long> getThreadIdToDebugLogId()
    {
        return this.threadIdToDebugLogId;
    }
}
