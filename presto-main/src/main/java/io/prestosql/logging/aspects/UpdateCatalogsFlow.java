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
package io.prestosql.logging.aspects;

import br.com.simbiose.debug_log.BaseAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Aspect
public class UpdateCatalogsFlow
        extends BaseAspect
{
    private static final String FLOW_NAME = "UpdateCatalogsFlow";

    private static final String START_METHOD =
            "execution(* io.prestosql.metadata.DynamicCatalogStore"
                    + ".updateCatalogDelta(..))";

    private static final String WHITE_AND_BLACK_LIST =
            "execution(* io.prestosql.metadata..*(..)) && " +
                    "!within(io.prestosql.logging.aspects..*) && " +
                    "!within(io.airlift..*)";

    private static final String FINISH_METHOD =
            "execution(* io.prestosql.metadata.DynamicCatalogStore"
                    + ".finishUpdateCatalogDelta(..))";

    protected final Map<Long, Integer> threadIdToStep = new ConcurrentHashMap<>();
    protected final Map<Long, Long> threadIdToDebugLogId = new ConcurrentHashMap<>();

    /**
     * Executed when the Thread starts execution of update catalogs method.
     *
     * <p>It is necessary to create the step and debug log id variable for each thread that will
     * execute the flow. So it will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point is an object of Aspectj library that represents the method Presto needs to
     * obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(START_METHOD)
    public Object startFlow(final ProceedingJoinPoint point)
            throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * Retrieves the thread id and delegate the processing to super class.
     *
     * <p>It will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point an object o Aspectj library that represents the method Presto needs to
     * obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(WHITE_AND_BLACK_LIST)
    public Object around(final ProceedingJoinPoint point)
            throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * Executed when the Thread finishes execution of the command to update catalogs.
     *
     * <p>It is necessary to remove the step and debug log id variable for each thread that already
     * executed the flow. It will first delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log, information about method execution and it will reset information
     * about step and debug log id inside maps.
     *
     * @param point an object of Aspectj library that represents the method Presto needs to
     * obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(FINISH_METHOD)
    public Object finishFlow(final ProceedingJoinPoint point)
            throws Throwable
    {
        final long threadId = Thread.currentThread().getId();

        final Object resultFromMethod = printDebugLogForMethod(point, threadId);

        threadIdToStep.remove(threadId);
        threadIdToDebugLogId.remove(threadId);

        return resultFromMethod;
    }

    /**
     * Defines a name for a general flow inside Presto
     *
     * <p>The flow name is used inside logs because there are methods that are used in more than one
     * flow inside the code, so it is necessary to know the specific flow that call it.
     *
     * @return the name of a general flow inside Presto.
     */
    @Override
    protected String getFlowName()
    {
        return FLOW_NAME;
    }

    /**
     * Defines a map of a thread to a step(or sequence) for a general flow inside Presto
     *
     * <p>The step is a number that shows the sequence that methods are executed inside a flow.
     *
     * @return a map with a thread and and the order that a method is executed inside a flow.
     */
    @Override
    protected Map<Long, Integer> getThreadIdToStep()
    {
        return this.threadIdToStep;
    }

    /**
     * Defines a map of a thread to an identifier of flow's execution
     *
     * <p>Each time that a Thread execute a flow, a new debug log id is created and represents that
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
