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

//import br.com.s1mbi0se.shannondb.SDBInstanceSettings;
//import br.com.s1mbi0se.shannondb.server.SDBConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import io.airlift.log.Logger;

import java.util.Map;

@Aspect
public abstract class BaseAspect
{
    private static final Logger log = Logger.get(BaseAspect.class);

//    protected static final int NODE_ID = SDBConfig.getIntValue("S1S_NODE_ID", 0);
//    protected static final String DATACENTER_NAME = SDBInstanceSettings.get().getDataCenterName();
//    protected static final String NODE_HOSTNAME = getNodeHostname();
//
//    private static String getNodeHostname() {
//        final String[] shannondbHosts = SDBConfig.getValues("shannondb.nodes");
//
//        if (shannondbHosts == null || shannondbHosts.length <= NODE_ID) {
//            return "localhost";
//        }
//
//        final String completePath = shannondbHosts[NODE_ID];
//
//        final int separatorIndex = completePath.lastIndexOf(':');
//        // this class get host and port to send message from properties file
//        return separatorIndex > 0 ? completePath.substring(0, separatorIndex) : completePath;
//    }

    protected Object printDebugLogForMethod(final ProceedingJoinPoint point, final long threadId)
            throws Throwable
    {
        final long start = System.currentTimeMillis();

        Object result = null;

        try {
            result = point.proceed();
        }
        catch (Throwable throwable) {
            if (throwable instanceof Exception) {
                final Class<? extends Throwable> exceptionClass = throwable.getClass();

                throw exceptionClass.cast(throwable);
            }
            throw throwable;
        }
        final long timeToExecuteMethod = System.currentTimeMillis() - start;

        final Integer debugStepId = getThreadIdToStep().get(threadId);
        final Long debugLogId = getThreadIdToDebugLogId().get(threadId);
        System.out.println("Teste");
        if (debugLogId != null && debugStepId != null) {
            final MethodSignature methodAttached = (MethodSignature) point.getSignature();
            final String methodName = methodAttached.getMethod().getName();

            final String debugLogMessage =
                    String.format(
                            "%s - %d - %d - %d - %s - %s  - %s - %s",
                            getFlowName(),
                            debugLogId,
                            debugStepId,
                            timeToExecuteMethod,
                            methodName,
                            "",
                            "",
                            "");

            System.out.println(debugLogMessage);

            getThreadIdToStep().put(threadId, debugStepId + 1);
        }

        return result;
    }

    protected abstract String getFlowName();

    protected abstract Map<Long, Integer> getThreadIdToStep();

    protected abstract Map<Long, Long> getThreadIdToDebugLogId();
}
