package main.java.io.prestosql.plugin.logs;

//import br.com.s1mbi0se.shannondb.SDBInstanceSettings;
//import br.com.s1mbi0se.shannondb.server.SDBConfig;
import java.util.Map;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public abstract class BaseAspect {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseAspect.class);

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
            throws Throwable {
        final long start = System.currentTimeMillis();

        Object result = null;

        try {
            result = point.proceed();
        } catch (Throwable throwable) {
            if (throwable instanceof Exception) {
                final Class<? extends Throwable> exceptionClass = throwable.getClass();

                throw exceptionClass.cast(throwable);
            }

            throw throwable;
        }
        final long timeToExecuteMethod = System.currentTimeMillis() - start;

        final Integer debugStepId = getThreadIdToStep().get(threadId);
        final Long debugLogId = getThreadIdToDebugLogId().get(threadId);

        if (debugLogId != null && debugStepId != null && LOGGER.isDebugEnabled()) {
            final MethodSignature methodAttached = (MethodSignature) point.getSignature();
            final String methodName = methodAttached.getMethod().getName();

            final String debugLogMessage =
                    String.format(
                            "%s - %d - %d - %d - %s - %s  - %s - %s",
                            getFlowName(),
                            debugLogId,
                            debugStepId,
                            timeToExecuteMethod,
                            methodName
//                            DATACENTER_NAME,
//                            NODE_ID,
//                            NODE_HOSTNAME
                    );

            LOGGER.debug(debugLogMessage);

            getThreadIdToStep().put(threadId, debugStepId + 1);
        }

        return result;
    }

    protected abstract String getFlowName();

    protected abstract Map<Long, Integer> getThreadIdToStep();

    protected abstract Map<Long, Long> getThreadIdToDebugLogId();
}
