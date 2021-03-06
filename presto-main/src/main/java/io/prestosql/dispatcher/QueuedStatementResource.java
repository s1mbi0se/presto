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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryResults;
import io.prestosql.client.StatementStats;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.server.ServerConfig;
import io.prestosql.server.SessionContext;
import io.prestosql.server.protocol.Slug;
import io.prestosql.server.security.ResourceSecurity;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.GroupProvider;
import io.prestosql.spi.security.Identity;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static io.prestosql.server.protocol.Slug.Context.EXECUTING_QUERY;
import static io.prestosql.server.protocol.Slug.Context.QUEUED_QUERY;
import static io.prestosql.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/statement")
public class QueuedStatementResource
{
    private static final Logger log = Logger.get(QueuedStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private final GroupProvider groupProvider;
    private final DispatchManager dispatchManager;

    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("dispatch-query-purger"));
    private final boolean compressionEnabled;

    @Inject
    public QueuedStatementResource(
            GroupProvider groupProvider,
            DispatchManager dispatchManager,
            DispatchExecutor executor,
            ServerConfig serverConfig)
    {
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");

        requireNonNull(dispatchManager, "dispatchManager is null");
        this.responseExecutor = requireNonNull(executor, "responseExecutor is null").getExecutor();
        this.timeoutExecutor = requireNonNull(executor, "timeoutExecutor is null").getScheduledExecutor();
        this.compressionEnabled = requireNonNull(serverConfig, "serverConfig is null").isQueryResultsCompressionEnabled();

        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        // snapshot the queries before checking states to avoid registration race
                        for (Entry<QueryId, Query> entry : ImmutableSet.copyOf(queries.entrySet())) {
                            if (!entry.getValue().isSubmissionFinished()) {
                                continue;
                            }

                            // forget about this query if the query manager is no longer tracking it
                            if (!dispatchManager.isQueryRegistered(entry.getKey())) {
                                Query query = queries.remove(entry.getKey());
                                if (query != null) {
                                    try {
                                        query.destroy();
                                    }
                                    catch (Throwable e) {
                                        // this catch clause is broad so query purger does not get stuck
                                        log.warn(e, "Error destroying identity");
                                    }
                                }
                            }
                        }
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error removing old queries");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    /**
     * Receives POST requests that try to create new queries to be executed on the server.
     *
     * @param statement a SQL command
     * @param servletRequest an object that provides information about the HTTP request
     * @param httpHeaders an object containing information about the request headers
     * @param uriInfo an object containing metadata about the URI of the endpoint
     * @return the result of the processed query
     */
    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        String remoteAddress = servletRequest.getRemoteAddr();
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        SessionContext sessionContext = new HttpRequestSessionContext(headers, remoteAddress, identity, groupProvider);
        Query query = new Query(statement, sessionContext, dispatchManager);
        queries.put(query.getQueryId(), query);

        // let authentication filter know that identity lifecycle has been handed off
        servletRequest.setAttribute(AUTHENTICATED_IDENTITY, null);

        return createQueryResultsResponse(query.getQueryResults(query.getLastToken(), uriInfo), compressionEnabled);
    }

    /**
     * Receives GET HTTP requests used to retrieve the query's execution status.
     *
     * @param queryId the query's identifier
     * @param slug a identifier for the resource(query) and its state
     * @param token a generated token that identifies the user's request used to obtain the server response
     * @param maxWait the time limit to wait for a server response
     * @param uriInfo an object containing metadata about the endpoint URI
     * @param asyncResponse an object used to build the request response asynchronously
     */
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);

        // wait for query to be dispatched, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                query.waitForDispatched(),
                () -> null,
                WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait),
                timeoutExecutor);

        // when state changes, fetch the next result
        ListenableFuture<QueryResults> queryResultsFuture = Futures.transform(
                futureStateChange,
                ignored -> query.getQueryResults(token, uriInfo),
                responseExecutor);

        // transform to Response
        ListenableFuture<Response> response = Futures.transform(
                queryResultsFuture,
                queryResults -> createQueryResultsResponse(queryResults, compressionEnabled),
                directExecutor());
        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        getQuery(queryId, slug, token)
                .cancel();
        return Response.noContent().build();
    }

    /**
     * Gets a queued query by its id, slug and a generated token.
     *
     * @param queryId the query's identifier
     * @param slug a resource identifier made using the token and query state
     * @param token a generated token that identifies the user's request used to obtain the server response
     * @return an object that contains metadata about the query
     * @throws WebApplicationException if the token is invalid or query does not exists
     */
    private Query getQuery(QueryId queryId, String slug, long token)
    {
        Query query = queries.get(queryId);
        if (query == null || !query.getSlug().isValid(QUEUED_QUERY, slug, token)) {
            throw badRequest(NOT_FOUND, "Query not found");
        }
        return query;
    }

    private static Response createQueryResultsResponse(QueryResults results, boolean compressionEnabled)
    {
        Response.ResponseBuilder builder = Response.ok(results);
        if (!compressionEnabled) {
            builder.encoding("identity");
        }
        return builder.build();
    }

    /**
     * Builds the path where query will be available through Presto's web interface.
     *
     * @param queryId an object containing the query id
     * @param uriInfo an object containing the uri metadata
     * @return an URI with this format: ui/query.html?queryId
     */
    private static URI getQueryHtmlUri(QueryId queryId, UriInfo uriInfo)
    {
        return uriInfo.getRequestUriBuilder()
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();
    }

    /**
     * Builds the URI to retrieve information about a queued query.
     *
     * @param queryId an object containing the query identifier
     * @param slug a resource identifier made using the token and query state
     * @param token a generated token that identifies the user's request used to obtain the
     * response from server
     * @param uriInfo the object used to build the URI
     * @return the URI used to retrieve information about a queued query
     */
    private static URI getQueuedUri(QueryId queryId, Slug slug, long token, UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder()
                .replacePath("/v1/statement/queued/")
                .path(queryId.toString())
                .path(slug.makeSlug(QUEUED_QUERY, token))
                .path(String.valueOf(token))
                .replaceQuery("")
                .build();
    }

    /**
     * Instantiates the object that holds all metadata about a query result.
     *
     * @param queryId an object containing the query identifier
     * @param nextUri an object that contains the URI where information about query can be retrieved
     * @param queryError an object responsible for reporting the error that happened during the
     * query's execution
     * @param uriInfo an object that will be used to construct the query representation in web interface
     * @param elapsedTime the time elapsed until the query obtains its response
     * @param queuedTime the time the query waited to be executed
     * @return an object with all query's metadata
     */
    private static QueryResults createQueryResults(QueryId queryId, URI nextUri, Optional<QueryError> queryError, UriInfo uriInfo, Duration elapsedTime, Duration queuedTime)
    {
        QueryState state = queryError.map(error -> FAILED).orElse(QUEUED);
        return new QueryResults(
                queryId.toString(),
                getQueryHtmlUri(queryId, uriInfo),
                null,
                nextUri,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(state == QUEUED)
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static final class Query
    {
        private final String query;
        private final SessionContext sessionContext;
        private final DispatchManager dispatchManager;
        private final QueryId queryId;
        private final Slug slug = Slug.createNew();
        private final AtomicLong lastToken = new AtomicLong();

        @GuardedBy("this")
        private ListenableFuture<?> querySubmissionFuture;

        public Query(String query, SessionContext sessionContext, DispatchManager dispatchManager)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.queryId = dispatchManager.createQueryId();
        }

        /**
         * Gets the query's identifier
         *
         * @return the query's identifier
         */
        public QueryId getQueryId()
        {
            return queryId;
        }

        /**
         * Gets query's slug.
         * <p>
         * Is used to identify a resource(request) and its state.
         *
         * @return the query's slug.
         */
        public Slug getSlug()
        {
            return slug;
        }

        /**
         * Gets the generated token that identifies the user's request.
         *
         * @return the generated token that identifies the user's request
         */
        public long getLastToken()
        {
            return lastToken.get();
        }

        public synchronized boolean isSubmissionFinished()
        {
            return querySubmissionFuture != null && querySubmissionFuture.isDone();
        }

        /**
         * Waits the query being submitted or complete the execution.
         * <p>
         * Checks if the query was already submitted, if not it will wait for this process to continue,
         * otherwise it will wait until the query is processed.
         *
         * @return a future that encapsulates the task of submitting the query
         * or execute the query
         */
        private ListenableFuture<?> waitForDispatched()
        {
            // if query query submission has not finished, wait for it to finish
            synchronized (this) {
                if (querySubmissionFuture == null) {
                    querySubmissionFuture = dispatchManager.createQuery(queryId, slug, sessionContext, query);
                }
                if (!querySubmissionFuture.isDone()) {
                    return querySubmissionFuture;
                }
            }

            // otherwise, wait for the query to finish
            return dispatchManager.waitForDispatched(queryId);
        }

        /**
         * Retrieves an object that contains the result of a dispatched query.
         *
         * @param token a generated token that identifies the user's request used to obtain the server response
         * @param uriInfo an object used to obtain the URI where the query can be retrieved from the server or construct the query representation inside
         * the web interface
         * @return an object with all query's metadata
         * @throws WebApplicationException if the token is invalid or program could not find information about a dispatched query
         */
        public QueryResults getQueryResults(long token, UriInfo uriInfo)
        {
            long lastToken = this.lastToken.get();
            // token should be the last token or the next token
            if (token != lastToken && token != lastToken + 1) {
                throw new WebApplicationException(Response.Status.GONE);
            }
            // advance (or stay at) the token
            this.lastToken.compareAndSet(lastToken, token);

            synchronized (this) {
                // if query submission has not finished, return simple empty result
                if (querySubmissionFuture == null || !querySubmissionFuture.isDone()) {
                    return createQueryResults(
                            token + 1,
                            uriInfo,
                            DispatchInfo.queued(NO_DURATION, NO_DURATION));
                }
            }

            Optional<DispatchInfo> dispatchInfo = dispatchManager.getDispatchInfo(queryId);
            if (dispatchInfo.isEmpty()) {
                // query should always be found, but it may have just been determined to be abandoned
                throw new WebApplicationException(Response
                        .status(NOT_FOUND)
                        .build());
            }

            return createQueryResults(token + 1, uriInfo, dispatchInfo.get());
        }

        public synchronized void cancel()
        {
            querySubmissionFuture.addListener(() -> dispatchManager.cancelQuery(queryId), directExecutor());
        }

        public void destroy()
        {
            sessionContext.getIdentity().destroy();
        }

        /**
         * Instantiates the object that holds all metadata about query results.
         *
         * @param token a generated token that identifies the client's request used to obtains the response from server
         * @param uriInfo an object used to obtain the URI where the query can be retrieved from server or construct the
         * query representation inside the web interface
         * @param dispatchInfo an object responsible for taking the coordinator's location, response time and waiting time
         * @return an object with all query's metadata
         */
        private QueryResults createQueryResults(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            URI nextUri = getNextUri(token, uriInfo, dispatchInfo);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return QueuedStatementResource.createQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    uriInfo,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime());
        }

        /**
         * Gets the URI where a query's process result can be found inside the server if it is not completed.
         *
         * @param token a generated token that identifies the client's request used to obtain the response from server
         * @param uriInfo an object used to obtain the URI where the query can be retrieved from the server
         * @param dispatchInfo an object responsible for taking the coordinator's location, response time and waiting time
         * @return the URI where information about the query can be found inside the server if it is not completed.
         */
        private URI getNextUri(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                return null;
            }
            // if dispatched, redirect to new uri
            return dispatchInfo.getCoordinatorLocation()
                    .map(coordinatorLocation -> getRedirectUri(coordinatorLocation, uriInfo))
                    .orElseGet(() -> getQueuedUri(queryId, slug, token, uriInfo));
        }

        private URI getRedirectUri(CoordinatorLocation coordinatorLocation, UriInfo uriInfo)
        {
            URI coordinatorUri = coordinatorLocation.getUri(uriInfo);
            return UriBuilder.fromUri(coordinatorUri)
                    .replacePath("/v1/statement/executing")
                    .path(queryId.toString())
                    .path(slug.makeSlug(EXECUTING_QUERY, 0))
                    .path("0")
                    .build();
        }

        private QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
        {
            ErrorCode errorCode;
            if (executionFailureInfo.getErrorCode() != null) {
                errorCode = executionFailureInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryId);
            }

            return new QueryError(
                    firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }
}
