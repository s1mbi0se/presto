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
package io.prestosql.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session.ResourceEstimateBuilder;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GroupProvider;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.session.ResourceEstimates;
import io.prestosql.spi.session.metadata.QueryRequestMetadata;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.transaction.TransactionId;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_CAPABILITIES;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static io.prestosql.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static io.prestosql.client.PrestoHeaders.PRESTO_LANGUAGE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_QUERY_REQUEST_METADATA;
import static io.prestosql.client.PrestoHeaders.PRESTO_RESOURCE_ESTIMATE;
import static io.prestosql.client.PrestoHeaders.PRESTO_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TRACE_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class HttpRequestSessionContext
        implements SessionContext
{
    private static final Logger log = Logger.get(HttpRequestSessionContext.class);

    private static final Splitter DOT_SPLITTER = Splitter.on('.');
    public static final String AUTHENTICATED_IDENTITY = "presto.authenticated-identity";

    private final String catalog;
    private final String schema;
    private final String path;

    private final Optional<Identity> authenticatedIdentity;
    private final Identity identity;

    private final String source;
    private final Optional<String> traceToken;
    private final String userAgent;
    private final String remoteUserAddress;
    private final String timeZoneId;
    private final String language;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final ResourceEstimates resourceEstimates;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String clientInfo;

    private final JsonCodec<QueryRequestMetadata> jsonCodecQueryRequestMetadata = jsonCodec(QueryRequestMetadata.class);
    private final Optional<QueryRequestMetadata> queryRequestMetadata;

    public HttpRequestSessionContext(MultivaluedMap<String, String> headers, String remoteAddress, Optional<Identity> authenticatedIdentity, GroupProvider groupProvider)
            throws WebApplicationException
    {
        catalog = trimEmptyToNull(headers.getFirst(PRESTO_CATALOG));
        schema = trimEmptyToNull(headers.getFirst(PRESTO_SCHEMA));
        path = trimEmptyToNull(headers.getFirst(PRESTO_PATH));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        this.authenticatedIdentity = requireNonNull(authenticatedIdentity, "authenticatedIdentity is null");
        identity = buildSessionIdentity(authenticatedIdentity, headers, groupProvider);

        source = headers.getFirst(PRESTO_SOURCE);
        traceToken = Optional.ofNullable(trimEmptyToNull(headers.getFirst(PRESTO_TRACE_TOKEN)));
        userAgent = headers.getFirst(USER_AGENT);
        remoteUserAddress = remoteAddress;
        timeZoneId = headers.getFirst(PRESTO_TIME_ZONE);
        language = headers.getFirst(PRESTO_LANGUAGE);
        clientInfo = headers.getFirst(PRESTO_CLIENT_INFO);
        clientTags = parseClientTags(headers);
        clientCapabilities = parseClientCapabilities(headers);
        resourceEstimates = parseResourceEstimate(headers);
        queryRequestMetadata = parseQueryRequestMetadata(headers);

        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Entry<String, String> entry : parseSessionHeaders(headers).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties cannot be validated until the transaction has stated, so we delay system property validation also
                systemProperties.put(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), "Invalid %s header", PRESTO_SESSION);
                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties cannot be validated until the transaction has stated
                catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            }
            else {
                throw badRequest(format("Invalid %s header", PRESTO_SESSION));
            }
        }
        this.systemProperties = systemProperties.build();
        this.catalogSessionProperties = catalogSessionProperties.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));

        preparedStatements = parsePreparedStatementsHeaders(headers);

        String transactionIdHeader = headers.getFirst(PRESTO_TRANSACTION_ID);
        clientTransactionSupport = transactionIdHeader != null;
        transactionId = parseTransactionId(transactionIdHeader);
    }

    public static Identity extractAuthorizedIdentity(HttpServletRequest servletRequest, HttpHeaders httpHeaders, AccessControl accessControl, GroupProvider groupProvider)
    {
        return extractAuthorizedIdentity(
                Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY)),
                httpHeaders.getRequestHeaders(),
                accessControl,
                groupProvider);
    }

    public static Identity extractAuthorizedIdentity(Optional<Identity> optionalAuthenticatedIdentity, MultivaluedMap<String, String> headers, AccessControl accessControl, GroupProvider groupProvider)
            throws AccessDeniedException
    {
        Identity identity = buildSessionIdentity(optionalAuthenticatedIdentity, headers, groupProvider);

        accessControl.checkCanSetUser(identity.getPrincipal(), identity.getUser());

        // authenticated may not present for HTTP or if authentication is not setup
        optionalAuthenticatedIdentity.ifPresent(authenticatedIdentity -> {
            // only check impersonation if authenticated user is not the same as the explicitly set user
            if (!authenticatedIdentity.getUser().equals(identity.getUser())) {
                accessControl.checkCanImpersonateUser(authenticatedIdentity, identity.getUser());
            }
        });

        return identity;
    }

    /**
     * Extracts information from request headers about the user requester and his roles and credentials.
     * <p>
     * The headers used to retrieve this information are:
     * - X-Presto-User
     * - X-Presto-Extra-Credential
     * - X-Presto-Role
     *
     * @param authenticatedIdentity the authenticated user defined in the request
     * @param headers a map with all defined HTTP headers and respective values
     * @param groupProvider an object used to retrieves all groups for a user
     * @return an object with user's roles and credentials
     * @throws WebApplicationException if a user is not set for the request
     * @see SelectedRole
     */
    private static Identity buildSessionIdentity(Optional<Identity> authenticatedIdentity, MultivaluedMap<String, String> headers, GroupProvider groupProvider)
    {
        String prestoUser = trimEmptyToNull(headers.getFirst(PRESTO_USER));
        String user = prestoUser != null ? prestoUser : authenticatedIdentity.map(Identity::getUser).orElse(null);
        assertRequest(user != null, "User must be set");
        return authenticatedIdentity
                .map(identity -> Identity.from(identity).withUser(user))
                .orElseGet(() -> Identity.forUser(user))
                .withAdditionalRoles(parseRoleHeaders(headers))
                .withAdditionalExtraCredentials(parseExtraCredentials(headers))
                .withAdditionalGroups(groupProvider.getGroups(user))
                .build();
    }

    @Override
    public Optional<Identity> getAuthenticatedIdentity()
    {
        return authenticatedIdentity;
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public String getCatalog()
    {
        return catalog;
    }

    @Override
    public String getSchema()
    {
        return schema;
    }

    @Override
    public String getPath()
    {
        return path;
    }

    @Override
    public String getSource()
    {
        return source;
    }

    @Override
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Override
    public String getUserAgent()
    {
        return userAgent;
    }

    @Override
    public String getClientInfo()
    {
        return clientInfo;
    }

    @Override
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @Override
    public Set<String> getClientCapabilities()
    {
        return clientCapabilities;
    }

    @Override
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @Override
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @Override
    public String getLanguage()
    {
        return language;
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @Override
    public boolean supportClientTransaction()
    {
        return clientTransactionSupport;
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @Override
    public Optional<QueryRequestMetadata> getQueryRequestMetadata()
    {
        return queryRequestMetadata;
    }

    /**
     * Returns a list of properties for a given header name in HTTP header.
     * <p>
     * It is used for headers that contains a list of comma separated values. As an example:
     * - header in request: X-Generic-Header-Property: property1=value1, property2=value2
     * - method output: ['property1=value1' , 'property2=value2']
     *
     * @param headers a map of request headers to respective values
     * @param name the header property that needs to extract its value
     * @return a list of all values for an HTTP header property
     */
    private static List<String> splitHttpHeader(MultivaluedMap<String, String> headers, String name)
    {
        List<String> values = firstNonNull(headers.get(name), ImmutableList.of());
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return values.stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    /**
     * Extracts the specific header PRESTO_SESSION
     *
     * @param headers headers a MultivaluedMap containing all request headers.
     * @return a Map with the {@link PRESTO_SESSION} properties
     */
    private static Map<String, String> parseSessionHeaders(MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, PRESTO_SESSION);
    }

    /**
     * Checks if the received roles in X-Presto-Role header are valid.
     * <p>
     * The header contains a comma-separated list of connectors and respective roles for each one.
     * Example:
     * - X-Presto-Role: connector1=ALL,connector2=NONE,connector3=ROLE{role-name}
     *
     * @param headers a map with all passed HTTP headers and respective values
     * @return a map of connectors' names and their respective roles
     * @throws WebApplicationException if a type of role that does not exist is passed
     */
    private static Map<String, SelectedRole> parseRoleHeaders(MultivaluedMap<String, String> headers)
    {
        ImmutableMap.Builder<String, SelectedRole> roles = ImmutableMap.builder();
        parseProperty(headers, PRESTO_ROLE).forEach((key, value) -> {
            SelectedRole role;
            try {
                role = SelectedRole.valueOf(value);
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header", PRESTO_ROLE));
            }
            roles.put(key, role);
        });
        return roles.build();
    }

    /**
     * Extracts the values from X-Presto-Extra-Credential header.
     *
     * @param headers all request headers properties and respective values
     * @return a map containing each extra credential and its respective value
     */
    private static Map<String, String> parseExtraCredentials(MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, PRESTO_EXTRA_CREDENTIAL);
    }

    /**
     * Converts a header property value from a comma-separated list to a map
     * <p>
     * As an example:
     * - header in request: X-Generic-Header-Property: property1=value1, property2=value2
     * - method output: {property1: value1 , property2: value2}
     *
     * @param headers all request headers properties and respective values
     * @param headerName the header property that needs to extract its values
     * @return a map containing all values for an HTTP header property
     */
    private static Map<String, String> parseProperty(MultivaluedMap<String, String> headers, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitHttpHeader(headers, headerName)) {
            List<String> nameValue = Splitter.on('=').trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            try {
                properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", headerName, e));
            }
        }
        return properties;
    }

    /**
     * Extracts all tags defined on X-Presto-Client-Tags header.
     * <p>
     * The tags are defined as a comma-separated list of values and they are used to
     * identify resource groups.
     *
     * @param headers a map with all defined HTTP headers and respective values
     * @return the list of all passed tags
     */
    private static Set<String> parseClientTags(MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(PRESTO_CLIENT_TAGS))));
    }

    /**
     * Extracts the header PRESTO_CLIENT_CAPABILITIES.
     * <p>
     * Extract the header and add it to a Set.
     * Returns a Set with the extracted {@code PRESTO_CLIENT_CAPABILITIES} header.
     *
     * @param headers headers a MultivaluedMap containing all request headers.
     * @return a Set with {@code PRESTO_CLIENT_CAPABILITIES} headers.
     */
    private static Set<String> parseClientCapabilities(MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(PRESTO_CLIENT_CAPABILITIES))));
    }

    /**
     * Check which resource estimate will be started and extracts PRESTO_RESOURCE_ESTIMATE header.
     * <p>
     * Checks in switch case the name is to execute the EXECUTION_TIME, CPU_TIME or PEAK_MEMORY.
     * If the value is not valid, an IllegalArgumentException is generated an handled.
     *
     * @param headers headers a MultivaluedMap containing all request headers.
     * @return {@link ResourceEstimates} that estimates resource usage for a query.
     */
    private static ResourceEstimates parseResourceEstimate(MultivaluedMap<String, String> headers)
    {
        ResourceEstimateBuilder builder = new ResourceEstimateBuilder();
        parseProperty(headers, PRESTO_RESOURCE_ESTIMATE).forEach((name, value) -> {
            try {
                switch (name.toUpperCase()) {
                    case ResourceEstimates.EXECUTION_TIME:
                        builder.setExecutionTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.CPU_TIME:
                        builder.setCpuTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.PEAK_MEMORY:
                        builder.setPeakMemory(DataSize.valueOf(value));
                        break;
                    default:
                        throw badRequest(format("Unsupported resource name %s", name));
                }
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Unsupported format for resource estimate '%s': %s", value, e));
            }
        });

        return builder.build();
    }

    /**
     * Extracts the Presto_QUERY_REQUEST_METADATA header.
     * <p>
     * Checks whether serializedData is null. If so, an empty Optional is returned.
     * If not, put it in a pattern and put it in json format.
     *
     * @param headers headers a MultivaluedMap that takes a string as a key and another string as a value.
     * @return an Optional of type {@link QueryRequestMetadata}.
     */
    private Optional<QueryRequestMetadata> parseQueryRequestMetadata(MultivaluedMap<String, String> headers)
    {
        String serializedData = trimEmptyToNull(headers.getFirst(PRESTO_QUERY_REQUEST_METADATA));
        if (serializedData == null) {
            return Optional.empty();
        }

        String serialized = serializedData.replaceAll("^\"|\"$", "").replace("\\\"", "\"");

        QueryRequestMetadata queryRequestMetadata = jsonCodecQueryRequestMetadata.fromJson(serialized);

        return Optional.ofNullable(queryRequestMetadata);
    }

    /**
     * Asserts whether a given expression is true, throwing an exception otherwise.
     *
     * @param expression a generic boolean expression
     * @param format message that will be in exception if expression is false
     * @param args arguments used to construct the message that will be on exception
     * @throws WebApplicationException if the expression is false
     */
    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    /**
     * Extract the PRESTO_PREPARED_STATEMENT header and parse the sql command.
     * <p>
     * Check all headers and extract the PRESTO_PREPARED_STATEMENT header. Decodes the statementName.
     * Validates the sql command with sqlParser and returns a Map<String,String> with all occurrences.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value.
     * @return a Map<String,String> with the name and sql command as key/value.
     */
    private static Map<String, String> parsePreparedStatementsHeaders(MultivaluedMap<String, String> headers)
    {
        ImmutableMap.Builder<String, String> preparedStatements = ImmutableMap.builder();
        parseProperty(headers, PRESTO_PREPARED_STATEMENT).forEach((key, sqlString) -> {
            String statementName;
            try {
                statementName = urlDecode(key);
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage()));
            }

            // Validate statement
            SqlParser sqlParser = new SqlParser();
            try {
                sqlParser.createStatement(sqlString, new ParsingOptions(AS_DOUBLE /* anything */));
            }
            catch (ParsingException e) {
                throw badRequest(format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage()));
            }

            preparedStatements.put(statementName, sqlString);
        });

        return preparedStatements.build();
    }

    /**
     * Try to instantiate a new {@link TransactionId}
     * <p>
     * Checks whether the transactionId is null. if so, an empty option is returned.
     * If not, a new instance of the {@link TransactionId} is returned. For invalid values,
     * an WebApplicationException is thrown and handled.
     *
     * @param transactionId a string that represents the transaction id.
     * @return an Optional TransactionId
     */
    private static Optional<TransactionId> parseTransactionId(String transactionId)
    {
        transactionId = trimEmptyToNull(transactionId);
        if (transactionId == null || transactionId.equalsIgnoreCase("none")) {
            return Optional.empty();
        }
        try {
            return Optional.of(TransactionId.valueOf(transactionId));
        }
        catch (Exception e) {
            throw badRequest(e.getMessage());
        }
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(message, Response
                .status(Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }

    /**
     * Checks, trims, and returns a given string if it is neither null nor empty.
     *
     * @param value a given string value
     * @return the given string trimmed if it is neither null nor empty, null otherwise
     * @see io.prestosql.client.PrestoHeaders
     */
    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
