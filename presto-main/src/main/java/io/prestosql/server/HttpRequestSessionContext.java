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
     * Creates a session identity.
     * <p>
     * Checks if the user is non-null, add header, add extraCredentials and add groups to build a session identity.
     *
     * @param authenticatedIdentity an Optional of {@link Identity}
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     * @param groupProvider a {@link GroupProvider} interface implementation
     *
     * @return an identity to a session
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
     * Extracts a header from a headers map in the for of List<String>. Breaks each value with a comma, removes spaces and omits empty string.
     * Returns a flat (non-nested) list of resulting values ​​or an empty list
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     * @param name a string that represents PrestoHeader's name
     *
     * @return a list of all HttpHeaders and their names
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
     * Extracts a specific header PRESTO_SESSION
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     *
     * @return a Map with the {@link PRESTO_SESSION} properties
     */
    private static Map<String, String> parseSessionHeaders(MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, PRESTO_SESSION);
    }

    /**
     * Analyzes whether the {@code PRESTO_ROLE} header meets the standards. If it does, this header will be extracted,
     * if it doesn't, a WebApplicationException will be thrown.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     * @return a new ImmutableMap roles
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
     * Extracts a specific header PRESTO_EXTRA_CREDENTIAL
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     *
     * @return a Map with the {@link PRESTO_EXTRA_CREDENTIAL} properties
     */
    private static Map<String, String> parseExtraCredentials(MultivaluedMap<String, String> headers)
    {
        return parseProperty(headers, PRESTO_EXTRA_CREDENTIAL);
    }

    /**
     * Returns a map string with parsed properties.
     * <p>
     * Validates that each header presents a set of key/value and adds the valid values in a HashMap properties. For invalid values, an
     * IllegalArgumentException is generated and handled.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     * @param headerName a string with the header name.
     *
     * @return a HashMap with the properties.
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
     * Extracts a header from a headers map, break each value with a comma, remove spaces and omit empty strings.
     * Returns a Set<String> with the extracted {@code PRESTO_CLIENT_TAGS} header.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     *
     * @return a Set<String> with {@code PRESTO_CLIENT_TAGS} headers.
     */
    private static Set<String> parseClientTags(MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(PRESTO_CLIENT_TAGS))));
    }

    /**
     * Extracts a header from a headers map, break each value with a comma, remove spaces and omit empty strings.
     * Returns a Set<String> with the extracted {@code PRESTO_CLIENT_CAPABILITIES} headers.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value
     *
     * @return a Set<String> with {@code PRESTO_CLIENT_CAPABILITIES} headers.
     */
    private static Set<String> parseClientCapabilities(MultivaluedMap<String, String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(headers.getFirst(PRESTO_CLIENT_CAPABILITIES))));
    }

    /**
     * Check which resource will be started.
     * <p>
     * Checks in switch case the name is to execute the EXECUTION_TIME, CPU_TIME or PEAK_MEMORY.
     * If the value is not valid, an IllegalArgumentException is generated an handled.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value.
     *
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
     * Sets a default for serializedData and transforms ir to json.
     * <p>
     * Checks whether serializedData is null. If so, an empty Optional is returned.
     * If not, put it in a pattern and put it in json format.
     *
     * @param headers headers a MultivaluedMap that takes a string as a key and another string as a value.
     *
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
     * Its a generic check whether an expression is true or false.
     * <p>
     * Receives a boolean expression, a message and multiple arguments as parameters.
     * If the expression is false, the method will throw an WebApplicationException.
     *
     * @param expression any boolean expression
     * @param format message that will appear if it is false
     * @param args multiple arguments that can be inserted in the format
     *
     * @throws WebApplicationException will be thrown if the expression is false.
     */
    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    /**
     * Extract the PRESTO_PREPARED_STATEMENT header and parser the sql command.
     * <p>
     * Check all headers and extract the PRESTO_PREPARED_STATEMENT header. Decodes the statementName.
     * Validates the sql command with sqlParser and returns a Map<String,String> with all occurrences.
     *
     * @param headers a MultivaluedMap that takes a string as a key and another string as a value.
     *
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
     * Checks if the parameter is null and instantiates a new {@link TransactionId}
     * <p>
     * Checks whether the transactionId is null. if so, an empty option is returned.
     * If not, a new instance of the {@link TransactionId} is returned. For invalid values,
     * an WebApplicationException is thrown and handled.
     *
     * @param transactionId an string that represents the transaction id.
     *
     * @return an Optional of type TransactionId
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
     * Returns the given string if is non-null and non-empty.
     * <p>
     * Checks if the parameter is non-null and non-empty as well as checks the amount of bits
     * to classify it according to the Unicode standard.
     *
     * @param value a string that represents PrestoHeaders
     *
     * @return {@code string} itself if it is non-null and non-empty.
     *
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
