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
package io.prestosql.plugin.hive.authentication;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.session.metadata.QueryRequestMetadata;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class HiveIdentity
{
//    private static final HiveIdentity NONE_IDENTITY = new HiveIdentity();

    private final Optional<String> username;

    private final Optional<QueryRequestMetadata> metadata;

//    private HiveIdentity()
//    {
//        this.username = Optional.empty();
//        this.metadata = Optional.empty();
//    }

    public HiveIdentity(ConnectorSession session)
    {
        this(requireNonNull(session, "session is null").getIdentity(), requireNonNull(session, "session is null").getQueryRequestMetadata());
    }

    public HiveIdentity(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        this.username = Optional.of(requireNonNull(identity.getUser(), "identity.getUser() is null"));
        this.metadata = Optional.empty();
    }

    public HiveIdentity(ConnectorIdentity identity, Optional<QueryRequestMetadata> metadata)
    {
        requireNonNull(identity, "identity is null");
        this.username = Optional.of(requireNonNull(identity.getUser(), "identity.getUser() is null"));
        this.metadata = metadata;
    }

    private HiveIdentity(Optional<QueryRequestMetadata> metadata)
    {
        this.username = Optional.empty();
        this.metadata = metadata;
    }

//    // this should be called only by CachingHiveMetastore
//    public static HiveIdentity none()
//    {
//        return NONE_IDENTITY;
//    }

    public static HiveIdentity none(Optional<QueryRequestMetadata> metadata)
    {
        return new HiveIdentity(metadata);
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    public Optional<QueryRequestMetadata> getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("username", username)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveIdentity other = (HiveIdentity) o;
        return Objects.equals(username, other.username);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username);
    }
}
