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
package io.prestosql.server.protocol;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.security.SecureRandom;

import static java.util.Objects.requireNonNull;

/**
 * Slug is a unique identifier for a resource.
 */
public final class Slug
{
    public enum Context
    {
        QUEUED_QUERY,
        EXECUTING_QUERY,
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Creates a slug for a request.
     * <p>
     * Slug is the object used in the request URL to identify a resource.
     *
     * @return a unique identifier for a resource
     */
    public static Slug createNew()
    {
        byte[] randomBytes = new byte[16];
        RANDOM.nextBytes(randomBytes);
        return new Slug(randomBytes);
    }

    private final HashFunction hmac;

    private Slug(byte[] slugKey)
    {
        this.hmac = Hashing.hmacSha1(requireNonNull(slugKey, "slugKey is null"));
    }

    /**
     * Computes a hash code based on data about the query state and the received token.
     *
     * @param context a constant that represents if the query is running or is queued
     * @param token a generated token that identifies the client's request used to obtain the response from server
     * @return a hash code based on data about the query state and the received token
     */
    public String makeSlug(Context context, long token)
    {
        // "y" is an arbitrary prefix distinguishing this slug version. Added for troubleshooting purposes.
        return "y" + hmac.newHasher()
                .putInt(requireNonNull(context, "context is null").ordinal())
                .putLong(token)
                .hash()
                .toString();
    }

    /**
     * Checks if a slug is valid based on the received context and token.
     *
     * @param context a constant that represents if query is running or is queued
     * @param slug a hash code based on data about the query state and the received token
     * @param token a generated token that identifies the client's request used to obtain the response from server
     * @return a flag that indicates if received slug is valid for the respective context and token
     */
    public boolean isValid(Context context, String slug, long token)
    {
        return makeSlug(context, token).equals(slug);
    }
}
