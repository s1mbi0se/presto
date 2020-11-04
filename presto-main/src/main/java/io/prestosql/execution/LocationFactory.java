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

import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.QueryId;

import java.net.URI;

public interface LocationFactory
{
    URI createQueryLocation(QueryId queryId);

    /**
     * Gets the URI where is possible to retrieve the task's metadata.
     *
     * @param taskId the task identifier
     * @return the URI where is possible to retrieve the task's metadata
     */
    URI createLocalTaskLocation(TaskId taskId);

    /**
     * Gets the URI where is possible to retrieve the task's metadata.
     *
     * @param taskId the task identifier
     * @param node the object with metadata about the node instance
     * @return the URI where is possible to retrieve the task's metadata
     */
    URI createTaskLocation(InternalNode node, TaskId taskId);

    URI createMemoryInfoLocation(InternalNode node);
}
