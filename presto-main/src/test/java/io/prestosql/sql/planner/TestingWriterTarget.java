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

package io.prestosql.sql.planner;

import io.prestosql.metadata.TableHandle;
import io.prestosql.sql.planner.plan.TableWriterNode;

import java.util.Optional;

public class TestingWriterTarget
        extends TableWriterNode.WriterTarget
{
    @Override
    public Optional<TableHandle> getTableHandle()
    {
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return "testing handle";
    }
}
