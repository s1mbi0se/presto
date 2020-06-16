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

import io.prestosql.plugin.annotations.CreateSchemaFlowLoggable;
import io.prestosql.plugin.annotations.FinishCreateSchemaFlowLoggable;
import io.prestosql.plugin.annotations.StartCreateSchemaFlowLoggable;

public class Hello {
    public static void main(String[] args) {
        sayHello();
        sayContinue();
        sayGoodbye();
    }
    @StartCreateSchemaFlowLoggable
    public static void sayHello() {
        System.out.println("Hello");
    }
    @CreateSchemaFlowLoggable
    public static void sayContinue() {
        System.out.println("continue");
    }
    @FinishCreateSchemaFlowLoggable
    public static void sayGoodbye() {
        System.out.println("Goodbye");
    }
}
