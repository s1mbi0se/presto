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
