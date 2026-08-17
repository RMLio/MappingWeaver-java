package be.ugent.idlab.knows.mappingweaver.fixedbugs;

import org.junit.jupiter.api.Test;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;

/**
 * Test for the bug where multiple function executions in the same LogicalView
 * share the first function's result instead of evaluating independently.
 */
public class MultipleFunctionExecutionsTest extends TestCore {

    @Test
    public void multipleFunctionExecutionsInLogicalView() throws Exception {
        positiveTest("src/test/resources/fixed_bugs", "multiple-function-executions");
    }
}
