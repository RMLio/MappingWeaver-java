package be.ugent.idlab.knows.mappingweaver.rml_kgc.regressions;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Test;

public class FixedBugsTest extends TestCore {

    @Test
    public void skipRowWithEmptyValue() throws Exception {
        positiveTest("src/test/resources/rml_kgc/test-cases/regressions/fixed-bugs", "skip-row-with-empty-value", false);
    }

}
