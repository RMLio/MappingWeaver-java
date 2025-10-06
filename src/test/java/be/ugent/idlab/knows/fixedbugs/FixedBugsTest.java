package be.ugent.idlab.knows.fixedbugs;

import be.ugent.idlab.knows.cores.TestCore;
import org.junit.jupiter.api.Test;

public class FixedBugsTest extends TestCore {

    @Test
    public void skipRowWithEmptyValue() throws Exception {
        positiveTest("src/test/resources/fixed_bugs", "skip-row-with-empty-value");
    }

}
