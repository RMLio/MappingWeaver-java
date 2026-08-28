package be.ugent.idlab.knows.mappingweaver.csvw;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * A CSV on the Web table, whose dialect says how its rows are written.
 * <p>
 * The plan is read from the case rather than translated from the mapping: a table is
 * understood by MappingLoom as of the version after 0.7.1, and until that is released the
 * translation the engine is given has to come from the case itself.
 */
public class CSVWSourceTest extends TestCore {

    private static final String BASE = "src/test/resources/rml_kgc/spec/rml-io-registry/";

    /**
     * Runs the case against the plan stored next to it.
     */
    private void testWithStoredPlan(String directory) throws Exception {
        String plan = Files.readString(Paths.get(BASE, directory, "plan.json"), StandardCharsets.UTF_8);

        this.positiveTest(BASE, directory + '/', plan, false);
    }

    /**
     * The columns are separated by a semicolon, and "NULL" stands for no value: the record
     * holding it generates nothing, while the record holding "null" is a record like any
     * other, as the value that stands for nothing is the one the table named.
     */
    @Test
    public void aDialectSaysHowTheRowsAreWritten() throws Exception {
        testWithStoredPlan("RMLIOREGTC0012b");
    }
}
