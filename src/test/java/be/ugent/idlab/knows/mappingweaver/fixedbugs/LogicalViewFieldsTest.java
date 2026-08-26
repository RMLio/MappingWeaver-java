package be.ugent.idlab.knows.mappingweaver.fixedbugs;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * A logical view read by more than one triples map: three triples maps over a view whose
 * field iterates over three items give nine triples.
 */
public class LogicalViewFieldsTest extends TestCore {

    private static final String BASE = "src/test/resources/test-cases/rml_kgc/lv/";

    private static Stream<Arguments> positiveTests() {
        return Stream.of("RMLLVTC2001-JSON").map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest(BASE, directory, false);
    }
}
