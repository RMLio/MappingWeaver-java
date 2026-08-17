package be.ugent.idlab.knows.mappingweaver.rml_kgc.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * Logical-view + FNML: a rml:LogicalView field computed by an FnO function
 * (e.g. Name = toUpperCase(name)). Exercises the computed-source-field path.
 * <p>
 * Note: the {@code student.csv} in RMLLVFNML0001a-CSV is a local stand-in; the official
 * upstream input data was not yet available. Replace it (and output.nq if needed) with the
 * upstream case when it lands.
 */
public class RMLLVFNMLTest extends TestCore {

    private static Stream<Arguments> positive() {
        return Stream.of(
                "RMLLVFNML0001a-CSV"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positive")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/rml_kgc/test-cases/spec-adaptations/rml-lv/", directory);
    }
}
