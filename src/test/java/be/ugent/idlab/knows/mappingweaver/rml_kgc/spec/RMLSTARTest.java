package be.ugent.idlab.knows.mappingweaver.rml_kgc.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class RMLSTARTest extends TestCore {

    // Negative tests (README: "**Error expected?** Yes") that pass: RDF-star plan
    // generation throws, which the harness accepts as the expected failure.
    private static Stream<Arguments> negativePassing() {
        return Stream.of(
                "RMLSTARTC009",
                "RMLSTARTC010"
        ).map(Arguments::of);
    }

    // Positive tests (README: "**Error expected?** No") that don't pass: RDF-star plan
    // generation (quoted / asserted triples) is not supported, so translation throws
    // before any output is produced. (Additionally, the expected outputs use output.nt,
    // which TestCore does not currently recognize.)
    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLSTARTC001a",
                "RMLSTARTC001b",
                "RMLSTARTC002a",
                "RMLSTARTC002b",
                "RMLSTARTC003a",
                "RMLSTARTC003b",
                "RMLSTARTC004a",
                "RMLSTARTC004b",
                "RMLSTARTC005a",
                "RMLSTARTC005b",
                "RMLSTARTC006a",
                "RMLSTARTC006b",
                "RMLSTARTC007a",
                "RMLSTARTC007b",
                "RMLSTARTC008a",
                "RMLSTARTC008b"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativePassing")
    public void negativePassingTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/rml_kgc/spec/rml-star/", directory, false);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/rml_kgc/spec/rml-star/", directory, false);
    }
}
