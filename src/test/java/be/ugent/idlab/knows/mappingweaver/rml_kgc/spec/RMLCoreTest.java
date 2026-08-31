package be.ugent.idlab.knows.mappingweaver.rml_kgc.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@Execution(ExecutionMode.CONCURRENT)
public class RMLCoreTest extends TestCore {
    private static Stream<Arguments> unfixable() {
        return Stream.of(
                // negative (see RML)
                // panics in Rust


        ).map(Arguments::of);
    }

    public static String getModule() {
        return "rml_kgc/core";
    }

    private static Stream<Arguments> positivePassing() {
        return Stream.of(
                "RMLTC0000-JSON",
                "RMLTC0001a-JSON",
                "RMLTC0001b-JSON",
                "RMLTC0002a-JSON",
                "RMLTC0002b-JSON",
                "RMLTC0003c-JSON",
                "RMLTC0004a-JSON",
                "RMLTC0005a-JSON",
                "RMLTC0006a-JSON",
                "RMLTC0007a-JSON",
                "RMLTC0007b-JSON",
                "RMLTC0007c-JSON",
                "RMLTC0007d-JSON",
                "RMLTC0007e-JSON",
                "RMLTC0007f-JSON",
                "RMLTC0007g-JSON",
                "RMLTC0008a-JSON",
                "RMLTC0008b-JSON",
                "RMLTC0008c-JSON",
                "RMLTC0009a-JSON",
                "RMLTC0009b-JSON",
                "RMLTC0010a-JSON",
                "RMLTC0010b-JSON",
                "RMLTC0010c-JSON",
                "RMLTC0011b-JSON",
                "RMLTC0012a-JSON",
                "RMLTC0012b-JSON",
                "RMLTC0012e-JSON",
                "RMLTC0013a-JSON",
                "RMLTC0015a-JSON",
                "RMLTC0019a-JSON",
                "RMLTC0020a-JSON",
                "RMLTC0021a-JSON",
                "RMLTC0022a-JSON",
                "RMLTC0022b-JSON",
                "RMLTC0022c-JSON",
                "RMLTC0022d-JSON",
                "RMLTC0022e-JSON",
                "RMLTC0023f-JSON",
                "RMLTC0025a-JSON",
                "RMLTC0025c-JSON",
                "RMLTC0026a-JSON",
                "RMLTC0026b-JSON",
                "RMLTC0026c-JSON",
                "RMLTC0026d-JSON",
                "RMLTC0027a-JSON",
                "RMLTC0028a-JSON",
                "RMLTC0028b-JSON",
                "RMLTC0028c-JSON",
                "RMLTC0029a-JSON",
                "RMLTC0030a-JSON",
                "RMLTC0030b-JSON",
                "RMLTC0031a-JSON",
                "RMLTC0031b-JSON",
                "RMLTC0031c-JSON"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativePassing() {
        return Stream.of(
                "RMLTC0002e-JSON",
                "RMLTC0002g-JSON",
                "RMLTC0004b-JSON",
                "RMLTC0007h-JSON",
                "RMLTC0012c-JSON",
                "RMLTC0012d-JSON",
                "RMLTC0015b-JSON",
                "RMLTC0019b-JSON",
                "RMLTC0023a-JSON",
                "RMLTC0023b-JSON",
                "RMLTC0023c-JSON",
                "RMLTC0023d-JSON",
                "RMLTC0023e-JSON",
                "RMLTC0024a-JSON",
                "RMLTC0025b-JSON"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLTC0027b-JSON",  // awaiting outcome of https://github.com/kg-construct/rml-core/issues/72
                "RMLTC0027c-JSON",  // Doesn't support difference between IRI- and URI encoding + java only supports URL encoding (standard, libs do support it). See https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/48
                "RMLTC0030c-JSON",  // Join: constant-valued parentMap not supported
                "RMLTC0030d-JSON",  // Join: constant-valued parentMap not supported
                "RMLTC0030e-JSON",  // Join: constant-valued childMap not supported
                "RMLTC0030f-JSON"   // Join: constant-valued childMap not supported
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeFailing() {
        return Stream.of(
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positivePassing")
    public void positivePassingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/rml_kgc/spec/rml-core/", directory + '/', false);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativePassing")
    public void negativePassingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/rml_kgc/spec/rml-core/", directory + '/', false);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/rml_kgc/spec/rml-core/", directory + '/', false);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/rml_kgc/spec/rml-core/", directory + '/', false);
    }

    @Disabled("These tests panic the Rust thread")
    @ParameterizedTest(name = "Unfixable test index: {index} Filename: {0}")
    @MethodSource("unfixable")
    public void unfixable(String directory) throws Exception {
        super.positiveTest("src/test/resources/rml_kgc/spec/rml-core/", directory + '/', false);

    }
}
