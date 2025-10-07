package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
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
                "RMLTC0010a-JSON",
                "RMLTC0010b-JSON",
                "RMLTC0010c-JSON",
                "RMLTC0011b-JSON",
                "RMLTC0012a-JSON",
                "RMLTC0012b-JSON",
                "RMLTC0013a-JSON",
                "RMLTC0015a-JSON",
                "RMLTC0019b-JSON",
                "RMLTC0021a-JSON",
                "RMLTC0022a-JSON",
                "RMLTC0022b-JSON",
                "RMLTC0023a-JSON",
                "RMLTC0026a-JSON",
                "RMLTC0027a-JSON",
                "RMLTC0027b-JSON",
                "RMLTC0027c-JSON",
                "RMLTC0027d-JSON",
                "RMLTC0028a-JSON",
                "RMLTC0028b-JSON",
                "RMLTC0028c-JSON",
                "RMLTC0029b-JSON"
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
                "RMLTC0023b-JSON",
                "RMLTC0023c-JSON",
                "RMLTC0023d-JSON",
                "RMLTC0023f-JSON",
                "RMLTC0025b-JSON",
                "RMLTC0026b-JSON"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLTC0009b-JSON",
                "RMLTC0019a-JSON",
                "RMLTC0020a-JSON",
                "RMLTC0022c-JSON",
                "RMLTC0029a-JSON",
                "RMLTC0029c-JSON"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeFailing() {
        return Stream.of(
                "RMLTC0023e-JSON"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positivePassing")
    public void positivePassingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rml_kgc/rml-core/", directory + '/');
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativePassing")
    public void negativePassingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rml_kgc/rml-core/", directory + '/');
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rml_kgc/rml-core/", directory + '/');
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rml_kgc/rml-core/", directory + '/');
    }

    @Disabled("These tests panic the Rust thread")
    @ParameterizedTest(name = "Unfixable test index: {index} Filename: {0}")
    @MethodSource("unfixable")
    public void unfixable(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rml_kgc/rml-core/", directory + '/');

    }
}
