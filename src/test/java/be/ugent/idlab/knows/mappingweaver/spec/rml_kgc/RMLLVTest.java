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
public class RMLLVTest extends TestCore {
    public static String getModule() {
        return "rml-kgc/lv";
    }


    private static Stream<Arguments> unfixable() {
        return Stream.of(
                "RMLLVTC0008a", // panic in Rust thread
                "RMLLVTC0008b",  // panic in Rust thread
                "RMLLVTC0008c" // panic in Rust thread
        ).map(Arguments::of);
    }

    private static Stream<Arguments> broken() {
        return Stream.of(
                "RMLLVTC0001c", // https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/14
                // No join instructions from Rust
                "RMLLVTC0006a",
                "RMLLVTC0006b",
                "RMLLVTC0006c",
                "RMLLVTC0006d",
                "RMLLVTC0006e",
                "RMLLVTC0006f"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        return Stream.of(
                "RMLLVTC0005a",
                "RMLLVTC0005b",
                "RMLLVTC0005c"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveTests() {
        return Stream.of(
                "RMLLVTC0000a",
                "RMLLVTC0000b",
                "RMLLVTC0000c",
                "RMLLVTC0001a",
                "RMLLVTC0001b",
                "RMLLVTC0001d",
                "RMLLVTC0002a",
                "RMLLVTC0002b",
                "RMLLVTC0002c",
                "RMLLVTC0003a",
                "RMLLVTC0003b",
                "RMLLVTC0003c",
                "RMLLVTC0004a",
                "RMLLVTC0004b",
                "RMLLVTC0004c",
                "RMLLVTC0004d"
        ).map(Arguments::of);
    }

    public static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLLVTC0007a",
                "RMLLVTC0007b",
                "RMLLVTC0007c"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-lv", directory);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-lv", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTests(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-lv", directory);
    }
}
