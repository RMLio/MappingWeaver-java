package be.ugent.idlab.knows.mappingweaver.rml_kgc.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;


public class RMLLVTest extends TestCore {
    public static String getModule() {
        return "rml-kgc/lv";
    }


    private static Stream<Arguments> negativeTests() {
        return Stream.of(
                "RMLLVTC0005a",
                "RMLLVTC0005b",
                "RMLLVTC0005c",
                "RMLLVTC0008a", // panic in Rust thread
                "RMLLVTC0008b",  // panic in Rust thread
                "RMLLVTC0008c",
                "RMLLVTC0008d",
                "RMLLVTC0009a",
                "RMLLVTC0009c"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveTests() {
        return Stream.of(
                "RMLLVTC0000a",
                "RMLLVTC0000b",
                "RMLLVTC0000c",
                "RMLLVTC0001a",
                "RMLLVTC0001b",
                "RMLLVTC0001c",
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
                // No join instructions from Rust
                "RMLLVTC0006a",
                "RMLLVTC0006b",
                "RMLLVTC0006c",
                "RMLLVTC0006d",
                "RMLLVTC0006e",
                "RMLLVTC0006f",
                "RMLLVTC0007a",
                "RMLLVTC0007b",
                "RMLLVTC0007c",
                // rml:LogicalView joins carrying rml:ExpressionField are not materialised: the joined field is absent downstream
                "RMLLVTC0010a", // MappingException: reference 'json_item' (joined field) not present in the input data
                "RMLLVTC0010b", // MappingException: reference 'json_item' (joined field) not present in the input data
                "RMLLVTC0010c", // MappingException: reference 'json_item' (joined field) not present in the input data
                "RMLLVTC0010d", // Rust translation panic: "Logical view join's field cannot be an iterable" (template-valued joined field)
                "RMLLVTC0010e"  // MappingException: reference 'status' (constant joined field) not present in the input data
        ).map(Arguments::of);
    }

    public static Stream<Arguments> negativeFailing() {
        return Stream.of(

                "RMLLVTC0009b"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/rml_kgc/spec/rml-lv", directory);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/rml_kgc/spec/rml-lv", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTests(String directory) throws Exception {
        this.positiveTest("src/test/resources/rml_kgc/spec/rml-lv", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTests(String directory) throws Exception {
        this.negativeTest("src/test/resources/rml_kgc/spec/rml-lv", directory);
    }
}
