package be.ugent.idlab.knows.mappingweaver.spec.rmlio;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class RMLIOCoreFileTest extends TestCore {

    private static Stream<Arguments> unfixable() {
        return Stream.of(
                // rust panic
                "RMLTC0004b-XML",
                "RMLTC0012c-CSV",
                "RMLTC0012c-JSON",
                "RMLTC0012c-XML",
                "RMLTC0012d-CSV",
                "RMLTC0012d-JSON",
                "RMLTC0012d-XML"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveFailing() {
        return Stream.of(

                "RMLTC0009a-CSV",
                "RMLTC0009a-JSON",
                "RMLTC0009a-XML",

                "RMLTC0009b-CSV",
                "RMLTC0009b-JSON",
                "RMLTC0009b-XML"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveTests() {
        return Stream.of(
                "RMLTC0000-CSV",
                "RMLTC0000-JSON",
                "RMLTC0000-XML",

                "RMLTC0001a-CSV",
                "RMLTC0001a-JSON",
                "RMLTC0001a-XML",

                "RMLTC0001b-CSV",
                "RMLTC0001b-JSON",
                "RMLTC0001b-XML",

                "RMLTC0002a-CSV",
                "RMLTC0002a-JSON",
                "RMLTC0002a-XML",

                "RMLTC0002b-CSV",
                "RMLTC0002b-JSON",
                "RMLTC0002b-XML",

                "RMLTC0002c-CSV",
                "RMLTC0002c-JSON",
                "RMLTC0002c-XML",


                "RMLTC0003c-CSV",
                "RMLTC0003c-JSON",
                "RMLTC0003c-XML",

                "RMLTC0004a-CSV",
                "RMLTC0004a-JSON",
                "RMLTC0004a-XML",

                "RMLTC0005a-CSV",
                "RMLTC0005a-JSON",
                "RMLTC0005a-XML",

                "RMLTC0006a-CSV",
                "RMLTC0006a-JSON",
                "RMLTC0006a-XML",

                "RMLTC0007a-CSV",
                "RMLTC0007a-JSON",
                "RMLTC0007a-XML",

                "RMLTC0007b-CSV",
                "RMLTC0007b-JSON",
                "RMLTC0007b-XML",

                "RMLTC0007c-CSV",
                "RMLTC0007c-JSON",
                "RMLTC0007c-XML",

                "RMLTC0007d-CSV",
                "RMLTC0007d-JSON",
                "RMLTC0007d-XML",

                "RMLTC0007e-CSV",
                "RMLTC0007e-JSON",
                "RMLTC0007e-XML",

                "RMLTC0007f-CSV",
                "RMLTC0007f-JSON",
                "RMLTC0007f-XML",

                "RMLTC0007g-CSV",
                "RMLTC0007g-JSON",
                "RMLTC0007g-XML",

                "RMLTC0008a-CSV",
                "RMLTC0008a-JSON",
                "RMLTC0008a-XML",

                "RMLTC0008b-CSV",
                "RMLTC0008b-JSON",
                "RMLTC0008b-XML",

                "RMLTC0008c-CSV",
                "RMLTC0008c-JSON",
                "RMLTC0008c-XML",

                "RMLTC0010a-CSV",
                "RMLTC0010a-JSON",

                "RMLTC0010b-CSV",
                "RMLTC0010b-JSON",
                "RMLTC0010b-XML",

                "RMLTC0010c-CSV",
                "RMLTC0010c-JSON",
                "RMLTC0010c-XML",

                "RMLTC0011b-CSV",
                "RMLTC0011b-JSON",
                "RMLTC0011b-XML",

                "RMLTC0012a-CSV",
                "RMLTC0012a-JSON",
                "RMLTC0012a-XML",


                "RMLTC0012b-CSV",
                "RMLTC0012b-JSON",
                "RMLTC0012b-XML",

                "RMLTC0013a-JSON",

                "RMLTC0015a-CSV",
                "RMLTC0015a-JSON",
                "RMLTC0015a-XML",


                "RMLTC0019a-CSV",
                "RMLTC0019a-JSON",
                "RMLTC0019a-XML",

                "RMLTC0019b-CSV",
                "RMLTC0019b-JSON",
                "RMLTC0019b-XML",

                "RMLTC0020a-CSV",
                "RMLTC0020a-JSON",
                "RMLTC0020a-XML",

                "RMLTC0020b-CSV",
                "RMLTC0020b-JSON",
                "RMLTC0020b-XML"
        ).map(Arguments::of);
    }

    public static Stream<Arguments> negativeTests() {
        return Stream.of(
                "RMLTC0002e-CSV",
                "RMLTC0002g-JSON",
                "RMLTC0002e-JSON",
                "RMLTC0002e-XML",
                "RMLTC0004b-CSV",
                "RMLTC0004b-JSON",
                "RMLTC0007h-CSV",
                "RMLTC0007h-JSON",
                "RMLTC0007h-XML",
                "RMLTC0015b-CSV",
                "RMLTC0015b-JSON",
                "RMLTC0015b-XML"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeFailing() {

        return Stream.of(
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/core/", directory + '/');
    }

//    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTests(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/core/", directory + '/');
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rmlio/core/", directory + '/');
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rmlio/core/", directory + '/');
    }

    @Disabled("These tests cause panics in the thread, stopping the entire process")
    @ParameterizedTest
    @MethodSource("unfixable")
    public void unfixableTests(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/core/", directory + '/');
    }
}
