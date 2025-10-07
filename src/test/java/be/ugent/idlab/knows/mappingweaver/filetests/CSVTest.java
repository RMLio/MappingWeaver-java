package be.ugent.idlab.knows.mappingweaver.filetests;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class CSVTest extends TestCore {
    private static final List<String> unfixable = List.of(
            "RMLTC0004b-CSV", // negative, panic in Rust thread, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/7
            "RMLTC0007h-CSV", // negative, same as above
            "RMLTC0012c-CSV",  // negative, empty job graph, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/8
            "RMLTC0012d-CSV" // negative, panic in Rust thread
    );

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                "RMLTC0000-CSV",
                "RMLTC0001a-CSV",
                "RMLTC0001b-CSV",
                "RMLTC0002a-CSV",
                "RMLTC0002b-CSV",
                "RMLTC0003c-CSV",
                "RMLTC0004a-CSV",
                "RMLTC0005a-CSV",
                "RMLTC0006a-CSV",
                "RMLTC0007a-CSV",
                "RMLTC0007b-CSV",
                "RMLTC0007c-CSV",
                "RMLTC0007d-CSV",
                "RMLTC0007e-CSV",
                "RMLTC0007f-CSV",
                "RMLTC0007g-CSV",
                "RMLTC0008a-CSV",
                "RMLTC0008b-CSV",
                "RMLTC0008c-CSV",
                "RMLTC0009a-CSV",
                "RMLTC0009b-CSV",
                "RMLTC0010a-CSV",
                "RMLTC0010b-CSV",
                "RMLTC0010c-CSV",
                "RMLTC0011b-CSV",
                "RMLTC0012a-CSV",
                "RMLTC0012b-CSV",
                "RMLTC0015a-CSV",
                "RMLTC0019a-CSV",
                "RMLTC0019b-CSV",
                "RMLTC0020a-CSV",
                "RMLTC0020b-CSV"
        );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(
                "RMLTC0002c-CSV",
                "RMLTC0002e-CSV",
                "RMLTC0015b-CSV"
        );

        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/test-cases/csv/", directory);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/test-cases/csv/", directory);
    }
}
