package be.ugent.idlab.knows.filetests;

import be.ugent.idlab.knows.cores.TestCore;
import be.ugent.idlab.knows.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class JSONTest extends TestCore {

    private static final List<String> unfixable = List.of(
            "RMLTC0004b-JSON", // negative, panic in Rust thread, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/7
            "RMLTC0007h-JSON", // negative, same as above
            "RMLTC0012c-JSON", // negative, empty job graph, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/8
            "RMLTC0012d-JSON" // negative, panic in Rust thread
            // TODO: these are written manually, bypassing Rust
//            "RMLTC0009a-JSON", // positive, incorrect plan generated, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/9
//            "RMLTC0009b-JSON" // same as above
            );

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
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
                "RMLTC0015a-JSON",
                "RMLTC0019a-JSON",
                "RMLTC0019b-JSON",
                "RMLTC0020a-JSON",
                "RMLTC0020b-JSON"
        );
        return directories.stream().map(Arguments::of);
    }

    public static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(
                "RMLTC0002c-JSON",
                "RMLTC0002e-JSON",
                "RMLTC0015b-JSON"
        );
        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/test-cases/json/", directory + '/');
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/test-cases/json/", directory + '/');
    }

//    @Test
//    public void rmltc0009a_manual() throws IOException {
//        String basePath = "src/test/resources/test-cases/json/";
//        String directory = "RMLTC0009a-JSON/";
//        String mappingPlan = Files.readString(Paths.get(basePath, directory, "mapping.json"));
//        this.positiveTest(basePath, directory, mappingPlan);
//    }
//
//    @Test
//    public void rmltc0009b_manual() throws IOException {
//        String basePath = "src/test/resources/test-cases/json/";
//        String directory = "RMLTC0009b-JSON/";
//        String mappingPlan = Files.readString(Paths.get(basePath, directory, "mapping.json"));
//        this.positiveTest(basePath, directory, mappingPlan);
//    }
}
