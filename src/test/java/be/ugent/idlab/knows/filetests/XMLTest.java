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
public class XMLTest extends TestCore {
    private static final List<String> unfixable = List.of(
            "RMLTC0004b-XML", // negative, panic in Rust thread, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/7
            "RMLTC0007h-XML", // negative, same as above
            "RMLTC0012c-XML",  // negative, empty job graph, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/8
            "RMLTC0012d-XML", // negative, panic in Rust thread
            // TODO: implemented as manual tests with adapted mapping.json
            "RMLTC0009a-XML", // positive, incorrect plan generated, waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/9
            "RMLTC0009b-XML" // same as above

            );

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                "RMLTC0000-XML",
                "RMLTC0001a-XML",
                "RMLTC0001b-XML",
                "RMLTC0002a-XML",
                "RMLTC0002b-XML",
                "RMLTC0003c-XML",
                "RMLTC0004a-XML",
                "RMLTC0005a-XML",
                "RMLTC0006a-XML",
                "RMLTC0007a-XML",
                "RMLTC0007b-XML",
                "RMLTC0007c-XML",
                "RMLTC0007d-XML",
                "RMLTC0007e-XML",
                "RMLTC0007f-XML",
                "RMLTC0007g-XML",
                "RMLTC0008a-XML",
                "RMLTC0008b-XML",
                "RMLTC0008c-XML",
                "RMLTC0010b-XML",
                "RMLTC0010c-XML",
                "RMLTC0011b-XML",
                "RMLTC0012a-XML",
                "RMLTC0012b-XML",
                "RMLTC0015a-XML",
                "RMLTC0019a-XML",
                "RMLTC0019b-XML",
                "RMLTC0020a-XML",
                "RMLTC0020b-XML");
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(
                "RMLTC0002c-XML",
                "RMLTC0002e-XML",
                "RMLTC0015b-XML"
        );

        return directories.stream().map(Arguments::of);
    }


    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/test-cases/xml/", directory);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/test-cases/xml/", directory);
    }

    @Test
    public void rmltc0009a_manual() throws IOException {
        String basePath = "src/test/resources/test-cases/xml/";
        String directory = "RMLTC0009a-XML/";
        String mappingPlan = Files.readString(Paths.get(basePath, directory, "mapping.json"));
        this.positiveTest(basePath, directory, mappingPlan);
    }

    @Test
    public void rmltc0009b_manual() throws IOException {
        String basePath = "src/test/resources/test-cases/xml/";
        String directory = "RMLTC0009b-XML/";
        String mappingPlan = Files.readString(Paths.get(basePath, directory, "mapping.json"));
        this.positiveTest(basePath, directory, mappingPlan);
    }
}
