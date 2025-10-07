package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@Disabled
@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLSTARTest extends TestCore {

    private static final List<String> testsFailed = List.of(
            //// positive, rust panic
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19

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
            "RMLSTARTC008b",

            //// negative, rust panic
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19
            "RMLSTARTC009",
            "RMLSTARTC010"


    );

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(

                );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(

        );

        return directories.stream().map(Arguments::of);
    }

    @Disabled("Plan generation does not work yet")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-star/", directory);
    }

    @Disabled("Plan generation does not work yet")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-star/", directory);
    }
}
