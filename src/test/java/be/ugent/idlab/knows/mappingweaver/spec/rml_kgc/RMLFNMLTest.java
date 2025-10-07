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

@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLFNMLTest extends TestCore {

    private static final List<String> testsFailed = List.of(
            //// positive, rust panic.
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19

            //// negative, rust panic.
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19

            "RMLFNMLTC0081-CSV"

    );

    private static Stream<Arguments> unfixable() {
        return Stream.of(
                "RMLFNMLTC0001-CSV",
                "RMLFNMLTC0002-CSV",
                "RMLFNMLTC0003-CSV",
                "RMLFNMLTC0004-CSV",
                "RMLFNMLTC0005-CSV",
                "RMLFNMLTC0007-CSV",
                "RMLFNMLTC0008-CSV",
                "RMLFNMLTC0011-CSV",
                "RMLFNMLTC0021-CSV",
                "RMLFNMLTC0031-CSV",
                "RMLFNMLTC0041-CSV",
                "RMLFNMLTC0051-CSV",
                "RMLFNMLTC0061-CSV",
                "RMLFNMLTC0101-CSV",
                "RMLFNMLTC0102-CSV",
                "RMLFNMLTC0103-CSV",
                "RMLFNMLTC0104-CSV"

        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                "RMLFNMLTC0071-CSV"
        );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        return Stream.of().map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }

    @Disabled
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }
}

