package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;

@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLFNMLTest extends TestCore {


    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                // "RMLFNMLTC0001-CSV", correct but contains random, which is not supported in testing (non-deterministic)
                "RMLFNMLTC0002-CSV",
                "RMLFNMLTC0003-CSV",
                // "RMLFNMLTC0004-CSV", fails: grel:length function IRI not found
                "RMLFNMLTC0005-CSV",
                "RMLFNMLTC0007-CSV",
                "RMLFNMLTC0008-CSV",
                "RMLFNMLTC0021-CSV",
                // "RMLFNMLTC0041-CSV", fails: literal value mismatch (expected example.com)
                "RMLFNMLTC0051-CSV",
                "RMLFNMLTC0071-CSV",
                "RMLFNMLTC0081-CSV",
                "RMLFNMLTC0101-CSV",
                // "RMLFNMLTC0102-CSV", error: unknown GREL function IRI
                "RMLFNMLTC0103-CSV"
                // "RMLFNMLTC0104-CSV"  fails: expected no output but got VENUS
        );
        return directories.stream().map(Arguments::of);
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> negativeTests() {
        return Stream.of(
                "RMLFNMLTC0051-CSV" 
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }
}

