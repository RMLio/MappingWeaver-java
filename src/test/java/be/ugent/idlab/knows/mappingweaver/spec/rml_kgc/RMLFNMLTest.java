package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class RMLFNMLTest extends TestCore {


    private static Stream<Arguments> positivePassing() {
        return Stream.of(
                "RMLFNMLTC0002-CSV",
                "RMLFNMLTC0003-CSV",
                "RMLFNMLTC0004-CSV",
                "RMLFNMLTC0005-CSV",
                "RMLFNMLTC0007-CSV",
                "RMLFNMLTC0021-CSV",
                "RMLFNMLTC0041-CSV", //RETURNMAP -> rml:constant
                "RMLFNMLTC0051-CSV",
                "RMLFNMLTC0071-CSV",
                "RMLFNMLTC0081-CSV"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLFNMLTC0001-CSV", // Function not found (HTTPS://W3ID.ORG/IMEC/IDLAB/FUNCTION#ALWAYSRETURNSABC)
                "RMLFNMLTC0008-CSV", // Fails because the used 'substring' functions throws an exception. If it were to return an empty string, the test would pass
                "RMLFNMLTC0011-CSV", // expected output contains invalid IRI (HTTP://VENUS)
                "RMLFNMLTC0031-CSV", // expected output contains invalid IRI (HTTP://WWW.EXAMPLE.COM)
                "RMLFNMLTC0032-CSV", // condition doesn't work
                "RMLFNMLTC0061-CSV" // expected output contains invalid IRI (HTTP://EXAMPLE.COM/VENUS)
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        return Stream.of(
                "RMLFNMLTC0101-CSV",
                "RMLFNMLTC0102-CSV",
                "RMLFNMLTC0103-CSV"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeFailing() {
        return Stream.of(
                "RMLFNMLTC0104-CSV" // Doesn't take rml:return into account (Return Map not properly implemented - probably also not in AlgeMapLoom)
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positivePassing")
    public void positivePassingTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory + '/');
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory + '/');
    }
}

