package be.ugent.idlab.knows.mappingweaver.fnml;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;

public class FnmlTest extends TestCore {

    private static Stream<Arguments> positiveTests() {
        List<String> args = List.of(
                // "RMLFNMLTC0001-CSV",
                "RMLFNMLTC0002-CSV"
                // "RMLFNMLTC0003-CSV"
        );

        return args.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        List<String> args = List.of();

        return args.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("positiveTests")
    public void runPositive(String directory) throws Exception {
        this.positiveTest("src/test/resources/rmlfnml/", directory);
    }

    @ParameterizedTest
    @MethodSource("negativeTests")
    public void runNegative(String directory) throws Exception {
        this.negativeTest("src/test/resources/rmlfnml/", directory);
    }

}
