package be.ugent.idlab.knows.fno;

import be.ugent.idlab.knows.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class FnOTest extends TestCore {

    private static Stream<Arguments> positiveTests() {
        List<String> args = List.of(
                "RMLFNOTC0001-CSV",
                "RMLFNOTC0002-CSV",
                "RMLFNOTC0003-CSV",
//                "RMLFNOTC0004-CSV",
                "RMLFNOTC0005-CSV",
//                "RMLFNOTC0006-CSV",
                "RMLFNOTC0008-CSV",
                "RMLFNOTC0013-CSV",
                "RMLFNOTC0018-CSV",
                "RMLFNOTCA004",
                "RMLFNOTCA004b"
        );

        return args.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        List<String> args = List.of("RMLFNOTC0007-CSV");

        return args.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("positiveTests")
    public void runPositive(String directory) throws Exception {
        this.positiveTest("src/test/resources/test-cases/fno/", directory);
    }

    @ParameterizedTest
    @MethodSource("negativeTests")
    public void runNegative(String directory) throws Exception {
        this.negativeTest("src/test/resources/test-cases/fno/", directory);
    }

}
