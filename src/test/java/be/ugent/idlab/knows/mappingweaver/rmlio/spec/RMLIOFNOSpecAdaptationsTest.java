package be.ugent.idlab.knows.mappingweaver.rmlio.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class RMLIOFNOSpecAdaptationsTest extends TestCore {

    private static Stream<Arguments> positive() {
        List<String> directories = List.of(
                 "RMLFNOTCC0001-MIXED"  // TODO: Fix and enable when this test passes
        );
        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positive")
    @Disabled("TODO: Fix and enable when this test passes")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/rmlio/test-cases/spec-adaptations/fno/", directory + '/', false);
    }
}
