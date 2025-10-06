package be.ugent.idlab.knows.spec.rml_kgc;

import be.ugent.idlab.knows.cores.TestCore;
import be.ugent.idlab.knows.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLRegistryTest extends TestCore {
    private static Stream<Arguments> positive() {
        return Stream.of(
                "RMLIOREGTC0001a",
                "RMLIOREGTC0002a",
                "RMLIOREGTC0002d",
                "RMLIOREGTC0003a",
                "RMLIOREGTC0003e"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negative() {
        return Stream.of(
                "RMLIOREGTC0001b",
                "RMLIOREGTC0002b",
                "RMLIOREGTC0002c",
                "RMLIOREGTC0003b",
                "RMLIOREGTC0003c"

        ).map(Arguments::of);
    }

    private static Stream<Arguments> unfixable() {
        // panic in rust
        return Stream.of(
                "RMLIOREGTC0003d",
                "RMLIOREGTC0004a",
                "RMLIOREGTC0004b",
                "RMLIOREGTC0004c",
                "RMLIOREGTC0004d",
                "RMLIOREGTC0004e",
                "RMLIOREGTC0004f",
                "RMLIOREGTC0004g",
                "RMLIOREGTC0004h",
                "RMLIOREGTC0004i",
                "RMLIOREGTC0004j",
                "RMLIOREGTC0004k",
                "RMLIOREGTC0004l",
                "RMLIOREGTC0004m",
                "RMLIOREGTC0004n",
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004p",
                "RMLIOREGTC0004q",
                "RMLIOREGTC0004r",
                "RMLIOREGTC0004s",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004u",
                "RMLIOREGTC0004v",
                "RMLIOREGTC0004w",
                "RMLIOREGTC0004x",
                "RMLIOREGTC0004y",
                "RMLIOREGTC0004z",
                "RMLIOREGTC0005a",
                "RMLIOREGTC0005b",
                "RMLIOREGTC0005c",
                "RMLIOREGTC0005d",
                "RMLIOREGTC0005e",
                "RMLIOREGTC0005f",
                "RMLIOREGTC0005g",
                "RMLIOREGTC0005h",
                "RMLIOREGTC0005i",
                "RMLIOREGTC0005j",
                "RMLIOREGTC0005k",
                "RMLIOREGTC0005l",
                "RMLIOREGTC0005m",
                "RMLIOREGTC0005n",
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005p",
                "RMLIOREGTC0005q",
                "RMLIOREGTC0005r",
                "RMLIOREGTC0005s",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005u",
                "RMLIOREGTC0005v",
                "RMLIOREGTC0005w",
                "RMLIOREGTC0005x",
                "RMLIOREGTC0005y",
                "RMLIOREGTC0005z",
                "RMLIOREGTC0006a",
                "RMLIOREGTC0006b",
                "RMLIOREGTC0006c",
                "RMLIOREGTC0006d",
                "RMLIOREGTC0006e",
                "RMLIOREGTC0006f",
                "RMLIOREGTC0006g",
                "RMLIOREGTC0006h",
                "RMLIOREGTC0006i",
                "RMLIOREGTC0006j",
                "RMLIOREGTC0006k",
                "RMLIOREGTC0006l",
                "RMLIOREGTC0006m",
                "RMLIOREGTC0006n",
                "RMLIOREGTC0006p",
                "RMLIOREGTC0006q",
                "RMLIOREGTC0006r",
                "RMLIOREGTC0006s",
                "RMLIOREGTC0006t",
                "RMLIOREGTC0006u",
                "RMLIOREGTC0006v",
                "RMLIOREGTC0006w",
                "RMLIOREGTC0006x",
                "RMLIOREGTC0006y",
                "RMLIOREGTC0006z",
                "RMLIOREGTC0007a",
                "RMLIOREGTC0008a",
                "RMLIOREGTC0009a",
                "RMLIOREGTC0010a",
                "RMLIOREGTC0011a",
                "RMLIOREGTC0012a",
                "RMLIOREGTC0012b",
                "RMLIOREGTC0012c",
                "RMLIOREGTC0012d",
                "RMLIOREGTC0012e",
                "RMLIOREGTC0012f",
                "RMLIOREGTC0012g",
                "RMLIOREGTC0012h",
                "RMLIOREGTC0012i"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positive")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/registry/", directory);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("negative")
    public void negativeTest(String directory) throws Exception {
        super.negativeTest("src/test/resources/spec/rmlio/registry/", directory);
    }
}
