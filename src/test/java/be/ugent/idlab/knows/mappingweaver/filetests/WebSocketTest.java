package be.ugent.idlab.knows.mappingweaver.filetests;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;

@ExtendWith(FlinkMiniClusterExtension.class)
public class WebSocketTest extends TestCore {

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                "websocket_test"
        );
        return directories.stream().map(Arguments::of);
    }

    public static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(
                // Add negative test cases here if needed
        );
        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/test-cases/websocket/", directory + '/');
    }

}
