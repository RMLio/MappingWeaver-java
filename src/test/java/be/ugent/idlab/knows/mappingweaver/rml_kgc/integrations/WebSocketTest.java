package be.ugent.idlab.knows.mappingweaver.rml_kgc.integrations;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.SimpleWebSocketServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class WebSocketTest extends TestCore {

    private static SimpleWebSocketServer webSocketServer;
    private static final int WS_PORT = 1234;

    @BeforeAll
    public static void startWebSocketServer() throws Exception {
        String jsonFilePath = "src/test/resources/rml_kgc/test-cases/integrations/websocket/websocket_test/sensor.json";
        webSocketServer = new SimpleWebSocketServer(WS_PORT, jsonFilePath);
        webSocketServer.start();
        // Give the server a moment to start
        Thread.sleep(500);
        System.out.println("WebSocket server started on port " + WS_PORT);
    }

    @AfterAll
    public static void stopWebSocketServer() throws Exception {
        if (webSocketServer != null) {
            webSocketServer.stop(1000);
            System.out.println("WebSocket server stopped");
        }
    }

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
        super.positiveTest("src/test/resources/rml_kgc/test-cases/integrations/websocket/", directory + '/', false);
    }

}
