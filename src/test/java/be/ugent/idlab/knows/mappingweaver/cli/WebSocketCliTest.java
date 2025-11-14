package be.ugent.idlab.knows.mappingweaver.cli;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;

@ExtendWith(FlinkMiniClusterExtension.class)
public class WebSocketCliTest {

    private static final int WS_PORT = 9123;
    private static TestServer server;
    private static final BlockingQueue<String> received = new LinkedBlockingQueue<>();

    public static class TestServer extends WebSocketServer {
        public TestServer(int port) {
            super(new InetSocketAddress(port));
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            // no-op
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            // no-op
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            received.add(message);
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            ex.printStackTrace();
        }

        @Override
        public void onStart() {
            // no-op
        }
    }

    @BeforeAll
    public static void startServer() throws Exception {
        server = new TestServer(WS_PORT);
        server.start();
        Thread.sleep(300);
    }

    @AfterAll
    public static void stopServer() throws Exception {
        if (server != null) {
            server.stop(1000);
        }
    }

    @Test
    public void testCliWebSocketOutput() throws Exception {
        // Use an existing simple mapping test case (JSON) from resources
        String mapping = "src/test/resources/test-cases/json/RMLTC0019a-JSON/mapping.ttl";

    String[] args = new String[]{"-m", mapping, "toWebSocket", "-u", "ws://localhost:" + WS_PORT};

        // Run the CLI main which will execute the mapping and then send CommonSink output to websocket
        Main.main(args);

        // Wait for at least one message from the server
        String msg = received.poll(15, TimeUnit.SECONDS);
        // Assert we received something
        assertFalse(msg == null || msg.isEmpty(), "Expected at least one message on websocket but got none");
    }
}
