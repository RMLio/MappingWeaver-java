package be.ugent.idlab.knows.mappingweaver.cli;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;

@Disabled("Not yet implemented")
@ExtendWith(FlinkMiniClusterExtension.class)
public class WebSocketCliTest {

    private static int WS_PORT;
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
            System.err.println("WebSocket server error: " + ex.getMessage());
        }

        @Override
        public void onStart() {
            // no-op
        }
    }

    @BeforeAll
    public static void startServer() throws Exception {
        // pick a free ephemeral port to avoid collisions in CI/repeated runs
        try (ServerSocket s = new ServerSocket(0)) {
            WS_PORT = s.getLocalPort();
        } catch (IOException e) {
            // fallback to default if we cannot get a free port
            WS_PORT = 9123;
        }

        server = new TestServer(WS_PORT);
        server.start();

        // give the server a short moment to bind
        Thread.sleep(500);
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

        // Collect any messages we receive for up to 15 seconds
        StringBuilder all = new StringBuilder();
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        String msg;
        while (System.currentTimeMillis() < end) {
            msg = received.poll(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                all.append(msg).append('\n');
            } else {
                // If we've already collected something, break early
                if (all.length() > 0) break;
            }
        }

        String joined = all.toString();
        // Basic validation: the mapping should produce triples with these subjects
        assertTrue(joined.contains("<http://example.com/ns#Jhon>"), "Expected message to contain subject <http://example.com/ns#Jhon>");
        assertTrue(joined.contains("<http://example.com/base/Carlos>"), "Expected message to contain subject <http://example.com/base/Carlos>");
    }
}
