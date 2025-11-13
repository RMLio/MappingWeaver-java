package be.ugent.idlab.knows.mappingweaver.utilities;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Simple WebSocket server for testing purposes.
 * Sends JSON data from a file to connected clients.
 */
public class SimpleWebSocketServer extends WebSocketServer {

    private final String jsonData;

    public SimpleWebSocketServer(int port, String jsonFilePath) throws Exception {
        super(new InetSocketAddress(port));
        this.jsonData = Files.readString(Paths.get(jsonFilePath));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("WebSocket opened: " + conn.getRemoteSocketAddress());
        // Send the JSON data to the client immediately upon connection
        conn.send(jsonData);
        // Close the connection after a short delay to allow the client to process the data
        new Thread(() -> {
            try {
                Thread.sleep(1000); // Wait 1 second
                conn.close();
                System.out.println("WebSocket connection closed after sending data");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("WebSocket closed: " + conn.getRemoteSocketAddress() + " Reason: " + reason);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("Received message: " + message);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
        ex.printStackTrace();
    }

    @Override
    public void onStart() {
        System.out.println("WebSocket server started on port " + getPort());
    }
}
