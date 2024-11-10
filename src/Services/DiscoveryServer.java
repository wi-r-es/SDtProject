package Services;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DiscoveryServer {
    private static final int PORT = 9090;  // Port for discovery service
    private final Map<String, String> activeNodes = new ConcurrentHashMap<>();  // Maps node ID to IP address

    public static void main(String[] args) {
        new DiscoveryServer().start();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Discovery server started on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String message = in.readLine();
            String[] parts = message.split(" ");
            String command = parts[0];

            if (command.equals("REGISTER")) {
                String nodeId = parts[1];
                String address = clientSocket.getInetAddress().getHostAddress();
                activeNodes.put(nodeId, address);
                System.out.println("Registered node: " + nodeId + " at " + address);
            } else if (command.equals("UNREGISTER")) {
                String nodeId = parts[1];
                activeNodes.remove(nodeId);
                System.out.println("Unregistered node: " + nodeId);
            } else if (command.equals("GET_NODES")) {
                for (Map.Entry<String, String> entry : activeNodes.entrySet()) {
                    out.println(entry.getKey() + " " + entry.getValue());
                }
                out.println("END");  // end of request
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
