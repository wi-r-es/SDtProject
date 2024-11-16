package Services;

import java.io.*;
import java.net.*;

import shared.Message;

public class AckServiceClient {

    private final String serverHost;
    private final int serverPort;

    public AckServiceClient(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public void sendMessage(Message message) {
        try (
            Socket socket = new Socket(serverHost, serverPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            // Send the custom Message object
            out.writeObject(message);

            // Receive the response from the server
            Message response = (Message) in.readObject();
            System.out.println("Server Response: " + response);

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
